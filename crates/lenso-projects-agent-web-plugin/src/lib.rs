//! Authenticated ingress over one explicitly bound Projects Tool provider.
use lenso::Port;
use lenso_auth_sdk::{AuthOutcome, CredentialEvidence, authenticate_request, decode_auth_response};
use lenso_capability_agent_tool_provider as tools;
use lenso_capability_auth as auth;
use lenso_capability_http_endpoint::{
    self as http_endpoint_contract, EndpointHandleInvocationError, HandleRequest, HandleResponse,
    HandleResponseHeadersItem, endpoint,
};
use lenso_kernel::{InvocationContext, RuntimeFailure};
use serde::{Deserialize, Serialize};

const MAX_REQUEST: usize = 300_000;
const MAX_RESPONSE: usize = 2_097_152;

#[lenso::plugin]
#[derive(Clone, Debug)]
struct ProjectsAgentWebPlugin {
    auth: Port<auth::AuthClient>,
    tools: Port<tools::ToolProviderClient>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolInput {
    name: String,
    arguments_json: String,
}

#[endpoint]
impl ProjectsAgentWebPlugin {
    /// Public API descriptions only. Execution always authenticates independently.
    #[get("projects.agent.manifest", "/projects/agent/manifest")]
    async fn manifest(
        &self,
        context: InvocationContext,
        _request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        match self
            .tools
            .catalog_with_context(context, tools::CatalogRequest {})
            .await
        {
            Ok(catalog) => reply(200, &catalog),
            Err(tools::ToolProviderCatalogInvocationError::Domain(_)) => {
                problem(503, "catalog_unavailable")
            }
            Err(tools::ToolProviderCatalogInvocationError::Runtime(error)) => {
                Err(EndpointHandleInvocationError::Runtime(error))
            }
        }
    }

    #[get("projects.agent.catalog", "/projects/agent/tools")]
    async fn catalog(
        &self,
        context: InvocationContext,
        request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        let context = match self.authenticate(context, &request).await? {
            Ok(context) => context,
            Err(response) => return Ok(response),
        };
        match self
            .tools
            .catalog_with_context(context, tools::CatalogRequest {})
            .await
        {
            Ok(catalog) => reply(200, &catalog),
            Err(tools::ToolProviderCatalogInvocationError::Domain(_)) => {
                problem(503, "catalog_unavailable")
            }
            Err(tools::ToolProviderCatalogInvocationError::Runtime(error)) => {
                Err(EndpointHandleInvocationError::Runtime(error))
            }
        }
    }

    #[post("projects.agent.execute", "/projects/agent/tools/execute")]
    async fn execute(
        &self,
        context: InvocationContext,
        request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        let context = match self.authenticate(context, &request).await? {
            Ok(context) => context,
            Err(response) => return Ok(response),
        };
        if request.body.len() > MAX_REQUEST {
            return problem(413, "request_too_large");
        }
        let Ok(input) = serde_json::from_slice::<ToolInput>(request.body.as_ref()) else {
            return problem(400, "invalid_request");
        };
        if input.name.is_empty() || input.name.len() > 128 || input.arguments_json.len() > 262_144 {
            return problem(400, "invalid_arguments");
        }
        let Ok(arguments_json) = input.arguments_json.parse() else {
            return problem(400, "invalid_arguments");
        };
        match self
            .tools
            .execute_with_context(
                context,
                tools::ExecuteRequest {
                    name: input.name,
                    arguments_json,
                },
            )
            .await
        {
            Ok(response) => reply(200, &response),
            Err(tools::ToolProviderExecuteInvocationError::Runtime(error)) => {
                Err(EndpointHandleInvocationError::Runtime(error))
            }
            Err(tools::ToolProviderExecuteInvocationError::Domain(error)) => {
                let status = match &error {
                    tools::ExecuteError::InvalidArguments => 400,
                    tools::ExecuteError::PermissionDenied => 403,
                    tools::ExecuteError::NotFound => 404,
                    tools::ExecuteError::OutputLimitExceeded => 413,
                    tools::ExecuteError::ExecutionFailed { .. } => 422,
                    tools::ExecuteError::Unknown(_) => {
                        return Err(EndpointHandleInvocationError::Runtime(
                            RuntimeFailure::ProtocolViolation {
                                capability: tools::CAPABILITY_ID,
                            },
                        ));
                    }
                };
                reply(status, &error)
            }
        }
    }
}

impl ProjectsAgentWebPlugin {
    async fn authenticate(
        &self,
        context: InvocationContext,
        request: &HandleRequest,
    ) -> Result<Result<InvocationContext, HandleResponse>, EndpointHandleInvocationError> {
        // Only ingress-selected protocol-neutral session evidence is accepted. JSON
        // never carries credentials, subjects, assertions or InvocationContext.
        let Some(credential) = request
            .credential
            .as_ref()
            .filter(|value| value.scheme == "session")
        else {
            return Ok(Err(problem(401, "authentication_required")?));
        };
        let evidence = CredentialEvidence::new(&credential.scheme, &credential.value);
        let response = match self
            .auth
            .authenticate_with_context(context.clone(), authenticate_request(Some(evidence)))
            .await
        {
            Ok(response) => response,
            Err(auth::AuthInvocationError::Domain(_)) => {
                return Ok(Err(problem(401, "invalid_session")?));
            }
            Err(auth::AuthInvocationError::Runtime(error)) => {
                return Err(EndpointHandleInvocationError::Runtime(error));
            }
        };
        let outcome = decode_auth_response(response).map_err(|_| {
            EndpointHandleInvocationError::Runtime(RuntimeFailure::ProtocolViolation {
                capability: auth::CAPABILITY_ID,
            })
        })?;
        let AuthOutcome::Authenticated(assertion) = outcome else {
            return Ok(Err(problem(401, "authentication_required")?));
        };
        if assertion.actor_kind() != "user" {
            return Ok(Err(problem(403, "user_required")?));
        }
        let context = assertion.attach(context).map_err(|_| {
            EndpointHandleInvocationError::Runtime(RuntimeFailure::ProtocolViolation {
                capability: auth::CAPABILITY_ID,
            })
        })?;
        Ok(Ok(context))
    }
}

fn problem(status: i64, code: &str) -> Result<HandleResponse, EndpointHandleInvocationError> {
    reply(status, &serde_json::json!({"error":code}))
}
fn reply(
    status: i64,
    value: &impl Serialize,
) -> Result<HandleResponse, EndpointHandleInvocationError> {
    let body = serde_json::to_vec(value).map_err(|_| {
        EndpointHandleInvocationError::Runtime(RuntimeFailure::ProtocolViolation {
            capability: tools::CAPABILITY_ID,
        })
    })?;
    if body.len() > MAX_RESPONSE {
        return problem(413, "response_too_large");
    }
    Ok(HandleResponse {
        body: body.into(),
        status,
        headers: [
            ("content-type", "application/json"),
            ("cache-control", "no-store"),
            ("x-content-type-options", "nosniff"),
        ]
        .into_iter()
        .map(|(name, value)| HandleResponseHeadersItem {
            name: name.into(),
            value: value.into(),
        })
        .collect(),
    })
}

/// Retains this linked Plugin in native Host assemblies.
pub fn link() {}
