//! Authenticated ingress over one explicitly bound Projects Tool provider.
use lenso::Port;
use lenso_auth_sdk::{AuthOutcome, CredentialEvidence, authenticate_request, decode_auth_response};
use lenso_capability_agent_prompt_provider as prompts;
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

/// Public navigation authority selected by the App; absent for legacy installs.
#[derive(Clone, Debug, Default, Deserialize, Serialize, lenso::PluginConfig)]
#[serde(deny_unknown_fields)]
pub struct ProjectsAgentWebConfig {
    #[serde(default)]
    pub origin: Option<String>,
}

fn clean_origin(value: &str) -> Option<url::Url> {
    let origin = url::Url::parse(value).ok()?;
    let local = matches!(origin.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(origin.scheme() == "https" || (origin.scheme() == "http" && local))
        || origin.host_str().is_none()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        return None;
    }
    Some(origin)
}

fn validate_config(config: &ProjectsAgentWebConfig) -> Result<(), RuntimeFailure> {
    if config
        .origin
        .as_deref()
        .is_some_and(|origin| clean_origin(origin).is_none())
    {
        return Err(RuntimeFailure::InvalidResolvedPlan {
            detail: "Projects Agent origin must be a clean HTTPS origin or local HTTP origin"
                .into(),
        });
    }
    Ok(())
}

#[lenso::plugin(validate=validate_config)]
#[derive(Clone, Debug)]
struct ProjectsAgentWebPlugin {
    #[config]
    config: ProjectsAgentWebConfig,
    auth: Port<auth::AuthClient>,
    tools: Port<tools::ToolProviderClient>,
    prompts: Port<prompts::PromptProviderClient>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolInput {
    name: String,
    arguments_json: String,
}

#[endpoint]
impl ProjectsAgentWebPlugin {
    /// Public, user-independent domain instructions from the bound Projects owner.
    #[get("projects.agent.prompts", "/projects/agent/prompts")]
    async fn prompts(
        &self,
        context: InvocationContext,
        _request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        match self
            .prompts
            .contribute_with_context(context, prompts::ContributeRequest {})
            .await
        {
            Ok(response) => reply(200, &response),
            Err(prompts::PromptProviderInvocationError::Domain(_)) => {
                problem(503, "prompts_unavailable")
            }
            Err(prompts::PromptProviderInvocationError::Runtime(error)) => {
                Err(EndpointHandleInvocationError::Runtime(error))
            }
        }
    }
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
        let tool_name = input.name.clone();
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
            Ok(mut response) => {
                if let Some(origin) = self.config.origin.as_deref() {
                    project_issue_links(origin, &tool_name, &mut response);
                }
                reply(200, &response)
            }
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

fn project_issue_links(origin: &str, tool_name: &str, result: &mut tools::ExecuteResponse) {
    if !matches!(
        tool_name,
        "projects_get_issue"
            | "projects_list_issues"
            | "projects_create_issue"
            | "projects_update_issue"
            | "projects_move_issue"
    ) {
        return;
    }
    let Some(origin) = clean_origin(origin) else {
        return;
    };
    let Ok(mut body) = serde_json::from_str::<serde_json::Value>(&result.content) else {
        return;
    };
    let link = |issue: &serde_json::Value| {
        let id = issue.get("issue_id")?.as_str()?;
        let organization = issue.get("organization_id")?.as_str()?;
        let title = issue.get("title")?.as_str()?;
        let mut url = origin.clone();
        url.set_path("/projects");
        url.query_pairs_mut()
            .append_pair("organization_id", organization)
            .append_pair("issue", id);
        Some(serde_json::json!({"title":title,"url":url.as_str(),"issue_id":id}))
    };
    let links = if let Some(items) = body.get("items").and_then(serde_json::Value::as_array) {
        items.iter().filter_map(link).collect::<Vec<_>>()
    } else {
        link(&body).into_iter().collect()
    };
    if !links.is_empty() {
        if let Some(object) = body.as_object_mut() {
            object.insert("_links".into(), serde_json::Value::Array(links));
            if let Ok(content) = serde_json::to_string_pretty(&body) {
                result.content = content;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn output(value: &serde_json::Value) -> tools::ExecuteResponse {
        tools::ExecuteResponse {
            content: value.to_string(),
            content_type: tools::ContentType::Text,
            content_blocks: None,
            metadata_json: "{}".parse().unwrap(),
        }
    }

    #[test]
    fn issue_links_use_only_clean_configured_origin_and_preserve_domain_fields() {
        let issue = serde_json::json!({"issue_id":"a&issue=other#x","organization_id":"org?x","title":"An issue","revision":"3","description":"keep me","origin":"https://attacker.example"});
        let mut response = output(&issue);
        project_issue_links("https://app.example", "projects_get_issue", &mut response);
        let projected: serde_json::Value = serde_json::from_str(&response.content).unwrap();
        for (key, value) in issue.as_object().unwrap() {
            assert_eq!(&projected[key], value);
        }
        let link = &projected["_links"][0];
        let url = url::Url::parse(link["url"].as_str().unwrap()).unwrap();
        assert_eq!(url.origin().ascii_serialization(), "https://app.example");
        assert_eq!(url.path(), "/projects");
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            vec![
                ("organization_id".into(), "org?x".into()),
                ("issue".into(), "a&issue=other#x".into())
            ]
        );
        assert_eq!(link["title"], "An issue");
        let list = serde_json::json!({"items":[issue],"next_cursor":"cursor"});
        let mut response = output(&list);
        project_issue_links("https://app.example", "projects_list_issues", &mut response);
        let projected: serde_json::Value = serde_json::from_str(&response.content).unwrap();
        assert_eq!(projected["items"], list["items"]);
        assert_eq!(projected["next_cursor"], "cursor");
        assert_eq!(projected["_links"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn unsafe_origins_and_non_issue_tools_never_project_links() {
        for origin in [
            "https://user:password@app.example",
            "https://app.example/path",
            "https://app.example?x",
            "https://app.example#x",
            "http://remote.example",
            "file:///tmp/issues",
        ] {
            assert!(
                validate_config(&ProjectsAgentWebConfig {
                    origin: Some(origin.into())
                })
                .is_err()
            );
            let mut response = output(
                &serde_json::json!({"issue_id":"x","organization_id":"org","title":"title"}),
            );
            let original = response.content.clone();
            project_issue_links(origin, "projects_get_issue", &mut response);
            assert_eq!(response.content, original);
        }
        assert!(validate_config(&ProjectsAgentWebConfig::default()).is_ok());
        assert!(clean_origin("http://127.0.0.1:8080").is_some());
        let mut response =
            output(&serde_json::json!({"issue_id":"x","organization_id":"org","title":"title"}));
        let original = response.content.clone();
        project_issue_links("https://app.example", "projects_add_comment", &mut response);
        assert_eq!(response.content, original);
    }
}
