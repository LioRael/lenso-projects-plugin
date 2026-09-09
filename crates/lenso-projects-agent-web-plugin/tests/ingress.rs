use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan, ResolvedAppPlan,
};
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionIssuer, ActorProjectionError, FixedClock, TypedActor, Validity,
    audience, authenticated_response,
};
use lenso_capability_agent_tool_provider as tools;
use lenso_capability_auth as auth;
use lenso_capability_http_endpoint as http;
use lenso_kernel::{
    InvocationContext, Kernel, NativeRequestFuture, RuntimeFailure, ShutdownOutcome,
};
use lenso_native_adapter::{
    NativePluginFactory, NativePluginFactoryContext, NativePluginInstance, NativePluginRegistry,
};
use lenso_runner::TokioDriver;
use std::{
    collections::BTreeMap,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration as StdDuration,
};
use time::{Duration, OffsetDateTime};

#[derive(Debug)]
struct Caller;
impl NativePluginFactory for Caller {
    fn package_id(&self) -> &'static str {
        "test.caller"
    }
    fn instantiate(
        &self,
        _: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::default())
    }
}

#[derive(Clone, Debug)]
struct Fixtures {
    issuer: ActorAssertionIssuer,
    revoked: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}
impl NativePluginFactory for Fixtures {
    fn package_id(&self) -> &'static str {
        "test.ingress"
    }
    fn instantiate(
        &self,
        _: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::new(vec![
            Rc::new(auth::AuthEndpoint::new(self.clone())),
            Rc::new(tools::ToolProviderEndpoint::new(self.clone())),
        ]))
    }
}
impl auth::AuthProvider for Fixtures {
    fn authenticate(
        &self,
        _: InvocationContext,
        request: auth::AuthenticateRequest,
    ) -> NativeRequestFuture<auth::Auth> {
        let result = if self.revoked.load(Ordering::SeqCst) {
            Ok(Err(auth::AuthenticateError::Revoked))
        } else if let Some(credential) = request.credential {
            if credential.value == "bad" {
                Ok(Err(auth::AuthenticateError::Invalid))
            } else {
                let now = OffsetDateTime::now_utc();
                let assertion = self.issuer.issue(
                    "user-1",
                    if credential.value == "service" {
                        "service"
                    } else {
                        "user"
                    },
                    "session",
                    [
                        audience(tools::CAPABILITY_ID, tools::EXECUTE_OPERATION),
                        audience(tools::CAPABILITY_ID, tools::CATALOG_OPERATION),
                    ],
                    Validity::new(now - Duration::seconds(1), now + Duration::minutes(1)).unwrap(),
                    BTreeMap::new(),
                );
                Ok(Ok(authenticated_response(&assertion)))
            }
        } else {
            Ok(Err(auth::AuthenticateError::Invalid))
        };
        Box::pin(async move { result })
    }
}
#[derive(Debug)]
struct User;
impl TypedActor for User {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        assert_eq!(assertion.subject(), "user-1");
        Ok(Self)
    }
}
impl Fixtures {
    fn inspect(&self, context: &InvocationContext, operation: &str) {
        self.issuer
            .verifier()
            .project_context::<User>(
                context,
                tools::CAPABILITY_ID,
                operation,
                &FixedClock::new(OffsetDateTime::now_utc()),
            )
            .unwrap();
        assert!(!format!("{context:?}").contains("private-session-credential"));
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
impl tools::ToolProviderProvider for Fixtures {
    fn catalog(
        &self,
        _context: InvocationContext,
        _: tools::CatalogRequest,
    ) -> NativeRequestFuture<tools::ToolProviderCatalog> {
        // Projects tool descriptions contain no user data or execution authority.
        Box::pin(async { Ok(Ok(tools::CatalogResponse { tools: vec![] })) })
    }
    fn execute(
        &self,
        context: InvocationContext,
        request: tools::ExecuteRequest,
    ) -> NativeRequestFuture<tools::ToolProviderExecute> {
        self.inspect(&context, tools::EXECUTE_OPERATION);
        let result = match request.name.as_str() {
            "denied" => Ok(Err(tools::ExecuteError::PermissionDenied)),
            "missing" => Ok(Err(tools::ExecuteError::NotFound)),
            "runtime" => Err(RuntimeFailure::Unavailable {
                capability: tools::CAPABILITY_ID,
            }),
            _ => Ok(Ok(tools::ExecuteResponse {
                content_type: tools::ContentType::Text,
                content: "done".into(),
                content_blocks: None,
                metadata_json: "{}".parse().unwrap(),
            })),
        };
        Box::pin(async move { result })
    }
}

fn plan() -> ResolvedAppPlan {
    let web = PluginInstancePlan::new("web", "lenso.projects.agent-web")
        .with_capability(CapabilityEndpointPlan::new(
            http::CAPABILITY_ID,
            http::DESCRIPTOR_VERSION,
            [http::DESCRIBE_OPERATION, http::HANDLE_OPERATION],
        ))
        .with_requirement(CapabilityRequirementPlan::one(
            auth::CAPABILITY_ID,
            auth::DESCRIPTOR_VERSION,
        ))
        .with_requirement(CapabilityRequirementPlan::one(
            tools::CAPABILITY_ID,
            tools::DESCRIPTOR_VERSION,
        ));
    let fixtures = PluginInstancePlan::new("fixtures", "test.ingress")
        .with_capability(CapabilityEndpointPlan::new(
            auth::CAPABILITY_ID,
            auth::DESCRIPTOR_VERSION,
            [auth::AUTHENTICATE_OPERATION],
        ))
        .with_capability(CapabilityEndpointPlan::new(
            tools::CAPABILITY_ID,
            tools::DESCRIPTOR_VERSION,
            [tools::CATALOG_OPERATION, tools::EXECUTE_OPERATION],
        ));
    let caller = PluginInstancePlan::new("caller", "test.caller").with_requirement(
        CapabilityRequirementPlan::one(http::CAPABILITY_ID, http::DESCRIPTOR_VERSION),
    );
    AppComposition::new(
        vec![web, fixtures, caller],
        vec![
            CapabilityBinding::new(
                "caller",
                http::CAPABILITY_ID,
                http::DESCRIPTOR_VERSION,
                "web",
            ),
            CapabilityBinding::new(
                "web",
                auth::CAPABILITY_ID,
                auth::DESCRIPTOR_VERSION,
                "fixtures",
            ),
            CapabilityBinding::new(
                "web",
                tools::CAPABILITY_ID,
                tools::DESCRIPTOR_VERSION,
                "fixtures",
            ),
        ],
    )
    .resolve()
    .unwrap()
}
fn request(credential: Option<&str>, body: &str) -> http::HandleRequest {
    http::HandleRequest {
        body: body.as_bytes().to_vec().into(),
        credential: credential.map(|value| http::HandleRequestCredential {
            scheme: "session".into(),
            value: value.into(),
        }),
        headers: vec![],
        method: "POST".into(),
        path: "/projects/agent/tools/execute".into(),
        path_parameters: vec![],
        query: None,
        request_id: "request-1".into(),
        route_id: "projects.agent.execute".into(),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one native lifecycle verifies anonymous metadata and authenticated execution boundaries"
)]
#[tokio::test(flavor = "current_thread")]
async fn ingress_authenticates_each_call_and_preserves_failure_boundaries() {
    tokio::task::LocalSet::new()
        .run_until(async {
            lenso_projects_agent_web_plugin::link();
            let fixture = Fixtures {
                issuer: ActorAssertionIssuer::new("test.auth", b"test-ingress-signing"),
                revoked: Arc::new(AtomicBool::new(false)),
                calls: Arc::new(AtomicUsize::new(0)),
            };
            let app = Kernel::start_native(
                plan(),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_linked_factories()
                    .with_factory(fixture.clone())
                    .with_factory(Caller),
            )
            .await
            .unwrap();
            let mut manifest = request(None, "");
            manifest.method = "GET".into();
            manifest.path = "/projects/agent/manifest".into();
            manifest.route_id = "projects.agent.manifest".into();
            let descriptions = app
                .invoke::<http::EndpointHandle>("caller", http::HANDLE_OPERATION, manifest)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(descriptions.status, 200);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(descriptions.body.as_ref()).unwrap(),
                serde_json::json!({"tools":[]})
            );
            let body = r#"{"name":"read","arguments_json":"{}"}"#;
            for (credential, body, status) in [
                (None, body, 401),
                (Some("bad"), body, 401),
                (Some("service"), body, 403),
                (
                    Some("private-session-credential"),
                    r#"{"name":"read","arguments_json":"{}","subject":"admin"}"#,
                    400,
                ),
            ] {
                let response = app
                    .invoke::<http::EndpointHandle>(
                        "caller",
                        http::HANDLE_OPERATION,
                        request(credential, body),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(response.status, status);
            }
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
            for (name, status) in [("read", 200), ("denied", 403), ("missing", 404)] {
                let body = serde_json::json!({"name":name,"arguments_json":"{}"}).to_string();
                let response = app
                    .invoke::<http::EndpointHandle>(
                        "caller",
                        http::HANDLE_OPERATION,
                        request(Some("private-session-credential"), &body),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(response.status, status);
                assert!(
                    response
                        .headers
                        .iter()
                        .any(|h| h.name == "cache-control" && h.value == "no-store")
                );
                assert!(
                    !String::from_utf8_lossy(response.body.as_ref())
                        .contains("private-session-credential")
                );
            }
            let runtime = r#"{"name":"runtime","arguments_json":"{}"}"#;
            assert!(
                app.invoke::<http::EndpointHandle>(
                    "caller",
                    http::HANDLE_OPERATION,
                    request(Some("private-session-credential"), runtime)
                )
                .await
                .is_err()
            );
            fixture.revoked.store(true, Ordering::SeqCst);
            let response = app
                .invoke::<http::EndpointHandle>(
                    "caller",
                    http::HANDLE_OPERATION,
                    request(Some("private-session-credential"), body),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status, 401);
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 4);
            assert_eq!(
                app.shutdown(StdDuration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
        })
        .await;
}
