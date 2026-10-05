use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan, ResolvedAppPlan,
};
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionIssuer, ActorProjectionError, FixedClock, TypedActor, Validity,
    audience,
};
use lenso_capability_agent_prompt_provider as prompts;
use lenso_capability_agent_tool_provider as tools;
use lenso_capability_projects as projects;
use lenso_capability_projects_collaboration as collaboration;
use lenso_kernel::{
    CancellationToken, InvocationContext, Kernel, NativeRequestFuture, RuntimeFailure,
    ShutdownOutcome,
};
use lenso_native_adapter::{
    NativePluginFactory, NativePluginFactoryContext, NativePluginInstance, NativePluginRegistry,
};
use lenso_runner::TokioDriver;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration as StdDuration};
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

#[derive(Debug)]
struct User(String);
impl TypedActor for User {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        Ok(Self(assertion.subject().into()))
    }
}

#[derive(Clone, Debug)]
struct Owner {
    issuer: ActorAssertionIssuer,
    calls: Rc<RefCell<Vec<(u64, collaboration::SetIssueAssigneeRequest)>>>,
}
impl NativePluginFactory for Owner {
    fn package_id(&self) -> &'static str {
        "test.owner"
    }
    fn instantiate(
        &self,
        _: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::new(vec![
            Rc::new(projects::ProjectsEndpoint::new(self.clone())),
            Rc::new(collaboration::ProjectsCollaborationEndpoint::new(
                self.clone(),
            )),
        ]))
    }
}
// Unused business operations fail explicitly; this fixture only owns assignment.
macro_rules! unavailable {
    ($module:ident; $($method:ident, $request:ident, $operation:ident);+ $(;)?) => { $(
        fn $method(&self, _: InvocationContext, _: $module::$request) -> NativeRequestFuture<$module::$operation> {
            Box::pin(async { Err(RuntimeFailure::Unavailable { capability: $module::CAPABILITY_ID }) })
        }
    )+ };
}
impl projects::ProjectsProvider for Owner {
    unavailable!(projects;
        archive_issue, ArchiveIssueRequest, ProjectsArchiveIssue;
        archive_project, ArchiveProjectRequest, ProjectsArchiveProject;
        create_issue, CreateIssueRequest, ProjectsCreateIssue;
        create_project, CreateProjectRequest, ProjectsCreateProject;
        get_issue, GetIssueRequest, ProjectsGetIssue;
        get_project, GetProjectRequest, ProjectsGetProject;
        list_activity, ListActivityRequest, ProjectsListActivity;
        list_issue_workflow_states, ListIssueWorkflowStatesRequest, ProjectsListIssueWorkflowStates;
        list_issues, ListIssuesRequest, ProjectsListIssues;
        list_projects, ListProjectsRequest, ProjectsListProjects;
        move_issue, MoveIssueRequest, ProjectsMoveIssue;
        put_external_link, PutExternalLinkRequest, ProjectsPutExternalLink;
        update_issue, UpdateIssueRequest, ProjectsUpdateIssue;
        update_project, UpdateProjectRequest, ProjectsUpdateProject;
    );
}
impl collaboration::ProjectsCollaborationProvider for Owner {
    unavailable!(collaboration;
        add_comment, AddCommentRequest, ProjectsCollaborationAddComment;
        add_issue_relation, AddIssueRelationRequest, ProjectsCollaborationAddIssueRelation;
        create_project_update, CreateProjectUpdateRequest, ProjectsCollaborationCreateProjectUpdate;
        delete_comment, DeleteCommentRequest, ProjectsCollaborationDeleteComment;
        get_issue_assignee, GetIssueAssigneeRequest, ProjectsCollaborationGetIssueAssignee;
        list_comments, ListCommentsRequest, ProjectsCollaborationListComments;
        list_project_updates, ListProjectUpdatesRequest, ProjectsCollaborationListProjectUpdates;
        remove_issue_relation, RemoveIssueRelationRequest, ProjectsCollaborationRemoveIssueRelation;
        update_comment, UpdateCommentRequest, ProjectsCollaborationUpdateComment;
    );
    fn set_issue_assignee(
        &self,
        context: InvocationContext,
        request: collaboration::SetIssueAssigneeRequest,
    ) -> NativeRequestFuture<collaboration::ProjectsCollaborationSetIssueAssignee> {
        assert_eq!(context.caller_instance(), Some("tools"));
        let user = self
            .issuer
            .verifier()
            .project_context::<User>(
                &context,
                collaboration::CAPABILITY_ID,
                collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
                &FixedClock::new(OffsetDateTime::now_utc()),
            )
            .unwrap();
        assert_eq!(request.assignee_subject.as_deref(), Some(user.0.as_str()));
        self.calls
            .borrow_mut()
            .push((context.request_id(), request.clone()));
        Box::pin(async move {
            if request.organization_id == "denied" {
                return Ok(Err(collaboration::SetIssueAssigneeError::Forbidden));
            }
            if request.expected_revision != "3" {
                return Ok(Err(collaboration::SetIssueAssigneeError::RevisionConflict));
            }
            Ok(Ok(collaboration::SetIssueAssigneeResponse {
                assignee_subject: request.assignee_subject,
                issue_id: request.issue_id,
                organization_id: request.organization_id,
                revision: "4".into(),
            }))
        })
    }
}

fn plan(issuer: &ActorAssertionIssuer) -> ResolvedAppPlan {
    let adapter = PluginInstancePlan::new("tools", "lenso.projects.agent-tools")
        .with_configuration(serde_json::json!({"auth_issuer":"auth","auth_assertion_public_key":issuer.public_key_base64()}).to_string())
        .with_capability(CapabilityEndpointPlan::new(tools::CAPABILITY_ID, tools::DESCRIPTOR_VERSION, [tools::CATALOG_OPERATION, tools::EXECUTE_OPERATION]))
        .with_capability(CapabilityEndpointPlan::new(prompts::CAPABILITY_ID, prompts::DESCRIPTOR_VERSION, [prompts::CONTRIBUTE_OPERATION]))
        .with_requirement(CapabilityRequirementPlan::one(projects::CAPABILITY_ID, projects::DESCRIPTOR_VERSION))
        .with_requirement(CapabilityRequirementPlan::one(collaboration::CAPABILITY_ID, collaboration::DESCRIPTOR_VERSION));
    let owner = PluginInstancePlan::new("owner", "test.owner")
        .with_capability(CapabilityEndpointPlan::new(
            projects::CAPABILITY_ID,
            projects::DESCRIPTOR_VERSION,
            [
                projects::ARCHIVE_ISSUE_OPERATION,
                projects::ARCHIVE_PROJECT_OPERATION,
                projects::CREATE_ISSUE_OPERATION,
                projects::CREATE_PROJECT_OPERATION,
                projects::GET_ISSUE_OPERATION,
                projects::GET_PROJECT_OPERATION,
                projects::LIST_ACTIVITY_OPERATION,
                projects::LIST_ISSUE_WORKFLOW_STATES_OPERATION,
                projects::LIST_ISSUES_OPERATION,
                projects::LIST_PROJECTS_OPERATION,
                projects::MOVE_ISSUE_OPERATION,
                projects::PUT_EXTERNAL_LINK_OPERATION,
                projects::UPDATE_ISSUE_OPERATION,
                projects::UPDATE_PROJECT_OPERATION,
            ],
        ))
        .with_capability(CapabilityEndpointPlan::new(
            collaboration::CAPABILITY_ID,
            collaboration::DESCRIPTOR_VERSION,
            [
                collaboration::ADD_COMMENT_OPERATION,
                collaboration::ADD_ISSUE_RELATION_OPERATION,
                collaboration::CREATE_PROJECT_UPDATE_OPERATION,
                collaboration::DELETE_COMMENT_OPERATION,
                collaboration::GET_ISSUE_ASSIGNEE_OPERATION,
                collaboration::LIST_COMMENTS_OPERATION,
                collaboration::LIST_PROJECT_UPDATES_OPERATION,
                collaboration::REMOVE_ISSUE_RELATION_OPERATION,
                collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
                collaboration::UPDATE_COMMENT_OPERATION,
            ],
        ));
    let caller = PluginInstancePlan::new("caller", "test.caller").with_requirement(
        CapabilityRequirementPlan::one(tools::CAPABILITY_ID, tools::DESCRIPTOR_VERSION),
    );
    AppComposition::new(
        vec![adapter, owner, caller],
        vec![
            CapabilityBinding::new(
                "caller",
                tools::CAPABILITY_ID,
                tools::DESCRIPTOR_VERSION,
                "tools",
            ),
            CapabilityBinding::new(
                "tools",
                projects::CAPABILITY_ID,
                projects::DESCRIPTOR_VERSION,
                "owner",
            ),
            CapabilityBinding::new(
                "tools",
                collaboration::CAPABILITY_ID,
                collaboration::DESCRIPTOR_VERSION,
                "owner",
            ),
        ],
    )
    .resolve()
    .unwrap()
}

fn request(organization: &str, revision: &str) -> tools::ExecuteRequest {
    tools::ExecuteRequest { name: "projects_assign_issue_to_me".into(), arguments_json: serde_json::json!({"organization_id":organization,"issue_id":"issue","expected_revision":revision,"idempotency_key":"intent-1"}).to_string().parse().unwrap() }
}
fn context(
    issuer: &ActorAssertionIssuer,
    subject: &str,
    kind: &str,
    expires: OffsetDateTime,
    request_id: u64,
) -> InvocationContext {
    issuer
        .issue(
            subject,
            kind,
            "session",
            [
                audience(tools::CAPABILITY_ID, tools::EXECUTE_OPERATION),
                audience(
                    collaboration::CAPABILITY_ID,
                    collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
                ),
            ],
            Validity::new(OffsetDateTime::now_utc() - Duration::minutes(2), expires).unwrap(),
            BTreeMap::new(),
        )
        .attach(InvocationContext::new(
            request_id,
            None,
            CancellationToken::new(),
        ))
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn assignment_forwards_exact_actor_operation_identity_and_owner_denials() {
    tokio::task::LocalSet::new()
        .run_until(async {
            lenso_projects_agent_tools_plugin::link();
            let issuer = ActorAssertionIssuer::new("auth", b"test-owner-key");
            let owner = Owner {
                issuer: issuer.clone(),
                calls: Rc::default(),
            };
            let app = Kernel::start_native(
                plan(&issuer),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_linked_factories()
                    .with_factory(owner.clone())
                    .with_factory(Caller),
            )
            .await
            .unwrap();
            let now = OffsetDateTime::now_utc();
            for (index, subject) in ["account-a", "account-b"].into_iter().enumerate() {
                let request_id = 80 + index as u64;
                let result = app
                    .invoke_with_context::<tools::ToolProviderExecute>(
                        "caller",
                        tools::EXECUTE_OPERATION,
                        context(
                            &issuer,
                            subject,
                            "user",
                            now + Duration::minutes(1),
                            request_id,
                        ),
                        request("org", "3"),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let body: serde_json::Value = serde_json::from_str(&result.content).unwrap();
                assert_eq!(body["assignee_subject"], subject);
                assert_eq!(body["revision"], "4");
                let calls = owner.calls.borrow();
                let (forwarded_id, forwarded_request) = calls.last().unwrap();
                assert_eq!(*forwarded_id, request_id);
                assert_eq!(forwarded_request.idempotency_key, "intent-1");
                assert_eq!(forwarded_request.issue_id, "issue");
                assert_eq!(forwarded_request.expected_revision, "3");
            }
            for rejected in [
                InvocationContext::new(90, None, CancellationToken::new()),
                context(&issuer, "admin", "service", now + Duration::minutes(1), 91),
                context(&issuer, "admin", "user", now - Duration::seconds(1), 92),
                context(
                    &ActorAssertionIssuer::new("auth", b"untrusted-key"),
                    "admin",
                    "user",
                    now + Duration::minutes(1),
                    93,
                ),
            ] {
                assert_eq!(
                    app.invoke_with_context::<tools::ToolProviderExecute>(
                        "caller",
                        tools::EXECUTE_OPERATION,
                        rejected,
                        request("org", "3")
                    )
                    .await
                    .unwrap()
                    .unwrap_err(),
                    tools::ExecuteError::PermissionDenied
                );
            }
            assert_eq!(owner.calls.borrow().len(), 2);
            let denial = app
                .invoke_with_context::<tools::ToolProviderExecute>(
                    "caller",
                    tools::EXECUTE_OPERATION,
                    context(&issuer, "account-a", "user", now + Duration::minutes(1), 94),
                    request("denied", "3"),
                )
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(denial, tools::ExecuteError::PermissionDenied);
            let conflict = app
                .invoke_with_context::<tools::ToolProviderExecute>(
                    "caller",
                    tools::EXECUTE_OPERATION,
                    context(&issuer, "account-a", "user", now + Duration::minutes(1), 95),
                    request("org", "2"),
                )
                .await
                .unwrap()
                .unwrap_err();
            let tools::ExecuteError::ExecutionFailed { payload } = conflict else {
                panic!("revision conflict must survive");
            };
            assert_eq!(payload.reason_code, "revision_conflict");
            assert_eq!(
                app.shutdown(StdDuration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
        })
        .await;
}
