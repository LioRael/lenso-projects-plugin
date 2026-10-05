//! Agent-facing Tools over explicitly bound Projects capabilities.

use lenso::prelude::*;
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionVerifier, ActorProjectionError, FixedClock, TypedActor,
};
use lenso_capability_agent_prompt_provider as prompts;
use lenso_capability_agent_tool_provider::{
    self as tool_contract, CatalogRequest, CatalogResponse, ContentType, ExecuteError,
    ExecuteRequest, ExecuteResponse, ExecutionFailedPayload, ToolDefinition, ToolExecutionClass,
};
use lenso_capability_projects::{
    self as projects, CreateIssueRequest, GetIssueRequest, GetProjectRequest, ListIssuesRequest,
    ListProjectsRequest, MoveIssueRequest, UpdateIssueRequest,
};
use lenso_capability_projects_collaboration::{
    self as collaboration, AddCommentRequest, ListCommentsRequest,
};
use lenso_kernel::RuntimeFailure;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use time::OffsetDateTime;

pub const LIST_PROJECTS_TOOL: &str = "projects_list_projects";
pub const GET_PROJECT_TOOL: &str = "projects_get_project";
pub const LIST_ISSUES_TOOL: &str = "projects_list_issues";
pub const LIST_ISSUE_WORKFLOW_STATES_TOOL: &str = "projects_list_issue_workflow_states";
pub const GET_ISSUE_TOOL: &str = "projects_get_issue";
pub const CREATE_ISSUE_TOOL: &str = "projects_create_issue";
pub const UPDATE_ISSUE_TOOL: &str = "projects_update_issue";
pub const MOVE_ISSUE_TOOL: &str = "projects_move_issue";
pub const LIST_COMMENTS_TOOL: &str = "projects_list_comments";
pub const ADD_COMMENT_TOOL: &str = "projects_add_comment";
pub const ASSIGN_SELF_TOOL: &str = "projects_assign_issue_to_me";

/// Verification authority selected by the App, shared with its Projects owner.
#[derive(Clone, Debug, Default, Deserialize, Serialize, lenso::PluginConfig)]
#[serde(deny_unknown_fields)]
pub struct ProjectsAgentToolsConfig {
    #[serde(default)]
    pub auth_issuer: Option<String>,
    #[serde(default)]
    pub auth_assertion_public_key: Option<String>,
}

impl ProjectsAgentToolsConfig {
    fn verifier(&self) -> Result<Option<ActorAssertionVerifier>, RuntimeFailure> {
        let (Some(issuer), Some(public_key)) = (&self.auth_issuer, &self.auth_assertion_public_key)
        else {
            return if self.auth_issuer.is_none() && self.auth_assertion_public_key.is_none() {
                Ok(None)
            } else {
                Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "Projects Agent Auth issuer and verification key must be configured together".into(),
                })
            };
        };
        if issuer.trim().is_empty() {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: "Projects Agent Auth issuer is required".into(),
            });
        }
        ActorAssertionVerifier::from_public_key_base64(issuer.clone(), public_key)
            .map(Some)
            .map_err(|_| RuntimeFailure::InvalidResolvedPlan {
                detail: "Projects Agent Auth verification key is invalid".into(),
            })
    }
}

fn validate_config(config: &ProjectsAgentToolsConfig) -> Result<(), RuntimeFailure> {
    config.verifier().map(|_| ())
}

#[derive(Debug)]
struct AuthenticatedUser(String);
impl TypedActor for AuthenticatedUser {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        if assertion.actor_kind() != "user" {
            return Err(ActorProjectionError::UnexpectedActorKind {
                expected: "user".into(),
                actual: assertion.actor_kind().into(),
            });
        }
        Ok(Self(assertion.subject().to_owned()))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignSelfRequest {
    organization_id: String,
    issue_id: String,
    expected_revision: String,
    idempotency_key: String,
}

fn self_assignment(
    config: &ProjectsAgentToolsConfig,
    context: &Ctx,
    request: &ExecuteRequest,
) -> PluginResult<collaboration::SetIssueAssigneeRequest, ExecuteError> {
    let arguments = decode::<AssignSelfRequest>(request)?;
    let verifier = config
        .verifier()
        .map_err(PluginError::runtime)?
        .ok_or_else(|| PluginError::domain(ExecuteError::PermissionDenied))?;
    let clock = FixedClock::new(OffsetDateTime::now_utc());
    let user = verifier
        .project_context::<AuthenticatedUser>(
            context,
            tool_contract::CAPABILITY_ID,
            tool_contract::EXECUTE_OPERATION,
            &clock,
        )
        .map_err(|_| PluginError::domain(ExecuteError::PermissionDenied))?;
    // The downstream owner independently verifies the same sealed assertion,
    // membership, permission, visibility, revision and idempotency on every call.
    verifier
        .project_context::<AuthenticatedUser>(
            context,
            collaboration::CAPABILITY_ID,
            collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
            &clock,
        )
        .map_err(|_| PluginError::domain(ExecuteError::PermissionDenied))?;
    Ok(collaboration::SetIssueAssigneeRequest {
        organization_id: arguments.organization_id,
        issue_id: arguments.issue_id,
        expected_revision: arguments.expected_revision,
        idempotency_key: arguments.idempotency_key,
        assignee_subject: Some(user.0),
    })
}

#[lenso::plugin(validate=validate_config)]
#[derive(Clone, Debug)]
struct ProjectsAgentToolsPlugin {
    #[config]
    config: ProjectsAgentToolsConfig,
    projects: Port<projects::ProjectsClient>,
    collaboration: Port<collaboration::ProjectsCollaborationClient>,
}

#[lenso::provides(tool_contract::ToolProvider, prompts::PromptProvider)]
impl ProjectsAgentToolsPlugin {
    async fn contribute(
        &self,
        _context: Ctx,
        _request: prompts::ContributeRequest,
    ) -> PluginResult<prompts::ContributeResponse, prompts::ContributeError> {
        Ok(projects_prompt())
    }
    fn catalog(
        &self,
        _context: Ctx,
        _request: CatalogRequest,
    ) -> impl std::future::Future<Output = PluginResult<CatalogResponse, tool_contract::CatalogError>>
    {
        let _ = self;
        futures::future::ready(Ok(CatalogResponse {
            tools: tool_definitions(),
        }))
    }

    // Keep the typed Tool-to-Capability dispatch visible in one match.
    #[allow(clippy::too_many_lines)]
    async fn execute(
        &self,
        context: Ctx,
        request: ExecuteRequest,
    ) -> PluginResult<ExecuteResponse, ExecuteError> {
        macro_rules! invoke {
            ($future:expr, $tool:expr, $domain:path, $runtime:path) => {
                match $future.await {
                    Ok(response) => success($tool, &response),
                    Err($domain(error)) => Err(PluginError::domain(map_domain_error(&error))),
                    Err($runtime(error)) => Err(PluginError::runtime(error)),
                }
            };
        }

        match request.name.as_str() {
            ASSIGN_SELF_TOOL => {
                let arguments = self_assignment(&self.config, &context, &request)?;
                invoke!(
                    self.collaboration
                        .set_issue_assignee_with_context(context, arguments),
                    ASSIGN_SELF_TOOL,
                    collaboration::ProjectsCollaborationSetIssueAssigneeInvocationError::Domain,
                    collaboration::ProjectsCollaborationSetIssueAssigneeInvocationError::Runtime
                )
            }
            LIST_PROJECTS_TOOL => {
                let arguments = decode::<ListProjectsRequest>(&request)?;
                invoke!(
                    self.projects.list_projects_with_context(context, arguments),
                    LIST_PROJECTS_TOOL,
                    projects::ProjectsListProjectsInvocationError::Domain,
                    projects::ProjectsListProjectsInvocationError::Runtime
                )
            }
            GET_PROJECT_TOOL => {
                let arguments = decode::<GetProjectRequest>(&request)?;
                invoke!(
                    self.projects.get_project_with_context(context, arguments),
                    GET_PROJECT_TOOL,
                    projects::ProjectsGetProjectInvocationError::Domain,
                    projects::ProjectsGetProjectInvocationError::Runtime
                )
            }
            LIST_ISSUES_TOOL => {
                let arguments = decode::<ListIssuesRequest>(&request)?;
                invoke!(
                    self.projects.list_issues_with_context(context, arguments),
                    LIST_ISSUES_TOOL,
                    projects::ProjectsListIssuesInvocationError::Domain,
                    projects::ProjectsListIssuesInvocationError::Runtime
                )
            }
            LIST_ISSUE_WORKFLOW_STATES_TOOL => {
                let arguments = decode::<projects::ListIssueWorkflowStatesRequest>(&request)?;
                invoke!(
                    self.projects
                        .list_issue_workflow_states_with_context(context, arguments),
                    LIST_ISSUE_WORKFLOW_STATES_TOOL,
                    projects::ProjectsListIssueWorkflowStatesInvocationError::Domain,
                    projects::ProjectsListIssueWorkflowStatesInvocationError::Runtime
                )
            }
            GET_ISSUE_TOOL => {
                let arguments = decode::<GetIssueRequest>(&request)?;
                invoke!(
                    self.projects.get_issue_with_context(context, arguments),
                    GET_ISSUE_TOOL,
                    projects::ProjectsGetIssueInvocationError::Domain,
                    projects::ProjectsGetIssueInvocationError::Runtime
                )
            }
            CREATE_ISSUE_TOOL => {
                let arguments = decode::<CreateIssueRequest>(&request)?;
                invoke!(
                    self.projects.create_issue_with_context(context, arguments),
                    CREATE_ISSUE_TOOL,
                    projects::ProjectsCreateIssueInvocationError::Domain,
                    projects::ProjectsCreateIssueInvocationError::Runtime
                )
            }
            UPDATE_ISSUE_TOOL => {
                let arguments = decode::<UpdateIssueRequest>(&request)?;
                invoke!(
                    self.projects.update_issue_with_context(context, arguments),
                    UPDATE_ISSUE_TOOL,
                    projects::ProjectsUpdateIssueInvocationError::Domain,
                    projects::ProjectsUpdateIssueInvocationError::Runtime
                )
            }
            MOVE_ISSUE_TOOL => {
                let arguments = decode::<MoveIssueRequest>(&request)?;
                invoke!(
                    self.projects.move_issue_with_context(context, arguments),
                    MOVE_ISSUE_TOOL,
                    projects::ProjectsMoveIssueInvocationError::Domain,
                    projects::ProjectsMoveIssueInvocationError::Runtime
                )
            }
            LIST_COMMENTS_TOOL => {
                let arguments = decode::<ListCommentsRequest>(&request)?;
                invoke!(
                    self.collaboration
                        .list_comments_with_context(context, arguments),
                    LIST_COMMENTS_TOOL,
                    collaboration::ProjectsCollaborationListCommentsInvocationError::Domain,
                    collaboration::ProjectsCollaborationListCommentsInvocationError::Runtime
                )
            }
            "projects_get_issue_assignee" => {
                let arguments = decode::<collaboration::GetIssueAssigneeRequest>(&request)?;
                invoke!(
                    self.collaboration
                        .get_issue_assignee_with_context(context, arguments),
                    "projects_get_issue_assignee",
                    collaboration::ProjectsCollaborationGetIssueAssigneeInvocationError::Domain,
                    collaboration::ProjectsCollaborationGetIssueAssigneeInvocationError::Runtime
                )
            }
            "projects_set_issue_assignee" => {
                let arguments = decode::<collaboration::SetIssueAssigneeRequest>(&request)?;
                invoke!(
                    self.collaboration
                        .set_issue_assignee_with_context(context, arguments),
                    "projects_set_issue_assignee",
                    collaboration::ProjectsCollaborationSetIssueAssigneeInvocationError::Domain,
                    collaboration::ProjectsCollaborationSetIssueAssigneeInvocationError::Runtime
                )
            }
            ADD_COMMENT_TOOL => {
                let arguments = decode::<AddCommentRequest>(&request)?;
                invoke!(
                    self.collaboration
                        .add_comment_with_context(context, arguments),
                    ADD_COMMENT_TOOL,
                    collaboration::ProjectsCollaborationAddCommentInvocationError::Domain,
                    collaboration::ProjectsCollaborationAddCommentInvocationError::Runtime
                )
            }
            _ => Err(PluginError::domain(ExecuteError::NotFound)),
        }
    }
}

fn tool_definitions() -> Vec<ToolDefinition> {
    vec![
        tool(
            ASSIGN_SELF_TOOL,
            "Assign this Issue to the authenticated user of this Projects App. Use this for 'assign to me'; never guess a subject ID. Supply the latest Issue revision and a stable idempotency key. Other Issue fields remain unchanged.",
            r#"{"type":"object","additionalProperties":false,"required":["organization_id","issue_id","expected_revision","idempotency_key"],"properties":{"organization_id":{"type":"string"},"issue_id":{"type":"string"},"expected_revision":{"type":"string"},"idempotency_key":{"type":"string"}}}"#,
            ToolExecutionClass::Exclusive,
        ),
        tool(
            "projects_get_issue_assignee",
            "Read the assignee of a visible Issue. Assignment requires an active organization member who can access the Issue; use the current revision and a stable idempotency key for writes.",
            include_str!(
                "../../lenso-capability-projects-collaboration/schemas/get-issue-assignee-request.schema.json"
            ),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            "projects_set_issue_assignee",
            "Set the assignee of a visible Issue. Assignment requires an active organization member who can access the Issue; use the current revision and a stable idempotency key for writes.",
            include_str!(
                "../../lenso-capability-projects-collaboration/schemas/set-issue-assignee-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            LIST_PROJECTS_TOOL,
            "List Projects visible to the current actor with bounded cursor pagination.",
            include_str!(
                "../../lenso-capability-projects/schemas/list-projects-request.schema.json"
            ),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            GET_PROJECT_TOOL,
            "Get one visible Project by its stable ID, including the current revision.",
            include_str!("../../lenso-capability-projects/schemas/get-project-request.schema.json"),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            LIST_ISSUES_TOOL,
            "List visible Issues with optional Project, Team, and workflow-state filters.",
            include_str!("../../lenso-capability-projects/schemas/list-issues-request.schema.json"),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            LIST_ISSUE_WORKFLOW_STATES_TOOL,
            "List workflow states for a visible Team before updating an Issue. Choose an unarchived state_id; follow next_cursor for more states.",
            include_str!(
                "../../lenso-capability-projects/schemas/list-issue-workflow-states-request.schema.json"
            ),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            GET_ISSUE_TOOL,
            "Get one visible Issue by stable ID, current identifier, or historical identifier. For /projects?organization_id=ORG&issue=ID App links, use ORG and ID as the request values.",
            include_str!("../../lenso-capability-projects/schemas/get-issue-request.schema.json"),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            CREATE_ISSUE_TOOL,
            "Create one Issue. Supply a stable issue_id and reuse the same idempotency_key when retrying the same intent.",
            include_str!(
                "../../lenso-capability-projects/schemas/create-issue-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            UPDATE_ISSUE_TOOL,
            "Replace the editable fields of one Issue using the revision returned by get_issue. Reuse the same idempotency_key for retries.",
            include_str!(
                "../../lenso-capability-projects/schemas/update-issue-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            MOVE_ISSUE_TOOL,
            "Move one Issue to a Team and workflow state using its current revision. Reuse the same idempotency_key for retries.",
            include_str!("../../lenso-capability-projects/schemas/move-issue-request.schema.json"),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            LIST_COMMENTS_TOOL,
            "List visible comments for one Issue with bounded cursor pagination.",
            include_str!(
                "../../lenso-capability-projects-collaboration/schemas/list-comments-request.schema.json"
            ),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            ADD_COMMENT_TOOL,
            "Add one comment to an Issue. Supply a stable comment_id and reuse the same idempotency_key for retries.",
            include_str!(
                "../../lenso-capability-projects-collaboration/schemas/add-comment-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
    ]
}

fn projects_prompt() -> prompts::ContributeResponse {
    prompts::ContributeResponse {
        contributions: vec![prompts::ContributeResponseContributionsItem {
            id: "projects-business-workflow".into(),
            kind: prompts::ContributeResponseContributionsItemKind::Instruction,
            version: "1".into(),
            content: "For Projects business tasks, use the supplied Projects tools, not browser guesses. Resolve 'assign to me' with projects_assign_issue_to_me; never infer the connected user's identity. After completing an Issue task, include a Markdown link to that Issue. Copy the exact current-page URL supplied by the user, or an exact _links URL returned by Projects tools. Never invent a hostname, workspace slug, or Issue URL. If a link is requested and no verified URL is available, read the Issue again. Preserve unrelated fields and handle revision conflicts explicitly.".into(),
        }],
    }
}

fn tool(
    name: &str,
    description: &str,
    schema: &str,
    execution: ToolExecutionClass,
) -> ToolDefinition {
    let schema: serde_json::Value =
        serde_json::from_str(schema).expect("Projects Tool schema must be valid JSON");
    ToolDefinition {
        name: name.to_owned(),
        description: description.to_owned(),
        input_schema_json: schema
            .to_string()
            .try_into()
            .expect("Projects Tool schema must remain valid JSON"),
        execution,
    }
}

fn decode<T: DeserializeOwned>(request: &ExecuteRequest) -> PluginResult<T, ExecuteError> {
    serde_json::from_str(request.arguments_json.as_str())
        .map_err(|_| PluginError::domain(ExecuteError::InvalidArguments))
}

fn success<T: Serialize>(
    tool_name: &str,
    response: &T,
) -> PluginResult<ExecuteResponse, ExecuteError> {
    let content = serde_json::to_string_pretty(response).map_err(|error| {
        PluginError::runtime(RuntimeFailure::PluginFailure {
            detail: format!("Projects Tool could not serialize its typed response: {error}"),
        })
    })?;
    Ok(ExecuteResponse {
        content_blocks: None,
        content,
        content_type: ContentType::Text,
        metadata_json: serde_json::json!({ "tool": tool_name })
            .to_string()
            .try_into()
            .expect("Projects Tool metadata must be valid JSON"),
    })
}

trait DomainToolError {
    fn to_tool_error(&self) -> ExecuteError;
}

fn map_domain_error(error: &impl DomainToolError) -> ExecuteError {
    error.to_tool_error()
}

fn rejected(reason_code: &str) -> ExecuteError {
    ExecuteError::ExecutionFailed {
        payload: ExecutionFailedPayload {
            reason_code: reason_code.to_owned(),
            message: match reason_code {
                "revision_conflict" => "This issue changed since it was read. Read it again, compare the requested edit with the latest state, and ask the user if those changes conflict. Do not overwrite the newer revision automatically.",
                "idempotency_conflict" => "This request key was already used for a different edit. Read the issue to verify its current state before creating a new request.",
                "workflow_state_not_found" => "The selected workflow state is unavailable. Read this Team's current workflow states before updating the issue.",
                _ => "Projects rejected the requested operation.",
            }.to_owned(),
            details_json: serde_json::json!({ "domain_error": reason_code })
                .to_string()
                .try_into()
                .expect("Projects Tool error metadata must be valid JSON"),
        },
    }
}

macro_rules! impl_projects_domain_error {
    ($($error:ty),+ $(,)?) => {
        $(
            impl DomainToolError for $error {
                fn to_tool_error(&self) -> ExecuteError {
                    match self {
                        Self::InvalidRequest => ExecuteError::InvalidArguments,
                        Self::NotFound => ExecuteError::NotFound,
                        Self::Forbidden | Self::PrivateTeam | Self::Unauthenticated => {
                            ExecuteError::PermissionDenied
                        }
                        Self::CycleNotFound => rejected("cycle_not_found"),
                        Self::IdempotencyConflict => rejected("idempotency_conflict"),
                        Self::IdentifierConflict => rejected("identifier_conflict"),
                        Self::LabelNotFound => rejected("label_not_found"),
                        Self::MilestoneNotFound => rejected("milestone_not_found"),
                        Self::ParentNotFound => rejected("parent_not_found"),
                        Self::ProjectStatusNotFound => rejected("project_status_not_found"),
                        Self::RevisionConflict => rejected("revision_conflict"),
                        Self::TeamNotFound => rejected("team_not_found"),
                        Self::WorkflowStateNotFound => rejected("workflow_state_not_found"),
                        Self::Unknown(_) => rejected("unknown_domain_error"),
                    }
                }
            }
        )+
    };
}

impl_projects_domain_error!(
    projects::CreateIssueError,
    projects::GetIssueError,
    projects::GetProjectError,
    projects::ListIssuesError,
    projects::ListIssueWorkflowStatesError,
    projects::ListProjectsError,
    projects::MoveIssueError,
    projects::UpdateIssueError,
);

macro_rules! impl_collaboration_domain_error {
    ($($error:ty),+ $(,)?) => {
        $(
            impl DomainToolError for $error {
                fn to_tool_error(&self) -> ExecuteError {
                    match self {
                        Self::InvalidRequest => ExecuteError::InvalidArguments,
                        Self::NotFound => ExecuteError::NotFound,
                        Self::AuthorRequired
                        | Self::Forbidden
                        | Self::PrivateTeam
                        | Self::Unauthenticated => ExecuteError::PermissionDenied,
                        Self::CannotRelateSelf => rejected("cannot_relate_self"),
                        Self::IdempotencyConflict => rejected("idempotency_conflict"),
                        Self::RelationConflict => rejected("relation_conflict"),
                        Self::RevisionConflict => rejected("revision_conflict"),
                        Self::Unknown(_) => rejected("unknown_domain_error"),
                    }
                }
            }
        )+
    };
}

impl_collaboration_domain_error!(
    collaboration::GetIssueAssigneeError,
    collaboration::SetIssueAssigneeError,
    collaboration::AddCommentError,
    collaboration::ListCommentsError,
);

/// Retains this linked Plugin in native Host assemblies.
pub fn link() {}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(name: &str, arguments: &str) -> ExecuteRequest {
        ExecuteRequest {
            name: name.to_owned(),
            arguments_json: arguments.try_into().unwrap(),
        }
    }

    #[test]
    fn descriptor_is_a_removable_adapter_with_two_business_requirements() {
        let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        assert_eq!(descriptor["plugin_id"], "lenso.projects.agent-tools");
        let provided = descriptor["provided_capabilities"].as_array().unwrap();
        assert_eq!(provided.len(), 2);
        assert_eq!(
            provided
                .iter()
                .map(|entry| entry["capability_id"].as_str().unwrap())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from([
                "lenso.agent.prompt-provider@1",
                "lenso.agent.tool-provider@2"
            ]),
        );
        let required = descriptor["required_capabilities"].as_array().unwrap();
        let capabilities = required
            .iter()
            .map(|entry| entry["capability_id"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            capabilities,
            std::collections::BTreeSet::from([
                "lenso.projects-collaboration@1",
                "lenso.projects@1",
            ])
        );
    }

    #[test]
    fn catalog_has_seven_parallel_reads_and_six_exclusive_mutations() {
        let tools = tool_definitions();
        assert_eq!(tools.len(), 13);
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.execution == ToolExecutionClass::ParallelSafe)
                .count(),
            7
        );
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.execution == ToolExecutionClass::Exclusive)
                .count(),
            6
        );
        assert!(tools.iter().all(|tool| {
            let schema: serde_json::Value =
                serde_json::from_str(tool.input_schema_json.as_str()).unwrap();
            schema["additionalProperties"] == false
        }));
    }

    #[test]
    fn exact_capability_requests_decode_without_adapter_owned_business_fields() {
        let get = decode::<GetIssueRequest>(&request(
            GET_ISSUE_TOOL,
            r#"{"organization_id":"org-1","issue_ref":"ENG-42"}"#,
        ))
        .unwrap();
        assert_eq!(get.issue_ref, "ENG-42");

        assert!(
            decode::<GetIssueRequest>(&request(GET_ISSUE_TOOL, r#"{"issue_ref":42}"#)).is_err()
        );
    }

    #[test]
    fn authorization_and_revision_failures_remain_distinct() {
        assert_eq!(
            map_domain_error(&projects::GetIssueError::Forbidden),
            ExecuteError::PermissionDenied
        );
        assert_eq!(
            map_domain_error(&projects::GetIssueError::NotFound),
            ExecuteError::NotFound
        );
        let ExecuteError::ExecutionFailed { payload } =
            map_domain_error(&projects::MoveIssueError::RevisionConflict)
        else {
            panic!("revision conflict must remain an execution failure");
        };
        assert_eq!(payload.reason_code, "revision_conflict");
        assert!(payload.message.contains("Read it again"));
        assert!(payload.message.contains("Do not overwrite"));
    }

    #[test]
    fn self_assignment_requires_configured_signed_user_and_both_audiences() {
        use lenso_auth_sdk::{ActorAssertionIssuer, Validity, audience};
        use lenso_kernel::{CancellationToken, InvocationContext};
        use std::collections::BTreeMap;
        use time::Duration;
        let issuer = ActorAssertionIssuer::new("auth", b"trusted-test-key");
        let config = ProjectsAgentToolsConfig {
            auth_issuer: Some("auth".into()),
            auth_assertion_public_key: Some(issuer.public_key_base64()),
        };
        let request = request(
            ASSIGN_SELF_TOOL,
            r#"{"organization_id":"org","issue_id":"issue","expected_revision":"3","idempotency_key":"intent"}"#,
        );
        let blank = || InvocationContext::new(73, None, CancellationToken::new());
        let now = OffsetDateTime::now_utc();
        for subject in ["account-a", "account-b"] {
            let assertion = issuer.issue(
                subject,
                "user",
                "session",
                [
                    audience(
                        tool_contract::CAPABILITY_ID,
                        tool_contract::EXECUTE_OPERATION,
                    ),
                    audience(
                        collaboration::CAPABILITY_ID,
                        collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
                    ),
                ],
                Validity::new(now - Duration::minutes(1), now + Duration::minutes(1)).unwrap(),
                BTreeMap::new(),
            );
            let context = assertion.attach(blank()).unwrap();
            let assignment = self_assignment(&config, &context, &request).unwrap();
            assert_eq!(assignment.assignee_subject.as_deref(), Some(subject));
            assert_eq!(assignment.expected_revision, "3");
            assert_eq!(assignment.idempotency_key, "intent");
            assert_eq!(context.request_id(), 73);
        }
        assert!(self_assignment(&config, &blank(), &request).is_err());
        assert!(self_assignment(&ProjectsAgentToolsConfig::default(), &blank(), &request).is_err());
        for (signer, kind, expiry, audiences) in [
            (
                issuer.clone(),
                "service",
                now + Duration::minutes(1),
                vec![
                    audience(
                        tool_contract::CAPABILITY_ID,
                        tool_contract::EXECUTE_OPERATION,
                    ),
                    audience(
                        collaboration::CAPABILITY_ID,
                        collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
                    ),
                ],
            ),
            (
                issuer.clone(),
                "user",
                now - Duration::seconds(1),
                vec![
                    audience(
                        tool_contract::CAPABILITY_ID,
                        tool_contract::EXECUTE_OPERATION,
                    ),
                    audience(
                        collaboration::CAPABILITY_ID,
                        collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
                    ),
                ],
            ),
            (
                issuer.clone(),
                "user",
                now + Duration::minutes(1),
                vec![audience(
                    tool_contract::CAPABILITY_ID,
                    tool_contract::EXECUTE_OPERATION,
                )],
            ),
            (
                ActorAssertionIssuer::new("auth", b"other-key"),
                "user",
                now + Duration::minutes(1),
                vec![
                    audience(
                        tool_contract::CAPABILITY_ID,
                        tool_contract::EXECUTE_OPERATION,
                    ),
                    audience(
                        collaboration::CAPABILITY_ID,
                        collaboration::SET_ISSUE_ASSIGNEE_OPERATION,
                    ),
                ],
            ),
        ] {
            let assertion = signer.issue(
                "admin",
                kind,
                "session",
                audiences,
                Validity::new(now - Duration::minutes(2), expiry).unwrap(),
                BTreeMap::new(),
            );
            assert!(
                self_assignment(&config, &assertion.attach(blank()).unwrap(), &request).is_err()
            );
        }
    }

    #[test]
    fn self_assignment_rejects_model_identity_fields_and_partial_configuration() {
        assert!(
            validate_config(&ProjectsAgentToolsConfig {
                auth_issuer: Some("auth".into()),
                auth_assertion_public_key: None
            })
            .is_err()
        );
        for field in [
            "subject",
            "actor",
            "assignee_subject",
            "credential",
            "context",
        ] {
            let mut arguments = serde_json::json!({"organization_id":"org","issue_id":"issue","expected_revision":"3","idempotency_key":"intent"});
            arguments[field] = "other-account".into();
            assert!(
                decode::<AssignSelfRequest>(&request(ASSIGN_SELF_TOOL, &arguments.to_string()))
                    .is_err()
            );
        }
        let tool = tool_definitions()
            .into_iter()
            .find(|tool| tool.name == ASSIGN_SELF_TOOL)
            .unwrap();
        assert_eq!(tool.execution, ToolExecutionClass::Exclusive);
        assert!(
            projects_prompt().contributions[0]
                .content
                .contains(ASSIGN_SELF_TOOL)
        );
    }
}
