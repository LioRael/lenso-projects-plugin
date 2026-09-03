//! Agent-facing Tools over explicitly bound Projects capabilities.

use lenso::prelude::*;
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
use serde::{Serialize, de::DeserializeOwned};

pub const LIST_PROJECTS_TOOL: &str = "projects_list_projects";
pub const GET_PROJECT_TOOL: &str = "projects_get_project";
pub const LIST_ISSUES_TOOL: &str = "projects_list_issues";
pub const GET_ISSUE_TOOL: &str = "projects_get_issue";
pub const CREATE_ISSUE_TOOL: &str = "projects_create_issue";
pub const UPDATE_ISSUE_TOOL: &str = "projects_update_issue";
pub const MOVE_ISSUE_TOOL: &str = "projects_move_issue";
pub const LIST_COMMENTS_TOOL: &str = "projects_list_comments";
pub const ADD_COMMENT_TOOL: &str = "projects_add_comment";

#[lenso::plugin]
#[derive(Clone, Debug)]
struct ProjectsAgentToolsPlugin {
    projects: Port<projects::ProjectsClient>,
    collaboration: Port<collaboration::ProjectsCollaborationClient>,
}

#[lenso::provides(tool_contract::ToolProvider)]
impl ProjectsAgentToolsPlugin {
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
            GET_ISSUE_TOOL,
            "Get one visible Issue by stable ID, current identifier, or historical identifier.",
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
            message: "Projects rejected the requested operation.".to_owned(),
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
    collaboration::AddCommentError,
    collaboration::ListCommentsError,
);

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
        assert_eq!(provided.len(), 1);
        assert_eq!(provided[0]["capability_id"], "lenso.agent.tool-provider@2");
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
    fn catalog_has_five_parallel_reads_and_four_exclusive_mutations() {
        let tools = tool_definitions();
        assert_eq!(tools.len(), 9);
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.execution == ToolExecutionClass::ParallelSafe)
                .count(),
            5
        );
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.execution == ToolExecutionClass::Exclusive)
                .count(),
            4
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
    }
}
