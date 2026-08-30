//! PostgreSQL-backed, Linear-like Projects behavior for Lenso.

// Handler grouping follows the generated Capability endpoint layout, while request validators
// intentionally accept generated optional fields by reference.
#![allow(clippy::items_after_test_module, clippy::ref_option)]

mod operator;
mod schema;
mod storage;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, time::Duration};

use lenso::prelude::*;
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionVerifier, ActorProjectionError, AssertionClock, TypedActor,
};
use lenso_capability_access_control as access;
use lenso_capability_access_control::{
    AccessControlInvocationError, CheckPermissionRequest, CheckPermissionRequestScope,
};
use lenso_capability_data_export_source as export_source;
use lenso_capability_organization_membership as membership;
use lenso_capability_organization_membership::{
    CheckMembershipRequest, OrganizationMembershipInvocationError,
};
use lenso_capability_projects as projects;
use lenso_capability_projects_admin as admin;
use lenso_capability_projects_collaboration as collaboration;
use lenso_capability_retention_participant as retention;
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsInvocationError};
use lenso_kernel::{PluginDependencies, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use zeroize::Zeroizing;

use crate::storage::{DomainFailure, StorageError};

pub use operator::{ProjectsOperator, ProjectsOperatorError};

const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CALLERS: usize = 64;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectsConfig {
    schema: String,
    database_url_secret: String,
    auth_issuer: String,
    auth_assertion_public_key: String,
    project_callers: Vec<String>,
    admin_callers: Vec<String>,
    governance_callers: Vec<String>,
}

impl ProjectsConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        auth_issuer: impl Into<String>,
        auth_assertion_public_key: impl Into<String>,
        project_callers: Vec<String>,
        admin_callers: Vec<String>,
        governance_callers: Vec<String>,
    ) -> Result<Self, ProjectsConfigError> {
        let value = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            auth_issuer: auth_issuer.into(),
            auth_assertion_public_key: auth_assertion_public_key.into(),
            project_callers,
            admin_callers,
            governance_callers,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ProjectsConfigError> {
        schema::schema_plan(self.schema.clone()).map_err(|_| ProjectsConfigError::InvalidSchema)?;
        if !valid_secret_reference(&self.database_url_secret) {
            return Err(ProjectsConfigError::InvalidSecretReference);
        }
        if !valid_id(&self.auth_issuer) {
            return Err(ProjectsConfigError::InvalidAuthIssuer);
        }
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| ProjectsConfigError::InvalidAuthPublicKey)?;
        if !valid_callers(&self.project_callers) {
            return Err(ProjectsConfigError::InvalidProjectCallers);
        }
        if !valid_callers(&self.admin_callers) {
            return Err(ProjectsConfigError::InvalidAdminCallers);
        }
        if !valid_callers(&self.governance_callers) {
            return Err(ProjectsConfigError::InvalidGovernanceCallers);
        }
        Ok(())
    }
    fn verifier(&self) -> Result<ActorAssertionVerifier, RuntimeFailure> {
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| RuntimeFailure::InvalidResolvedPlan {
            detail: "Projects Auth verification key is invalid".to_owned(),
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProjectsConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid database URL secret reference")]
    InvalidSecretReference,
    #[error("invalid Auth issuer")]
    InvalidAuthIssuer,
    #[error("invalid Auth assertion public key")]
    InvalidAuthPublicKey,
    #[error("project_callers must contain 1 to 64 unique Instance keys")]
    InvalidProjectCallers,
    #[error("admin_callers must contain 1 to 64 unique Instance keys")]
    InvalidAdminCallers,
    #[error("governance_callers must contain 1 to 64 unique Instance keys")]
    InvalidGovernanceCallers,
}

fn validate_config(config: &ProjectsConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("Projects configuration is invalid: {error}"),
        })
}

#[derive(Clone, Debug)]
struct PreparedProjects {
    postgres: OwnedPostgres,
}

#[lenso::plugin(lifecycle,configuration_schema="configuration.schema.json",validate=validate_config)]
#[derive(Clone)]
struct ProjectsPlugin {
    #[config]
    config: ProjectsConfig,
    secrets: Port<secrets::SecretsClient>,
    membership: Port<membership::OrganizationMembershipClient>,
    access: Port<access::AccessControlClient>,
    prepared: Rc<RefCell<Option<PreparedProjects>>>,
}

impl fmt::Debug for ProjectsPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectsPlugin")
            .field("prepared", &self.prepared.borrow().is_some())
            .field("schema", &self.config.schema)
            .field("project_caller_count", &self.config.project_callers.len())
            .field("admin_caller_count", &self.config.admin_callers.len())
            .field(
                "governance_caller_count",
                &self.config.governance_callers.len(),
            )
            .finish_non_exhaustive()
    }
}

#[lenso::provides(
    projects::Projects,
    collaboration::ProjectsCollaboration,
    admin::ProjectsAdmin,
    export_source::DataExportSource,
    retention::RetentionParticipant
)]
impl ProjectsPlugin {}

#[derive(Clone, Debug)]
struct Authorized {
    caller: String,
    actor: String,
}
#[derive(Debug)]
enum AuthorizationFailure {
    Unauthenticated,
    Forbidden,
    Runtime(RuntimeFailure),
}

impl ProjectsPlugin {
    fn prepared(&self) -> Result<PreparedProjects, RuntimeFailure> {
        self.prepared
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "Projects Plugin is not prepared".to_owned(),
            })
    }
    async fn authorize(
        &self,
        context: &Ctx,
        allowed_callers: &[String],
        capability: &str,
        operation: &str,
        organization_id: &str,
        permission: &str,
    ) -> Result<Authorized, AuthorizationFailure> {
        let caller = context
            .caller_instance()
            .filter(|caller| allowed_callers.iter().any(|allowed| allowed == *caller))
            .map(ToOwned::to_owned)
            .ok_or(AuthorizationFailure::Forbidden)?;
        let actor = self
            .config
            .verifier()
            .map_err(AuthorizationFailure::Runtime)?
            .project_context::<ProjectsActor>(context, capability, operation, &UtcClock)
            .map_err(|_| AuthorizationFailure::Unauthenticated)?
            .subject;
        if !valid_id(organization_id) || !valid_id(&actor) {
            return Err(AuthorizationFailure::Forbidden);
        }
        let membership = self
            .membership
            .check_membership_with_context(
                context.clone(),
                CheckMembershipRequest {
                    organization_id: organization_id.to_owned(),
                    subject: actor.clone(),
                },
            )
            .await
            .map_err(|error| match error {
                OrganizationMembershipInvocationError::Runtime(error) => {
                    AuthorizationFailure::Runtime(error)
                }
                OrganizationMembershipInvocationError::Domain(_) => {
                    AuthorizationFailure::Runtime(RuntimeFailure::ProtocolViolation {
                        capability: membership::CAPABILITY_ID,
                    })
                }
            })?;
        if !membership.active {
            return Err(AuthorizationFailure::Forbidden);
        }
        let decision = self
            .access
            .check_permission_with_context(
                context.clone(),
                CheckPermissionRequest {
                    subject: actor.clone(),
                    scope: CheckPermissionRequestScope {
                        kind: "organization".to_owned(),
                        id: organization_id.to_owned(),
                    },
                    permission: permission.to_owned(),
                },
            )
            .await
            .map_err(|error| match error {
                AccessControlInvocationError::Runtime(error) => {
                    AuthorizationFailure::Runtime(error)
                }
                AccessControlInvocationError::Domain(_) => {
                    AuthorizationFailure::Runtime(RuntimeFailure::ProtocolViolation {
                        capability: access::CAPABILITY_ID,
                    })
                }
            })?;
        if !decision.allowed {
            return Err(AuthorizationFailure::Forbidden);
        }
        Ok(Authorized { caller, actor })
    }
    fn governance_caller(&self, context: &Ctx) -> bool {
        context.caller_instance().is_some_and(|caller| {
            self.config
                .governance_callers
                .iter()
                .any(|allowed| allowed == caller)
        })
    }
}

fn map_storage<T, E>(
    result: Result<T, StorageError>,
    map: impl FnOnce(DomainFailure) -> E,
) -> PluginResult<T, E> {
    match result {
        Ok(value) => Ok(value),
        Err(StorageError::Domain(error)) => Err(PluginError::domain(map(error))),
        Err(StorageError::Runtime(error)) => Err(PluginError::runtime(error)),
    }
}

macro_rules! project_error {
    ($failure:expr,$kind:ident) => {
        match $failure {
            DomainFailure::IdempotencyConflict => projects::$kind::IdempotencyConflict,
            DomainFailure::NotFound => projects::$kind::NotFound,
            DomainFailure::RevisionConflict => projects::$kind::RevisionConflict,
            DomainFailure::TeamNotFound => projects::$kind::TeamNotFound,
            DomainFailure::WorkflowStateNotFound => projects::$kind::WorkflowStateNotFound,
            DomainFailure::ProjectStatusNotFound => projects::$kind::ProjectStatusNotFound,
            DomainFailure::CycleNotFound => projects::$kind::CycleNotFound,
            DomainFailure::MilestoneNotFound => projects::$kind::MilestoneNotFound,
            DomainFailure::ParentNotFound => projects::$kind::ParentNotFound,
            DomainFailure::LabelNotFound => projects::$kind::LabelNotFound,
            DomainFailure::PrivateTeam => projects::$kind::PrivateTeam,
            DomainFailure::IdentifierConflict => projects::$kind::IdentifierConflict,
            _ => projects::$kind::InvalidRequest,
        }
    };
}
macro_rules! collaboration_error {
    ($failure:expr,$kind:ident) => {
        match $failure {
            DomainFailure::IdempotencyConflict => collaboration::$kind::IdempotencyConflict,
            DomainFailure::NotFound => collaboration::$kind::NotFound,
            DomainFailure::RevisionConflict => collaboration::$kind::RevisionConflict,
            DomainFailure::PrivateTeam => collaboration::$kind::PrivateTeam,
            DomainFailure::AuthorRequired => collaboration::$kind::AuthorRequired,
            DomainFailure::CannotRelateSelf => collaboration::$kind::CannotRelateSelf,
            DomainFailure::RelationConflict | DomainFailure::IdentifierConflict => {
                collaboration::$kind::RelationConflict
            }
            _ => collaboration::$kind::InvalidRequest,
        }
    };
}
macro_rules! admin_error {
    ($failure:expr,$kind:ident) => {
        match $failure {
            DomainFailure::IdempotencyConflict => admin::$kind::IdempotencyConflict,
            DomainFailure::NotFound => admin::$kind::NotFound,
            DomainFailure::RevisionConflict => admin::$kind::RevisionConflict,
            DomainFailure::KeyConflict => admin::$kind::KeyConflict,
            DomainFailure::DefaultStateInvalid => admin::$kind::DefaultStateInvalid,
            DomainFailure::ActiveReference => admin::$kind::ActiveReference,
            _ => admin::$kind::InvalidRequest,
        }
    };
}

macro_rules! auth_project {
    ($result:expr,$kind:ident) => {
        match $result {
            Ok(value) => value,
            Err(AuthorizationFailure::Unauthenticated) => {
                return Err(PluginError::domain(projects::$kind::Unauthenticated))
            }
            Err(AuthorizationFailure::Forbidden) => {
                return Err(PluginError::domain(projects::$kind::Forbidden))
            }
            Err(AuthorizationFailure::Runtime(error)) => return Err(PluginError::runtime(error)),
        }
    };
}
macro_rules! auth_collaboration {
    ($result:expr,$kind:ident) => {
        match $result {
            Ok(value) => value,
            Err(AuthorizationFailure::Unauthenticated) => {
                return Err(PluginError::domain(collaboration::$kind::Unauthenticated))
            }
            Err(AuthorizationFailure::Forbidden) => {
                return Err(PluginError::domain(collaboration::$kind::Forbidden))
            }
            Err(AuthorizationFailure::Runtime(error)) => return Err(PluginError::runtime(error)),
        }
    };
}
macro_rules! auth_admin {
    ($result:expr,$kind:ident) => {
        match $result {
            Ok(value) => value,
            Err(AuthorizationFailure::Unauthenticated) => {
                return Err(PluginError::domain(admin::$kind::Unauthenticated))
            }
            Err(AuthorizationFailure::Forbidden) => {
                return Err(PluginError::domain(admin::$kind::Forbidden))
            }
            Err(AuthorizationFailure::Runtime(error)) => return Err(PluginError::runtime(error)),
        }
    };
}

impl ProjectsPlugin {
    async fn add_comment(
        &self,
        context: Ctx,
        request: collaboration::AddCommentRequest,
    ) -> PluginResult<collaboration::AddCommentResponse, collaboration::AddCommentError> {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::ADD_COMMENT_OPERATION,
                &request.organization_id,
                "projects.collaborate"
            )
            .await,
            AddCommentError
        );
        if !valid_id(&request.idempotency_key)
            || !valid_id(&request.issue_id)
            || !valid_id(&request.comment_id)
            || !valid_text(&request.body, 20_000)
        {
            return Err(PluginError::domain(
                collaboration::AddCommentError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::add_comment(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| collaboration_error!(failure, AddCommentError),
        )
    }
    async fn update_comment(
        &self,
        context: Ctx,
        request: collaboration::UpdateCommentRequest,
    ) -> PluginResult<collaboration::UpdateCommentResponse, collaboration::UpdateCommentError> {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::UPDATE_COMMENT_OPERATION,
                &request.organization_id,
                "projects.collaborate"
            )
            .await,
            UpdateCommentError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.comment_id,
            &request.expected_revision,
        ) || !valid_text(&request.body, 20_000)
        {
            return Err(PluginError::domain(
                collaboration::UpdateCommentError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::update_comment(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| collaboration_error!(failure, UpdateCommentError),
        )
    }
    async fn delete_comment(
        &self,
        context: Ctx,
        request: collaboration::DeleteCommentRequest,
    ) -> PluginResult<collaboration::DeleteCommentResponse, collaboration::DeleteCommentError> {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::DELETE_COMMENT_OPERATION,
                &request.organization_id,
                "projects.collaborate"
            )
            .await,
            DeleteCommentError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.comment_id,
            &request.expected_revision,
        ) {
            return Err(PluginError::domain(
                collaboration::DeleteCommentError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::delete_comment(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| collaboration_error!(failure, DeleteCommentError),
        )
    }
    async fn list_comments(
        &self,
        context: Ctx,
        request: collaboration::ListCommentsRequest,
    ) -> PluginResult<collaboration::ListCommentsResponse, collaboration::ListCommentsError> {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::LIST_COMMENTS_OPERATION,
                &request.organization_id,
                "projects.read"
            )
            .await,
            ListCommentsError
        );
        if !valid_id(&request.issue_id) || !valid_page(request.limit, &request.after) {
            return Err(PluginError::domain(
                collaboration::ListCommentsError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_comments(&prepared.postgres, &auth.actor, &request).await,
            |failure| collaboration_error!(failure, ListCommentsError),
        )
    }
    async fn create_project_update(
        &self,
        context: Ctx,
        request: collaboration::CreateProjectUpdateRequest,
    ) -> PluginResult<
        collaboration::CreateProjectUpdateResponse,
        collaboration::CreateProjectUpdateError,
    > {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::CREATE_PROJECT_UPDATE_OPERATION,
                &request.organization_id,
                "projects.collaborate"
            )
            .await,
            CreateProjectUpdateError
        );
        if !valid_id(&request.idempotency_key)
            || !valid_id(&request.project_id)
            || !valid_id(&request.update_id)
            || !valid_text(&request.body, 20_000)
        {
            return Err(PluginError::domain(
                collaboration::CreateProjectUpdateError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::create_project_update(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| collaboration_error!(failure, CreateProjectUpdateError),
        )
    }
    async fn list_project_updates(
        &self,
        context: Ctx,
        request: collaboration::ListProjectUpdatesRequest,
    ) -> PluginResult<
        collaboration::ListProjectUpdatesResponse,
        collaboration::ListProjectUpdatesError,
    > {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::LIST_PROJECT_UPDATES_OPERATION,
                &request.organization_id,
                "projects.read"
            )
            .await,
            ListProjectUpdatesError
        );
        if !valid_id(&request.project_id) || !valid_page(request.limit, &request.after) {
            return Err(PluginError::domain(
                collaboration::ListProjectUpdatesError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_project_updates(&prepared.postgres, &auth.actor, &request).await,
            |failure| collaboration_error!(failure, ListProjectUpdatesError),
        )
    }
    async fn add_issue_relation(
        &self,
        context: Ctx,
        request: collaboration::AddIssueRelationRequest,
    ) -> PluginResult<collaboration::AddIssueRelationResponse, collaboration::AddIssueRelationError>
    {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::ADD_ISSUE_RELATION_OPERATION,
                &request.organization_id,
                "projects.collaborate"
            )
            .await,
            AddIssueRelationError
        );
        if [
            &request.idempotency_key,
            &request.relation_id,
            &request.issue_id,
            &request.related_issue_id,
        ]
        .into_iter()
        .any(|value| !valid_id(value))
        {
            return Err(PluginError::domain(
                collaboration::AddIssueRelationError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::add_issue_relation(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| collaboration_error!(failure, AddIssueRelationError),
        )
    }
    async fn remove_issue_relation(
        &self,
        context: Ctx,
        request: collaboration::RemoveIssueRelationRequest,
    ) -> PluginResult<
        collaboration::RemoveIssueRelationResponse,
        collaboration::RemoveIssueRelationError,
    > {
        let auth = auth_collaboration!(
            self.authorize(
                &context,
                &self.config.project_callers,
                collaboration::CAPABILITY_ID,
                collaboration::REMOVE_ISSUE_RELATION_OPERATION,
                &request.organization_id,
                "projects.collaborate"
            )
            .await,
            RemoveIssueRelationError
        );
        if !valid_id(&request.idempotency_key) || !valid_id(&request.relation_id) {
            return Err(PluginError::domain(
                collaboration::RemoveIssueRelationError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::remove_issue_relation(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| collaboration_error!(failure, RemoveIssueRelationError),
        )
    }
}

impl ProjectsPlugin {
    async fn put_team(
        &self,
        context: Ctx,
        request: admin::PutTeamRequest,
    ) -> PluginResult<admin::PutTeamResponse, admin::PutTeamError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::PUT_TEAM_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            PutTeamError
        );
        if !valid_team(&request) {
            return Err(PluginError::domain(admin::PutTeamError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::put_team(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| admin_error!(failure, PutTeamError),
        )
    }
    async fn list_teams(
        &self,
        context: Ctx,
        request: admin::ListTeamsRequest,
    ) -> PluginResult<admin::ListTeamsResponse, admin::ListTeamsError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::LIST_TEAMS_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ListTeamsError
        );
        if !valid_page(request.limit, &request.after) {
            return Err(PluginError::domain(admin::ListTeamsError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_teams(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, ListTeamsError),
        )
    }
    async fn set_team_member(
        &self,
        context: Ctx,
        request: admin::SetTeamMemberRequest,
    ) -> PluginResult<admin::SetTeamMemberResponse, admin::SetTeamMemberError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::SET_TEAM_MEMBER_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            SetTeamMemberError
        );
        if [&request.idempotency_key, &request.team_id, &request.subject]
            .into_iter()
            .any(|value| !valid_id(value))
        {
            return Err(PluginError::domain(
                admin::SetTeamMemberError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::set_team_member(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| admin_error!(failure, SetTeamMemberError),
        )
    }
    async fn put_workflow_state(
        &self,
        context: Ctx,
        request: admin::PutWorkflowStateRequest,
    ) -> PluginResult<admin::PutWorkflowStateResponse, admin::PutWorkflowStateError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::PUT_WORKFLOW_STATE_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            PutWorkflowStateError
        );
        if !valid_catalog_put(
            &request.idempotency_key,
            &request.state_id,
            &request.name,
            &request.color,
            &request.expected_revision,
        ) || !valid_id(&request.team_id)
        {
            return Err(PluginError::domain(
                admin::PutWorkflowStateError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::put_workflow_state(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| admin_error!(failure, PutWorkflowStateError),
        )
    }
    async fn get_workflow_state(
        &self,
        context: Ctx,
        request: admin::GetWorkflowStateRequest,
    ) -> PluginResult<admin::GetWorkflowStateResponse, admin::GetWorkflowStateError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::GET_WORKFLOW_STATE_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            GetWorkflowStateError
        );
        if !valid_id(&request.team_id) || !valid_id(&request.state_id) {
            return Err(PluginError::domain(
                admin::GetWorkflowStateError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::get_workflow_state(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, GetWorkflowStateError),
        )
    }
    async fn reorder_workflow_states(
        &self,
        context: Ctx,
        request: admin::ReorderWorkflowStatesRequest,
    ) -> PluginResult<admin::ReorderWorkflowStatesResponse, admin::ReorderWorkflowStatesError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::REORDER_WORKFLOW_STATES_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ReorderWorkflowStatesError
        );
        if !valid_workflow_reorder(&request) {
            return Err(PluginError::domain(
                admin::ReorderWorkflowStatesError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::reorder_workflow_states(
                &prepared.postgres,
                &auth.caller,
                &auth.actor,
                &request,
            )
            .await,
            |failure| admin_error!(failure, ReorderWorkflowStatesError),
        )
    }
    async fn archive_workflow_state(
        &self,
        context: Ctx,
        request: admin::ArchiveWorkflowStateRequest,
    ) -> PluginResult<admin::ArchiveWorkflowStateResponse, admin::ArchiveWorkflowStateError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::ARCHIVE_WORKFLOW_STATE_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ArchiveWorkflowStateError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.state_id,
            &request.expected_revision,
        ) {
            return Err(PluginError::domain(
                admin::ArchiveWorkflowStateError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::archive_workflow_state(
                &prepared.postgres,
                &auth.caller,
                &auth.actor,
                &request,
            )
            .await,
            |failure| admin_error!(failure, ArchiveWorkflowStateError),
        )
    }
    async fn delete_workflow_state(
        &self,
        context: Ctx,
        request: admin::DeleteWorkflowStateRequest,
    ) -> PluginResult<admin::DeleteWorkflowStateResponse, admin::DeleteWorkflowStateError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::DELETE_WORKFLOW_STATE_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            DeleteWorkflowStateError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.state_id,
            &request.expected_revision,
        ) || !valid_id(&request.team_id)
        {
            return Err(PluginError::domain(
                admin::DeleteWorkflowStateError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::delete_workflow_state(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| admin_error!(failure, DeleteWorkflowStateError),
        )
    }
    async fn list_workflow_states(
        &self,
        context: Ctx,
        request: admin::ListWorkflowStatesRequest,
    ) -> PluginResult<admin::ListWorkflowStatesResponse, admin::ListWorkflowStatesError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::LIST_WORKFLOW_STATES_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ListWorkflowStatesError
        );
        if !valid_id(&request.team_id) || !valid_page(request.limit, &request.after) {
            return Err(PluginError::domain(
                admin::ListWorkflowStatesError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_workflow_states(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, ListWorkflowStatesError),
        )
    }
    async fn put_project_status(
        &self,
        context: Ctx,
        request: admin::PutProjectStatusRequest,
    ) -> PluginResult<admin::PutProjectStatusResponse, admin::PutProjectStatusError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::PUT_PROJECT_STATUS_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            PutProjectStatusError
        );
        if !valid_catalog_put(
            &request.idempotency_key,
            &request.status_id,
            &request.name,
            &request.color,
            &request.expected_revision,
        ) {
            return Err(PluginError::domain(
                admin::PutProjectStatusError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::put_project_status(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| admin_error!(failure, PutProjectStatusError),
        )
    }
    async fn get_project_status(
        &self,
        context: Ctx,
        request: admin::GetProjectStatusRequest,
    ) -> PluginResult<admin::GetProjectStatusResponse, admin::GetProjectStatusError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::GET_PROJECT_STATUS_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            GetProjectStatusError
        );
        if !valid_id(&request.status_id) {
            return Err(PluginError::domain(
                admin::GetProjectStatusError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::get_project_status(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, GetProjectStatusError),
        )
    }
    async fn reorder_project_statuses(
        &self,
        context: Ctx,
        request: admin::ReorderProjectStatusesRequest,
    ) -> PluginResult<admin::ReorderProjectStatusesResponse, admin::ReorderProjectStatusesError>
    {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::REORDER_PROJECT_STATUSES_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ReorderProjectStatusesError
        );
        if !valid_project_status_reorder(&request) {
            return Err(PluginError::domain(
                admin::ReorderProjectStatusesError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::reorder_project_statuses(
                &prepared.postgres,
                &auth.caller,
                &auth.actor,
                &request,
            )
            .await,
            |failure| admin_error!(failure, ReorderProjectStatusesError),
        )
    }
    async fn archive_project_status(
        &self,
        context: Ctx,
        request: admin::ArchiveProjectStatusRequest,
    ) -> PluginResult<admin::ArchiveProjectStatusResponse, admin::ArchiveProjectStatusError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::ARCHIVE_PROJECT_STATUS_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ArchiveProjectStatusError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.status_id,
            &request.expected_revision,
        ) {
            return Err(PluginError::domain(
                admin::ArchiveProjectStatusError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::archive_project_status(
                &prepared.postgres,
                &auth.caller,
                &auth.actor,
                &request,
            )
            .await,
            |failure| admin_error!(failure, ArchiveProjectStatusError),
        )
    }
    async fn delete_project_status(
        &self,
        context: Ctx,
        request: admin::DeleteProjectStatusRequest,
    ) -> PluginResult<admin::DeleteProjectStatusResponse, admin::DeleteProjectStatusError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::DELETE_PROJECT_STATUS_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            DeleteProjectStatusError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.status_id,
            &request.expected_revision,
        ) {
            return Err(PluginError::domain(
                admin::DeleteProjectStatusError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::delete_project_status(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| admin_error!(failure, DeleteProjectStatusError),
        )
    }
    async fn list_project_statuses(
        &self,
        context: Ctx,
        request: admin::ListProjectStatusesRequest,
    ) -> PluginResult<admin::ListProjectStatusesResponse, admin::ListProjectStatusesError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::LIST_PROJECT_STATUSES_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ListProjectStatusesError
        );
        if !valid_page(request.limit, &request.after) {
            return Err(PluginError::domain(
                admin::ListProjectStatusesError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_project_statuses(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, ListProjectStatusesError),
        )
    }
    async fn put_label(
        &self,
        context: Ctx,
        request: admin::PutLabelRequest,
    ) -> PluginResult<admin::PutLabelResponse, admin::PutLabelError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::PUT_LABEL_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            PutLabelError
        );
        if !valid_catalog_put(
            &request.idempotency_key,
            &request.label_id,
            &request.name,
            &request.color,
            &request.expected_revision,
        ) || request.team_id.as_ref().is_some_and(|id| !valid_id(id))
            || request
                .description
                .as_ref()
                .is_some_and(|value| !valid_text(value, 1000))
        {
            return Err(PluginError::domain(admin::PutLabelError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::put_label(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| admin_error!(failure, PutLabelError),
        )
    }
    async fn list_labels(
        &self,
        context: Ctx,
        request: admin::ListLabelsRequest,
    ) -> PluginResult<admin::ListLabelsResponse, admin::ListLabelsError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::LIST_LABELS_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ListLabelsError
        );
        if !valid_page(request.limit, &request.after)
            || request.team_id.as_ref().is_some_and(|id| !valid_id(id))
        {
            return Err(PluginError::domain(admin::ListLabelsError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_labels(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, ListLabelsError),
        )
    }
    async fn put_cycle(
        &self,
        context: Ctx,
        request: admin::PutCycleRequest,
    ) -> PluginResult<admin::PutCycleResponse, admin::PutCycleError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::PUT_CYCLE_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            PutCycleError
        );
        if !valid_id(&request.idempotency_key)
            || !valid_id(&request.cycle_id)
            || !valid_id(&request.team_id)
            || request.number < 1
            || request
                .name
                .as_ref()
                .is_some_and(|value| !valid_text(value, 200))
            || request
                .expected_revision
                .as_ref()
                .is_some_and(|value| storage::parse_revision(value).is_err())
        {
            return Err(PluginError::domain(admin::PutCycleError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::put_cycle(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| admin_error!(failure, PutCycleError),
        )
    }
    async fn list_cycles(
        &self,
        context: Ctx,
        request: admin::ListCyclesRequest,
    ) -> PluginResult<admin::ListCyclesResponse, admin::ListCyclesError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::LIST_CYCLES_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ListCyclesError
        );
        if !valid_id(&request.team_id) || !valid_page(request.limit, &request.after) {
            return Err(PluginError::domain(admin::ListCyclesError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_cycles(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, ListCyclesError),
        )
    }
    async fn put_milestone(
        &self,
        context: Ctx,
        request: admin::PutMilestoneRequest,
    ) -> PluginResult<admin::PutMilestoneResponse, admin::PutMilestoneError> {
        let auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::PUT_MILESTONE_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            PutMilestoneError
        );
        if !valid_id(&request.idempotency_key)
            || !valid_id(&request.milestone_id)
            || !valid_id(&request.project_id)
            || !valid_text(&request.name, 200)
            || request
                .description
                .as_ref()
                .is_some_and(|value| !valid_text(value, 2000))
            || request
                .expected_revision
                .as_ref()
                .is_some_and(|value| storage::parse_revision(value).is_err())
        {
            return Err(PluginError::domain(
                admin::PutMilestoneError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::put_milestone(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| admin_error!(failure, PutMilestoneError),
        )
    }
    async fn list_milestones(
        &self,
        context: Ctx,
        request: admin::ListMilestonesRequest,
    ) -> PluginResult<admin::ListMilestonesResponse, admin::ListMilestonesError> {
        let _auth = auth_admin!(
            self.authorize(
                &context,
                &self.config.admin_callers,
                admin::CAPABILITY_ID,
                admin::LIST_MILESTONES_OPERATION,
                &request.organization_id,
                "projects.admin"
            )
            .await,
            ListMilestonesError
        );
        if !valid_id(&request.project_id) || !valid_page(request.limit, &request.after) {
            return Err(PluginError::domain(
                admin::ListMilestonesError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_milestones(&prepared.postgres, &request).await,
            |failure| admin_error!(failure, ListMilestonesError),
        )
    }
}

impl ProjectsPlugin {
    async fn collect_export(
        &self,
        context: Ctx,
        request: export_source::CollectExportRequest,
    ) -> PluginResult<export_source::CollectExportResponse, export_source::CollectExportError> {
        if !self.governance_caller(&context) {
            return Err(PluginError::domain(
                export_source::CollectExportError::Forbidden,
            ));
        }
        if request.scope_kind != "organization"
            || [&request.export_id, &request.scope_id, &request.subject]
                .into_iter()
                .any(|value| !valid_id(value))
        {
            return Err(PluginError::domain(
                export_source::CollectExportError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        match storage::collect_export(&prepared.postgres, &request).await {
            Ok(value) => Ok(value),
            Err(StorageError::Domain(_)) => Err(PluginError::domain(
                export_source::CollectExportError::InvalidRequest,
            )),
            Err(StorageError::Runtime(error)) => Err(PluginError::runtime(error)),
        }
    }
    async fn apply_retention(
        &self,
        context: Ctx,
        request: retention::ApplyRetentionRequest,
    ) -> PluginResult<retention::ApplyRetentionResponse, retention::ApplyRetentionError> {
        if !self.governance_caller(&context) {
            return Err(PluginError::domain(
                retention::ApplyRetentionError::Forbidden,
            ));
        }
        if request.scope_kind != "organization"
            || [&request.action_id, &request.scope_id, &request.subject]
                .into_iter()
                .any(|value| !valid_id(value))
            || !valid_text(&request.reason, 1000)
        {
            return Err(PluginError::domain(
                retention::ApplyRetentionError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        match storage::apply_retention(&prepared.postgres, &request).await {
            Ok(value) => Ok(value),
            Err(StorageError::Domain(_)) => Err(PluginError::domain(
                retention::ApplyRetentionError::InvalidRequest,
            )),
            Err(StorageError::Runtime(error)) => Err(PluginError::runtime(error)),
        }
    }
}

impl Lifecycle for ProjectsPlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        let database_url = resolve_secret(
            &self.secrets,
            context.dependencies(),
            context.cancellation(),
            &self.config.database_url_secret,
        )
        .await?;
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(self.config.schema.clone()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: error.to_string(),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })?;
        self.prepared
            .borrow_mut()
            .replace(PreparedProjects { postgres });
        Ok(())
    }
    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.prepared.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ProjectsActor {
    subject: String,
}
impl TypedActor for ProjectsActor {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        Ok(Self {
            subject: assertion.subject().to_owned(),
        })
    }
}
#[derive(Clone, Copy, Debug)]
struct UtcClock;
impl AssertionClock for UtcClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

async fn resolve_secret(
    secrets: &secrets::SecretsClient,
    dependencies: &PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context = dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
    secrets
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|value| Zeroizing::new(value.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
}

fn valid_callers(values: &[String]) -> bool {
    !values.is_empty()
        && values.len() <= MAX_CALLERS
        && values.iter().all(|value| valid_id(value))
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}
fn valid_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.contains('\0')
}
fn valid_optional_text(value: &Option<String>, max: usize) -> bool {
    value.as_ref().is_none_or(|value| valid_text(value, max))
}
fn valid_secret_reference(value: &str) -> bool {
    valid_id(value)
        || (!value.is_empty()
            && value.len() <= 256
            && !value.starts_with('/')
            && !value.ends_with('/')
            && !value.contains("//")
            && value.split('/').all(|part| part != "." && part != "..")
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/')
            }))
}
fn valid_page(limit: i64, after: &Option<String>) -> bool {
    (1..=100).contains(&limit)
        && after
            .as_ref()
            .is_none_or(|value| storage::parse_revision(value).is_ok())
}
fn valid_url(value: &str) -> bool {
    value.starts_with("https://") && value.len() <= 2048 && !value.chars().any(char::is_control)
}
fn valid_idempotent_revision(key: &str, id: &str, revision: &str) -> bool {
    valid_id(key) && valid_id(id) && storage::parse_revision(revision).is_ok()
}
fn unique_ids(values: &[String]) -> bool {
    !values.is_empty()
        && values.len() <= 64
        && values.iter().all(|value| valid_id(value))
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}
fn valid_project_create(request: &projects::CreateProjectRequest) -> bool {
    valid_id(&request.idempotency_key)
        && valid_id(&request.project_id)
        && valid_text(&request.name, 300)
        && valid_optional_text(&request.summary, 4000)
        && valid_id(&request.lead_team_id)
        && unique_ids(&request.team_ids)
        && request.team_ids.contains(&request.lead_team_id)
        && request.status_id.as_ref().is_none_or(|id| valid_id(id))
        && request.milestone_id.as_ref().is_none_or(|id| valid_id(id))
}
fn valid_project_update(request: &projects::UpdateProjectRequest) -> bool {
    valid_idempotent_revision(
        &request.idempotency_key,
        &request.project_id,
        &request.expected_revision,
    ) && valid_text(&request.name, 300)
        && valid_optional_text(&request.summary, 4000)
        && valid_id(&request.lead_team_id)
        && unique_ids(&request.team_ids)
        && request.team_ids.contains(&request.lead_team_id)
        && valid_id(&request.status_id)
        && request.milestone_id.as_ref().is_none_or(|id| valid_id(id))
}
fn valid_issue_common(
    title: &str,
    description: &Option<String>,
    workflow: Option<&str>,
    cycle: &Option<String>,
    milestone: &Option<String>,
    parent: &Option<String>,
    labels: &[String],
) -> bool {
    valid_text(title, 1000)
        && valid_optional_text(description, 20_000)
        && workflow.is_none_or(valid_id)
        && cycle.as_ref().is_none_or(|id| valid_id(id))
        && milestone.as_ref().is_none_or(|id| valid_id(id))
        && parent.as_ref().is_none_or(|id| valid_id(id))
        && labels.len() <= 64
        && labels.iter().all(|id| valid_id(id))
        && labels.iter().collect::<BTreeSet<_>>().len() == labels.len()
}
fn valid_issue_create(request: &projects::CreateIssueRequest) -> bool {
    [
        &request.idempotency_key,
        &request.issue_id,
        &request.project_id,
        &request.team_id,
    ]
    .into_iter()
    .all(|value| valid_id(value))
        && valid_issue_common(
            &request.title,
            &request.description,
            request.workflow_state_id.as_deref(),
            &request.cycle_id,
            &request.milestone_id,
            &request.parent_issue_id,
            &request.label_ids,
        )
        && request.parent_issue_id.as_ref() != Some(&request.issue_id)
}
fn valid_issue_update(request: &projects::UpdateIssueRequest) -> bool {
    valid_idempotent_revision(
        &request.idempotency_key,
        &request.issue_id,
        &request.expected_revision,
    ) && valid_issue_common(
        &request.title,
        &request.description,
        Some(&request.workflow_state_id),
        &request.cycle_id,
        &request.milestone_id,
        &request.parent_issue_id,
        &request.label_ids,
    ) && request.parent_issue_id.as_ref() != Some(&request.issue_id)
}
fn valid_team(request: &admin::PutTeamRequest) -> bool {
    valid_id(&request.idempotency_key)
        && valid_id(&request.team_id)
        && request.key.len() >= 2
        && request.key.len() <= 10
        && request
            .key
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        && request.key.as_bytes()[0].is_ascii_uppercase()
        && valid_text(&request.name, 200)
        && valid_optional_text(&request.description, 2000)
        && request
            .default_workflow_state_id
            .as_ref()
            .is_none_or(|id| valid_id(id))
        && request
            .expected_revision
            .as_ref()
            .is_none_or(|value| storage::parse_revision(value).is_ok())
        && matches!(
            (
                request.expected_revision.as_ref(),
                request.default_workflow_state_id.as_ref()
            ),
            (None, None) | (Some(_), Some(_))
        )
}
fn valid_color(value: &str) -> bool {
    value.len() == 7
        && value.starts_with('#')
        && value[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn valid_catalog_put(
    key: &str,
    id: &str,
    name: &str,
    color: &str,
    revision: &Option<String>,
) -> bool {
    valid_id(key)
        && valid_id(id)
        && valid_text(name, 200)
        && valid_color(color)
        && revision
            .as_ref()
            .is_none_or(|value| storage::parse_revision(value).is_ok())
}

fn valid_workflow_reorder(request: &admin::ReorderWorkflowStatesRequest) -> bool {
    (1..=100).contains(&request.items.len())
        && valid_id(&request.idempotency_key)
        && valid_id(&request.team_id)
        && request.items.iter().all(|item| {
            valid_id(&item.state_id)
                && storage::parse_revision(&item.expected_revision).is_ok()
                && (0..=i64::from(i32::MAX)).contains(&item.position)
        })
        && request
            .items
            .iter()
            .map(|item| &item.state_id)
            .collect::<BTreeSet<_>>()
            .len()
            == request.items.len()
        && request
            .items
            .iter()
            .map(|item| item.position)
            .collect::<BTreeSet<_>>()
            .len()
            == request.items.len()
}

fn valid_project_status_reorder(request: &admin::ReorderProjectStatusesRequest) -> bool {
    (1..=100).contains(&request.items.len())
        && valid_id(&request.idempotency_key)
        && request.items.iter().all(|item| {
            valid_id(&item.status_id)
                && storage::parse_revision(&item.expected_revision).is_ok()
                && (0..=i64::from(i32::MAX)).contains(&item.position)
        })
        && request
            .items
            .iter()
            .map(|item| &item.status_id)
            .collect::<BTreeSet<_>>()
            .len()
            == request.items.len()
        && request
            .items
            .iter()
            .map(|item| item.position)
            .collect::<BTreeSet<_>>()
            .len()
            == request.items.len()
}

#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_auth_sdk::{ActorAssertionIssuer, Validity, audience};
    use lenso_kernel::{CancellationToken, InvocationContext};
    use time::Duration as TimeDuration;

    fn config() -> ProjectsConfig {
        let issuer = ActorAssertionIssuer::new("auth.users", b"projects-test-key");
        ProjectsConfig::new(
            "projects",
            "projects/database-url",
            "auth.users",
            issuer.public_key_base64(),
            vec!["projects-api".to_owned()],
            vec!["projects-admin-api".to_owned()],
            vec!["privacy-service".to_owned()],
        )
        .unwrap()
    }

    fn plugin() -> ProjectsPlugin {
        ProjectsPlugin {
            config: config(),
            secrets: Port::default(),
            membership: Port::default(),
            access: Port::default(),
            prepared: Rc::new(RefCell::new(None)),
        }
    }

    fn context(caller: &str) -> InvocationContext {
        InvocationContext::new(1, None, CancellationToken::new()).with_caller_instance(caller)
    }

    fn create_project_request() -> projects::CreateProjectRequest {
        projects::CreateProjectRequest {
            idempotency_key: "create-acme".to_owned(),
            organization_id: "org_acme".to_owned(),
            project_id: "project_roadmap".to_owned(),
            name: "Roadmap".to_owned(),
            summary: None,
            lead_team_id: "team_eng".to_owned(),
            team_ids: vec!["team_eng".to_owned()],
            status_id: None,
            milestone_id: None,
            start_date: None,
            target_date: None,
        }
    }

    #[test]
    fn descriptor_exposes_three_product_roles_and_governance_roles() {
        let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        let provided = descriptor["provided_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["capability_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            provided,
            BTreeSet::from([
                projects::CAPABILITY_ID,
                collaboration::CAPABILITY_ID,
                admin::CAPABILITY_ID,
                export_source::CAPABILITY_ID,
                retention::CAPABILITY_ID,
            ])
        );
        let required = descriptor["required_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["capability_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            required,
            BTreeSet::from([
                secrets::CAPABILITY_ID,
                membership::CAPABILITY_ID,
                access::CAPABILITY_ID,
            ])
        );
    }

    #[test]
    fn configuration_rejects_duplicate_authority() {
        let mut invalid = config();
        invalid.project_callers.push("projects-api".to_owned());
        assert_eq!(
            invalid.validate(),
            Err(ProjectsConfigError::InvalidProjectCallers)
        );
    }

    #[test]
    fn untrusted_caller_is_rejected_before_assertion_ports_or_storage() {
        let result = futures::executor::block_on(
            plugin().create_project(context("unknown-api"), create_project_request()),
        );
        assert_eq!(
            result,
            Err(PluginError::Domain(projects::CreateProjectError::Forbidden))
        );
    }

    #[test]
    fn trusted_caller_still_requires_exact_operation_audience() {
        let issuer = ActorAssertionIssuer::new("auth.users", b"projects-test-key");
        let now = OffsetDateTime::now_utc();
        let assertion = issuer.issue(
            "usr_admin",
            "user",
            "strong",
            [audience(
                projects::CAPABILITY_ID,
                projects::GET_PROJECT_OPERATION,
            )],
            Validity::new(
                now - TimeDuration::seconds(1),
                now + TimeDuration::minutes(1),
            )
            .unwrap(),
            std::collections::BTreeMap::new(),
        );
        let context = assertion.attach(context("projects-api")).unwrap();
        let result =
            futures::executor::block_on(plugin().create_project(context, create_project_request()));
        assert_eq!(
            result,
            Err(PluginError::Domain(
                projects::CreateProjectError::Unauthenticated
            ))
        );
    }

    #[test]
    fn validation_enforces_multi_team_lead_and_unique_labels() {
        let mut project = create_project_request();
        project.lead_team_id = "team_product".to_owned();
        assert!(!valid_project_create(&project));

        let mut issue = projects::CreateIssueRequest {
            idempotency_key: "create-issue".to_owned(),
            organization_id: "org_acme".to_owned(),
            issue_id: "issue_1".to_owned(),
            project_id: "project_roadmap".to_owned(),
            team_id: "team_eng".to_owned(),
            title: "Ship it".to_owned(),
            description: None,
            priority: projects::Priority::High,
            workflow_state_id: Some("state_started".to_owned()),
            cycle_id: None,
            milestone_id: None,
            parent_issue_id: None,
            label_ids: vec!["label_bug".to_owned(), "label_bug".to_owned()],
        };
        assert!(!valid_issue_create(&issue));
        issue.label_ids.pop();
        assert!(valid_issue_create(&issue));
    }
}

impl ProjectsPlugin {
    async fn create_project(
        &self,
        context: Ctx,
        request: projects::CreateProjectRequest,
    ) -> PluginResult<projects::CreateProjectResponse, projects::CreateProjectError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::CREATE_PROJECT_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            CreateProjectError
        );
        if !valid_project_create(&request) {
            return Err(PluginError::domain(
                projects::CreateProjectError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::create_project(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| project_error!(failure, CreateProjectError),
        )
    }
    async fn get_project(
        &self,
        context: Ctx,
        request: projects::GetProjectRequest,
    ) -> PluginResult<projects::GetProjectResponse, projects::GetProjectError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::GET_PROJECT_OPERATION,
                &request.organization_id,
                "projects.read"
            )
            .await,
            GetProjectError
        );
        if !valid_id(&request.project_id) {
            return Err(PluginError::domain(
                projects::GetProjectError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::get_project(&prepared.postgres, &auth.actor, &request).await,
            |failure| project_error!(failure, GetProjectError),
        )
    }
    async fn list_projects(
        &self,
        context: Ctx,
        request: projects::ListProjectsRequest,
    ) -> PluginResult<projects::ListProjectsResponse, projects::ListProjectsError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::LIST_PROJECTS_OPERATION,
                &request.organization_id,
                "projects.read"
            )
            .await,
            ListProjectsError
        );
        if !valid_page(request.limit, &request.after)
            || request.team_id.as_ref().is_some_and(|id| !valid_id(id))
        {
            return Err(PluginError::domain(
                projects::ListProjectsError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_projects(&prepared.postgres, &auth.actor, &request).await,
            |failure| project_error!(failure, ListProjectsError),
        )
    }
    async fn update_project(
        &self,
        context: Ctx,
        request: projects::UpdateProjectRequest,
    ) -> PluginResult<projects::UpdateProjectResponse, projects::UpdateProjectError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::UPDATE_PROJECT_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            UpdateProjectError
        );
        if !valid_project_update(&request) {
            return Err(PluginError::domain(
                projects::UpdateProjectError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::update_project(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| project_error!(failure, UpdateProjectError),
        )
    }
    async fn archive_project(
        &self,
        context: Ctx,
        request: projects::ArchiveProjectRequest,
    ) -> PluginResult<projects::ArchiveProjectResponse, projects::ArchiveProjectError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::ARCHIVE_PROJECT_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            ArchiveProjectError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.project_id,
            &request.expected_revision,
        ) {
            return Err(PluginError::domain(
                projects::ArchiveProjectError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::archive_project(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| project_error!(failure, ArchiveProjectError),
        )
    }
    async fn create_issue(
        &self,
        context: Ctx,
        request: projects::CreateIssueRequest,
    ) -> PluginResult<projects::CreateIssueResponse, projects::CreateIssueError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::CREATE_ISSUE_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            CreateIssueError
        );
        if !valid_issue_create(&request) {
            return Err(PluginError::domain(
                projects::CreateIssueError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::create_issue(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| project_error!(failure, CreateIssueError),
        )
    }
    async fn get_issue(
        &self,
        context: Ctx,
        request: projects::GetIssueRequest,
    ) -> PluginResult<projects::GetIssueResponse, projects::GetIssueError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::GET_ISSUE_OPERATION,
                &request.organization_id,
                "projects.read"
            )
            .await,
            GetIssueError
        );
        if !valid_id(&request.issue_ref) {
            return Err(PluginError::domain(projects::GetIssueError::InvalidRequest));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::get_issue(&prepared.postgres, &auth.actor, &request).await,
            |failure| project_error!(failure, GetIssueError),
        )
    }
    async fn list_issues(
        &self,
        context: Ctx,
        request: projects::ListIssuesRequest,
    ) -> PluginResult<projects::ListIssuesResponse, projects::ListIssuesError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::LIST_ISSUES_OPERATION,
                &request.organization_id,
                "projects.read"
            )
            .await,
            ListIssuesError
        );
        if !valid_page(request.limit, &request.after)
            || [
                request.project_id.as_ref(),
                request.team_id.as_ref(),
                request.workflow_state_id.as_ref(),
            ]
            .into_iter()
            .flatten()
            .any(|id| !valid_id(id))
        {
            return Err(PluginError::domain(
                projects::ListIssuesError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_issues(&prepared.postgres, &auth.actor, &request).await,
            |failure| project_error!(failure, ListIssuesError),
        )
    }
    async fn update_issue(
        &self,
        context: Ctx,
        request: projects::UpdateIssueRequest,
    ) -> PluginResult<projects::UpdateIssueResponse, projects::UpdateIssueError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::UPDATE_ISSUE_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            UpdateIssueError
        );
        if !valid_issue_update(&request) {
            return Err(PluginError::domain(
                projects::UpdateIssueError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::update_issue(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| project_error!(failure, UpdateIssueError),
        )
    }
    async fn move_issue(
        &self,
        context: Ctx,
        request: projects::MoveIssueRequest,
    ) -> PluginResult<projects::MoveIssueResponse, projects::MoveIssueError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::MOVE_ISSUE_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            MoveIssueError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.issue_id,
            &request.expected_revision,
        ) || !valid_id(&request.team_id)
            || !valid_id(&request.workflow_state_id)
        {
            return Err(PluginError::domain(
                projects::MoveIssueError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::move_issue(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| project_error!(failure, MoveIssueError),
        )
    }
    async fn archive_issue(
        &self,
        context: Ctx,
        request: projects::ArchiveIssueRequest,
    ) -> PluginResult<projects::ArchiveIssueResponse, projects::ArchiveIssueError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::ARCHIVE_ISSUE_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            ArchiveIssueError
        );
        if !valid_idempotent_revision(
            &request.idempotency_key,
            &request.issue_id,
            &request.expected_revision,
        ) {
            return Err(PluginError::domain(
                projects::ArchiveIssueError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::archive_issue(&prepared.postgres, &auth.caller, &auth.actor, &request).await,
            |failure| project_error!(failure, ArchiveIssueError),
        )
    }
    async fn put_external_link(
        &self,
        context: Ctx,
        request: projects::PutExternalLinkRequest,
    ) -> PluginResult<projects::PutExternalLinkResponse, projects::PutExternalLinkError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::PUT_EXTERNAL_LINK_OPERATION,
                &request.organization_id,
                "projects.write"
            )
            .await,
            PutExternalLinkError
        );
        if !valid_id(&request.idempotency_key)
            || !valid_id(&request.issue_id)
            || !valid_id(&request.provider)
            || !valid_id(&request.external_key)
            || !valid_url(&request.url)
            || request
                .title
                .as_ref()
                .is_some_and(|value| !valid_text(value, 300))
        {
            return Err(PluginError::domain(
                projects::PutExternalLinkError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::put_external_link(&prepared.postgres, &auth.caller, &auth.actor, &request)
                .await,
            |failure| project_error!(failure, PutExternalLinkError),
        )
    }
    async fn list_activity(
        &self,
        context: Ctx,
        request: projects::ListActivityRequest,
    ) -> PluginResult<projects::ListActivityResponse, projects::ListActivityError> {
        let auth = auth_project!(
            self.authorize(
                &context,
                &self.config.project_callers,
                projects::CAPABILITY_ID,
                projects::LIST_ACTIVITY_OPERATION,
                &request.organization_id,
                "projects.read"
            )
            .await,
            ListActivityError
        );
        if !valid_page(request.limit, &request.after)
            || request.project_id.as_ref().is_some_and(|id| !valid_id(id))
            || request.issue_id.as_ref().is_some_and(|id| !valid_id(id))
        {
            return Err(PluginError::domain(
                projects::ListActivityError::InvalidRequest,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::runtime)?;
        map_storage(
            storage::list_activity(&prepared.postgres, &auth.actor, &request).await,
            |failure| project_error!(failure, ListActivityError),
        )
    }
}
