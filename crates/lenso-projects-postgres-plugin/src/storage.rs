// Transactional vertical slices keep their full invariant set together; request-shaped helper
// signatures and optional generated fields are clearer here than artificial tuple wrappers.
#![allow(clippy::ref_option, clippy::too_many_arguments, clippy::too_many_lines)]

use std::{collections::BTreeSet, fmt};

use lenso_capability_projects as projects;
use lenso_capability_projects_admin as admin;
use lenso_capability_projects_collaboration as collaboration;
use lenso_kernel::RuntimeFailure;
use lenso_postgres_kit::OwnedPostgres;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};
use time::{Date, OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DomainFailure {
    InvalidRequest,
    IdempotencyConflict,
    NotFound,
    RevisionConflict,
    TeamNotFound,
    WorkflowStateNotFound,
    ProjectStatusNotFound,
    CycleNotFound,
    MilestoneNotFound,
    ParentNotFound,
    LabelNotFound,
    PrivateTeam,
    IdentifierConflict,
    AuthorRequired,
    CannotRelateSelf,
    RelationConflict,
    KeyConflict,
    DefaultStateInvalid,
    ActiveReference,
}

#[derive(Debug)]
pub(crate) enum StorageError {
    Domain(DomainFailure),
    Runtime(RuntimeFailure),
}

impl From<DomainFailure> for StorageError {
    fn from(value: DomainFailure) -> Self {
        Self::Domain(value)
    }
}

impl From<RuntimeFailure> for StorageError {
    fn from(value: RuntimeFailure) -> Self {
        Self::Runtime(value)
    }
}

fn runtime(operation: &'static str, source: impl fmt::Display) -> StorageError {
    StorageError::Runtime(RuntimeFailure::PluginFailure {
        detail: format!("Projects PostgreSQL operation `{operation}` failed: {source}"),
    })
}

enum CommandStart {
    New,
    Replay(Value),
    Conflict,
}

async fn reserve_command<T: Serialize>(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    actor: &str,
    operation: &str,
    idempotency_key: &str,
    request: &T,
) -> Result<CommandStart, StorageError> {
    let request = serde_json::to_value(request)
        .map_err(|error| runtime("serialize command request", error))?;
    let inserted = sqlx::query("INSERT INTO project_commands(caller_instance,actor_subject,operation,idempotency_key,request) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
        .bind(caller)
        .bind(actor)
        .bind(operation)
        .bind(idempotency_key)
        .bind(sqlx::types::Json(request.clone()))
        .execute(&mut **transaction)
        .await
        .map_err(|error| runtime("reserve command", error))?;
    if inserted.rows_affected() == 1 {
        return Ok(CommandStart::New);
    }
    let row = sqlx::query("SELECT request,response FROM project_commands WHERE caller_instance=$1 AND actor_subject=$2 AND operation=$3 AND idempotency_key=$4 FOR UPDATE")
        .bind(caller)
        .bind(actor)
        .bind(operation)
        .bind(idempotency_key)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| runtime("read command replay", error))?;
    let stored: sqlx::types::Json<Value> = row
        .try_get("request")
        .map_err(|error| runtime("decode command request", error))?;
    if stored.0 != request {
        return Ok(CommandStart::Conflict);
    }
    let response: Option<sqlx::types::Json<Value>> = row
        .try_get("response")
        .map_err(|error| runtime("decode command response", error))?;
    response.map_or_else(
        || {
            Err(runtime(
                "read command replay",
                "completed response is missing",
            ))
        },
        |value| Ok(CommandStart::Replay(value.0)),
    )
}

async fn complete_command<T: Serialize>(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    actor: &str,
    operation: &str,
    idempotency_key: &str,
    response: &T,
) -> Result<(), StorageError> {
    let response = serde_json::to_value(response)
        .map_err(|error| runtime("serialize command response", error))?;
    let updated = sqlx::query("UPDATE project_commands SET response=$5,completed_at=transaction_timestamp() WHERE caller_instance=$1 AND actor_subject=$2 AND operation=$3 AND idempotency_key=$4 AND response IS NULL")
        .bind(caller)
        .bind(actor)
        .bind(operation)
        .bind(idempotency_key)
        .bind(sqlx::types::Json(response))
        .execute(&mut **transaction)
        .await
        .map_err(|error| runtime("complete command", error))?;
    if updated.rows_affected() != 1 {
        return Err(runtime(
            "complete command",
            "reserved command was not pending",
        ));
    }
    Ok(())
}

fn replay<T: DeserializeOwned>(value: Value) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(|error| runtime("decode command replay", error))
}

async fn append_activity(
    transaction: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    project_id: Option<&str>,
    issue_id: Option<&str>,
    actor: &str,
    operation: &str,
    entity_kind: &str,
    entity_id: &str,
    revision: Option<i64>,
) -> Result<(), StorageError> {
    // PostgreSQL identities are allocated before commit. Without a commit gate, a later
    // transaction could publish activity N+1 while activity N remains uncommitted, causing a
    // checkpointing consumer to skip N forever. Holding this schema-scoped advisory lock from
    // allocation through commit makes activity IDs a safe exclusive high-water cursor.
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext(current_schema()), hashtext('lenso.projects.activity.v1'))",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| runtime("acquire activity commit gate", error))?;
    sqlx::query("INSERT INTO project_activity(organization_id,project_id,issue_id,actor_subject,operation,entity_kind,entity_id,revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(organization_id)
        .bind(project_id)
        .bind(issue_id)
        .bind(actor)
        .bind(operation)
        .bind(entity_kind)
        .bind(entity_id)
        .bind(revision)
        .execute(&mut **transaction)
        .await
        .map_err(|error| runtime("append activity", error))?;
    Ok(())
}

fn format_time(value: OffsetDateTime) -> Result<String, StorageError> {
    value
        .format(&Rfc3339)
        .map_err(|error| runtime("format timestamp", error))
}

fn format_date(value: Option<Date>) -> Option<String> {
    value.map(|date| date.to_string())
}

fn fresh_catalog_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

async fn ensure_project_status_catalog(
    connection: &mut PgConnection,
    organization_id: &str,
) -> Result<String, StorageError> {
    let proposed_default = fresh_catalog_id("project_status");
    let inserted: Option<String> = sqlx::query_scalar(
        "INSERT INTO project_workspaces(organization_id,default_project_status_id) VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING default_project_status_id",
    )
    .bind(organization_id)
    .bind(&proposed_default)
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| runtime("initialize project workspace", error))?;
    if let Some(default_id) = inserted {
        let defaults = [
            (default_id.clone(), "Backlog", "backlog", "#6B7280", 0, true),
            (
                fresh_catalog_id("project_status"),
                "Planned",
                "planned",
                "#8B5CF6",
                1,
                false,
            ),
            (
                fresh_catalog_id("project_status"),
                "In Progress",
                "started",
                "#3B82F6",
                2,
                false,
            ),
            (
                fresh_catalog_id("project_status"),
                "Paused",
                "paused",
                "#F59E0B",
                3,
                false,
            ),
            (
                fresh_catalog_id("project_status"),
                "Completed",
                "completed",
                "#10B981",
                4,
                false,
            ),
            (
                fresh_catalog_id("project_status"),
                "Canceled",
                "canceled",
                "#9CA3AF",
                5,
                false,
            ),
        ];
        for (status_id, name, category, color, position, is_default) in defaults {
            sqlx::query("INSERT INTO project_statuses(status_id,organization_id,name,category,color,position,is_default) VALUES($1,$2,$3,$4,$5,$6,$7)")
                .bind(status_id)
                .bind(organization_id)
                .bind(name)
                .bind(category)
                .bind(color)
                .bind(position)
                .bind(is_default)
                .execute(&mut *connection)
                .await
                .map_err(|error| runtime("initialize project statuses", error))?;
        }
        return Ok(default_id);
    }
    sqlx::query_scalar(
        "SELECT default_project_status_id FROM project_workspaces WHERE organization_id=$1",
    )
    .bind(organization_id)
    .fetch_one(&mut *connection)
    .await
    .map_err(|error| runtime("load default project status", error))
}

async fn resolve_project_status(
    connection: &mut PgConnection,
    organization_id: &str,
    requested_status_id: Option<&str>,
) -> Result<(String, String), StorageError> {
    let default_id = ensure_project_status_catalog(connection, organization_id).await?;
    let status_id = requested_status_id.unwrap_or(&default_id);
    sqlx::query_as(
        "SELECT status_id,category FROM project_statuses WHERE organization_id=$1 AND status_id=$2 AND NOT archived FOR KEY SHARE",
    )
    .bind(organization_id)
    .bind(status_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| runtime("resolve project status", error))?
    .ok_or_else(|| DomainFailure::ProjectStatusNotFound.into())
}

async fn resolve_issue_workflow_state(
    connection: &mut PgConnection,
    organization_id: &str,
    team_id: &str,
    requested_state_id: Option<&str>,
) -> Result<String, StorageError> {
    let state_id: Option<String> = sqlx::query_scalar(
        "SELECT ws.state_id FROM teams t JOIN workflow_states ws ON ws.organization_id=t.organization_id AND ws.team_id=t.team_id AND ws.state_id=COALESCE($3,t.default_workflow_state_id) WHERE t.organization_id=$1 AND t.team_id=$2 AND NOT ws.archived FOR KEY SHARE OF ws",
    )
    .bind(organization_id)
    .bind(team_id)
    .bind(requested_state_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| runtime("resolve issue workflow state", error))?;
    state_id.ok_or_else(|| DomainFailure::WorkflowStateNotFound.into())
}

pub(crate) fn parse_date(value: &Option<String>) -> Result<Option<Date>, DomainFailure> {
    value
        .as_deref()
        .map(|date| Date::parse(date, &time::format_description::well_known::Iso8601::DATE))
        .transpose()
        .map_err(|_| DomainFailure::InvalidRequest)
}

pub(crate) fn parse_revision(value: &str) -> Result<i64, DomainFailure> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(DomainFailure::InvalidRequest)
}

pub(crate) fn parse_cursor(value: &Option<String>) -> Result<i64, DomainFailure> {
    value.as_deref().map_or(Ok(0), parse_revision)
}

async fn project_visibility(
    pool: &PgPool,
    organization_id: &str,
    project_id: &str,
    actor: &str,
) -> Result<Option<bool>, StorageError> {
    let row = sqlx::query("SELECT NOT EXISTS (SELECT 1 FROM project_teams pt JOIN teams t ON t.organization_id=pt.organization_id AND t.team_id=pt.team_id WHERE pt.organization_id=p.organization_id AND pt.project_id=p.project_id AND t.private AND NOT EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=t.organization_id AND tm.team_id=t.team_id AND tm.subject=$3 AND tm.active)) AS visible FROM projects p WHERE p.organization_id=$1 AND p.project_id=$2")
        .bind(organization_id)
        .bind(project_id)
        .bind(actor)
        .fetch_optional(pool)
        .await
        .map_err(|error| runtime("check project visibility", error))?;
    row.map(|row| {
        row.try_get("visible")
            .map_err(|error| runtime("decode project visibility", error))
    })
    .transpose()
}

async fn issue_visibility(
    pool: &PgPool,
    organization_id: &str,
    issue_id: &str,
    actor: &str,
) -> Result<Option<bool>, StorageError> {
    let row = sqlx::query("SELECT (NOT t.private OR EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=t.organization_id AND tm.team_id=t.team_id AND tm.subject=$3 AND tm.active)) AND NOT EXISTS (SELECT 1 FROM project_teams pt JOIN teams attached ON attached.organization_id=pt.organization_id AND attached.team_id=pt.team_id WHERE pt.organization_id=i.organization_id AND pt.project_id=i.project_id AND attached.private AND NOT EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=attached.organization_id AND tm.team_id=attached.team_id AND tm.subject=$3 AND tm.active)) AS visible FROM issues i JOIN teams t ON t.organization_id=i.organization_id AND t.team_id=i.team_id WHERE i.organization_id=$1 AND i.issue_id=$2")
        .bind(organization_id)
        .bind(issue_id)
        .bind(actor)
        .fetch_optional(pool)
        .await
        .map_err(|error| runtime("check issue visibility", error))?;
    row.map(|row| {
        row.try_get("visible")
            .map_err(|error| runtime("decode issue visibility", error))
    })
    .transpose()
}

async fn load_project_value(
    connection: &mut PgConnection,
    organization_id: &str,
    project_id: &str,
) -> Result<Option<Value>, StorageError> {
    let row = sqlx::query("SELECT p.project_id,p.organization_id,p.name,p.summary,p.lead_team_id,p.status_id,p.milestone_id,p.starts_on,p.target_date,p.completed_at,p.canceled_at,p.archived,p.revision,p.created_at,p.updated_at,ARRAY_AGG(pt.team_id ORDER BY pt.team_id) AS team_ids FROM projects p JOIN project_teams pt ON pt.organization_id=p.organization_id AND pt.project_id=p.project_id WHERE p.organization_id=$1 AND p.project_id=$2 GROUP BY p.project_id")
        .bind(organization_id)
        .bind(project_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| runtime("load project", error))?;
    row.map(|row| {
        let created_at: OffsetDateTime = row.try_get("created_at").map_err(|error| runtime("decode project", error))?;
        let updated_at: OffsetDateTime = row.try_get("updated_at").map_err(|error| runtime("decode project", error))?;
        let starts_on: Option<Date> = row.try_get("starts_on").map_err(|error| runtime("decode project", error))?;
        let target_date: Option<Date> = row.try_get("target_date").map_err(|error| runtime("decode project", error))?;
        Ok(json!({
            "project_id": row.try_get::<String,_>("project_id").map_err(|error| runtime("decode project", error))?,
            "organization_id": row.try_get::<String,_>("organization_id").map_err(|error| runtime("decode project", error))?,
            "name": row.try_get::<String,_>("name").map_err(|error| runtime("decode project", error))?,
            "summary": row.try_get::<Option<String>,_>("summary").map_err(|error| runtime("decode project", error))?,
            "lead_team_id": row.try_get::<String,_>("lead_team_id").map_err(|error| runtime("decode project", error))?,
            "team_ids": row.try_get::<Vec<String>,_>("team_ids").map_err(|error| runtime("decode project", error))?,
            "status_id": row.try_get::<String,_>("status_id").map_err(|error| runtime("decode project", error))?,
            "milestone_id": row.try_get::<Option<String>,_>("milestone_id").map_err(|error| runtime("decode project", error))?,
            "start_date": format_date(starts_on), "target_date": format_date(target_date),
            "completed_at": row.try_get::<Option<OffsetDateTime>,_>("completed_at").map_err(|error| runtime("decode project", error))?.map(format_time).transpose()?,
            "canceled_at": row.try_get::<Option<OffsetDateTime>,_>("canceled_at").map_err(|error| runtime("decode project", error))?.map(format_time).transpose()?,
            "archived": row.try_get::<bool,_>("archived").map_err(|error| runtime("decode project", error))?,
            "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode project", error))?.to_string(),
            "created_at": format_time(created_at)?, "updated_at": format_time(updated_at)?
        }))
    }).transpose()
}

async fn load_issue_value(
    connection: &mut PgConnection,
    organization_id: &str,
    issue_id: &str,
) -> Result<Option<Value>, StorageError> {
    let row = sqlx::query("SELECT issue_id,organization_id,identifier,project_id,team_id,title,description,priority,workflow_state_id,cycle_id,milestone_id,parent_issue_id,archived,revision,created_at,updated_at FROM issues WHERE organization_id=$1 AND issue_id=$2")
        .bind(organization_id)
        .bind(issue_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| runtime("load issue", error))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let identifier: String = row
        .try_get("identifier")
        .map_err(|error| runtime("decode issue", error))?;
    let previous = sqlx::query("SELECT identifier FROM issue_identifier_aliases WHERE organization_id=$1 AND issue_id=$2 AND identifier<>$3 ORDER BY created_at,identifier")
        .bind(organization_id).bind(issue_id).bind(&identifier).fetch_all(&mut *connection).await.map_err(|error| runtime("load issue identifier aliases", error))?
        .into_iter().map(|row| row.try_get("identifier").map_err(|error| runtime("decode issue alias", error))).collect::<Result<Vec<String>,_>>()?;
    let labels = sqlx::query("SELECT label_id FROM issue_labels WHERE organization_id=$1 AND issue_id=$2 ORDER BY label_id")
        .bind(organization_id).bind(issue_id).fetch_all(&mut *connection).await.map_err(|error| runtime("load issue labels", error))?
        .into_iter().map(|row| row.try_get("label_id").map_err(|error| runtime("decode issue label", error))).collect::<Result<Vec<String>,_>>()?;
    let created_at: OffsetDateTime = row
        .try_get("created_at")
        .map_err(|error| runtime("decode issue", error))?;
    let updated_at: OffsetDateTime = row
        .try_get("updated_at")
        .map_err(|error| runtime("decode issue", error))?;
    Ok(Some(json!({
        "issue_id": issue_id, "organization_id": organization_id, "identifier": identifier,
        "previous_identifiers": previous, "project_id": row.try_get::<String,_>("project_id").map_err(|error| runtime("decode issue", error))?,
        "team_id": row.try_get::<String,_>("team_id").map_err(|error| runtime("decode issue", error))?,
        "title": row.try_get::<String,_>("title").map_err(|error| runtime("decode issue", error))?,
        "description": row.try_get::<Option<String>,_>("description").map_err(|error| runtime("decode issue", error))?,
        "priority": row.try_get::<String,_>("priority").map_err(|error| runtime("decode issue", error))?,
        "workflow_state_id": row.try_get::<String,_>("workflow_state_id").map_err(|error| runtime("decode issue", error))?,
        "cycle_id": row.try_get::<Option<String>,_>("cycle_id").map_err(|error| runtime("decode issue", error))?,
        "milestone_id": row.try_get::<Option<String>,_>("milestone_id").map_err(|error| runtime("decode issue", error))?,
        "parent_issue_id": row.try_get::<Option<String>,_>("parent_issue_id").map_err(|error| runtime("decode issue", error))?,
        "label_ids": labels, "archived": row.try_get::<bool,_>("archived").map_err(|error| runtime("decode issue", error))?,
        "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode issue", error))?.to_string(),
        "created_at": format_time(created_at)?, "updated_at": format_time(updated_at)?
    })))
}

async fn resolve_issue_id(
    pool: &PgPool,
    organization_id: &str,
    issue_ref: &str,
) -> Result<Option<String>, StorageError> {
    sqlx::query_scalar("SELECT issue_id FROM (SELECT issue_id,0 AS priority FROM issues WHERE organization_id=$1 AND issue_id=$2 UNION ALL SELECT issue_id,1 AS priority FROM issue_identifier_aliases WHERE organization_id=$1 AND identifier=$2) resolved_refs ORDER BY priority LIMIT 1")
        .bind(organization_id).bind(issue_ref).fetch_optional(pool).await.map_err(|error| runtime("resolve issue reference", error))
}

async fn project_teams_cover_existing_issues(
    connection: &mut PgConnection,
    organization_id: &str,
    project_id: &str,
    team_ids: &[String],
) -> Result<bool, StorageError> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM issues WHERE organization_id=$1 AND project_id=$2 AND NOT (team_id=ANY($3)))")
        .bind(organization_id)
        .bind(project_id)
        .bind(team_ids)
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| runtime("validate project issue teams", error))
}

async fn parent_would_create_cycle(
    connection: &mut PgConnection,
    organization_id: &str,
    issue_id: &str,
    parent_issue_id: Option<&str>,
) -> Result<bool, StorageError> {
    let Some(parent_issue_id) = parent_issue_id else {
        return Ok(false);
    };
    sqlx::query_scalar("WITH RECURSIVE descendants(issue_id) AS (SELECT issue_id FROM issues WHERE organization_id=$1 AND parent_issue_id=$2 UNION ALL SELECT child.issue_id FROM issues child JOIN descendants parent ON child.parent_issue_id=parent.issue_id WHERE child.organization_id=$1) SELECT EXISTS(SELECT 1 FROM descendants WHERE issue_id=$3)")
        .bind(organization_id)
        .bind(issue_id)
        .bind(parent_issue_id)
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| runtime("validate issue parent cycle", error))
}

async fn teams_are_visible(
    pool: &PgPool,
    organization_id: &str,
    team_ids: &[String],
    actor: &str,
) -> Result<Result<(), DomainFailure>, StorageError> {
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM teams WHERE organization_id=$1 AND team_id=ANY($2)",
    )
    .bind(organization_id)
    .bind(team_ids)
    .fetch_one(pool)
    .await
    .map_err(|error| runtime("validate project teams", error))?;
    if usize::try_from(rows).ok() != Some(team_ids.len()) {
        return Ok(Err(DomainFailure::TeamNotFound));
    }
    let hidden: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM teams t WHERE t.organization_id=$1 AND t.team_id=ANY($2) AND t.private AND NOT EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=t.organization_id AND tm.team_id=t.team_id AND tm.subject=$3 AND tm.active)")
        .bind(organization_id).bind(team_ids).bind(actor).fetch_one(pool).await.map_err(|error| runtime("validate private project teams", error))?;
    if hidden > 0 {
        return Ok(Err(DomainFailure::PrivateTeam));
    }
    Ok(Ok(()))
}

async fn project_dependencies_exist(
    pool: &PgPool,
    organization_id: &str,
    project_id: Option<&str>,
    milestone_id: Option<&str>,
) -> Result<Result<(), DomainFailure>, StorageError> {
    if let Some(milestone_id) = milestone_id {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM milestones WHERE organization_id=$1 AND project_id=$2 AND milestone_id=$3)")
            .bind(organization_id).bind(project_id).bind(milestone_id).fetch_one(pool).await.map_err(|error| runtime("validate project milestone", error))?;
        if !exists {
            return Ok(Err(DomainFailure::MilestoneNotFound));
        }
    }
    Ok(Ok(()))
}

pub(crate) async fn create_project(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::CreateProjectRequest,
) -> Result<projects::CreateProjectResponse, StorageError> {
    teams_are_visible(
        postgres.pool(),
        &request.organization_id,
        &request.team_ids,
        actor,
    )
    .await??;
    project_dependencies_exist(
        postgres.pool(),
        &request.organization_id,
        Some(&request.project_id),
        request.milestone_id.as_deref(),
    )
    .await??;
    let starts_on = parse_date(&request.start_date)?;
    let target_date = parse_date(&request.target_date)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin create project", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::CREATE_PROJECT_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit create project replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let (status_id, status_category) = resolve_project_status(
        &mut tx,
        &request.organization_id,
        request.status_id.as_deref(),
    )
    .await?;
    let inserted = sqlx::query("INSERT INTO projects(project_id,organization_id,name,summary,lead_team_id,status_id,milestone_id,starts_on,target_date,completed_at,canceled_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,CASE WHEN $10='completed' THEN transaction_timestamp() END,CASE WHEN $10='canceled' THEN transaction_timestamp() END) ON CONFLICT DO NOTHING")
        .bind(&request.project_id).bind(&request.organization_id).bind(request.name.trim()).bind(request.summary.as_deref().map(str::trim))
        .bind(&request.lead_team_id).bind(&status_id).bind(&request.milestone_id).bind(starts_on).bind(target_date).bind(status_category)
        .execute(&mut *tx).await.map_err(|error| runtime("insert project", error))?;
    if inserted.rows_affected() != 1 {
        return Err(DomainFailure::IdentifierConflict.into());
    }
    for team_id in &request.team_ids {
        sqlx::query(
            "INSERT INTO project_teams(organization_id,project_id,team_id) VALUES($1,$2,$3)",
        )
        .bind(&request.organization_id)
        .bind(&request.project_id)
        .bind(team_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| runtime("attach project team", error))?;
    }
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&request.project_id),
        None,
        actor,
        projects::CREATE_PROJECT_OPERATION,
        "project",
        &request.project_id,
        Some(1),
    )
    .await?;
    let value = load_project_value(&mut tx, &request.organization_id, &request.project_id)
        .await?
        .ok_or_else(|| runtime("load created project", "project disappeared"))?;
    let response = replay(value)?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::CREATE_PROJECT_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit create project", error))?;
    Ok(response)
}

pub(crate) async fn get_project(
    postgres: &OwnedPostgres,
    actor: &str,
    request: &projects::GetProjectRequest,
) -> Result<projects::GetProjectResponse, StorageError> {
    match project_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.project_id,
        actor,
    )
    .await?
    {
        None => return Err(DomainFailure::NotFound.into()),
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        Some(true) => {}
    }
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire project reader", error))?;
    let value = load_project_value(
        &mut connection,
        &request.organization_id,
        &request.project_id,
    )
    .await?
    .ok_or(DomainFailure::NotFound)?;
    replay(value)
}

pub(crate) async fn list_projects(
    postgres: &OwnedPostgres,
    actor: &str,
    request: &projects::ListProjectsRequest,
) -> Result<projects::ListProjectsResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let fetch_limit = request.limit + 1;
    let rows = sqlx::query("SELECT DISTINCT p.project_id,p.row_seq FROM projects p JOIN project_teams filter_pt ON filter_pt.organization_id=p.organization_id AND filter_pt.project_id=p.project_id WHERE p.organization_id=$1 AND p.row_seq>$2 AND ($3::text IS NULL OR filter_pt.team_id=$3) AND ($4 OR NOT p.archived) AND NOT EXISTS (SELECT 1 FROM project_teams pt JOIN teams t ON t.organization_id=pt.organization_id AND t.team_id=pt.team_id WHERE pt.organization_id=p.organization_id AND pt.project_id=p.project_id AND t.private AND NOT EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=t.organization_id AND tm.team_id=t.team_id AND tm.subject=$5 AND tm.active)) ORDER BY p.row_seq LIMIT $6")
        .bind(&request.organization_id).bind(after).bind(&request.team_id).bind(request.include_archived).bind(actor).bind(fetch_limit)
        .fetch_all(postgres.pool()).await.map_err(|error| runtime("list projects", error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire project list reader", error))?;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let project_id: String = row
            .try_get("project_id")
            .map_err(|error| runtime("decode project cursor", error))?;
        let row_seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode project cursor", error))?;
        let value = load_project_value(&mut connection, &request.organization_id, &project_id)
            .await?
            .ok_or_else(|| runtime("load listed project", "project disappeared"))?;
        items.push(
            serde_json::from_value(value)
                .map_err(|error| runtime("decode project list item", error))?,
        );
        next_cursor = Some(row_seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(projects::ListProjectsResponse { items, next_cursor })
}

pub(crate) async fn update_project(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::UpdateProjectRequest,
) -> Result<projects::UpdateProjectResponse, StorageError> {
    match project_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.project_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    teams_are_visible(
        postgres.pool(),
        &request.organization_id,
        &request.team_ids,
        actor,
    )
    .await??;
    project_dependencies_exist(
        postgres.pool(),
        &request.organization_id,
        Some(&request.project_id),
        request.milestone_id.as_deref(),
    )
    .await??;
    let expected = parse_revision(&request.expected_revision)?;
    let starts_on = parse_date(&request.start_date)?;
    let target_date = parse_date(&request.target_date)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin update project", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::UPDATE_PROJECT_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit update project replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    if !project_teams_cover_existing_issues(
        &mut tx,
        &request.organization_id,
        &request.project_id,
        &request.team_ids,
    )
    .await?
    {
        return Err(DomainFailure::ActiveReference.into());
    }
    let (status_id, status_category) =
        resolve_project_status(&mut tx, &request.organization_id, Some(&request.status_id)).await?;
    let updated: Option<i64> = sqlx::query_scalar("UPDATE projects SET name=$4,summary=$5,lead_team_id=$6,status_id=$7,milestone_id=$8,starts_on=$9,target_date=$10,completed_at=CASE WHEN $11='completed' THEN COALESCE(completed_at,transaction_timestamp()) ELSE NULL END,canceled_at=CASE WHEN $11='canceled' THEN COALESCE(canceled_at,transaction_timestamp()) ELSE NULL END,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND project_id=$2 AND revision=$3 RETURNING revision")
        .bind(&request.organization_id).bind(&request.project_id).bind(expected).bind(request.name.trim()).bind(request.summary.as_deref().map(str::trim)).bind(&request.lead_team_id).bind(status_id).bind(&request.milestone_id).bind(starts_on).bind(target_date).bind(status_category)
        .fetch_optional(&mut *tx).await.map_err(|error| runtime("update project", error))?;
    let Some(revision) = updated else {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE organization_id=$1 AND project_id=$2)",
        )
        .bind(&request.organization_id)
        .bind(&request.project_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| runtime("classify project revision", error))?;
        return Err(if exists {
            DomainFailure::RevisionConflict
        } else {
            DomainFailure::NotFound
        }
        .into());
    };
    sqlx::query("DELETE FROM project_teams WHERE organization_id=$1 AND project_id=$2")
        .bind(&request.organization_id)
        .bind(&request.project_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| runtime("replace project teams", error))?;
    for team_id in &request.team_ids {
        sqlx::query(
            "INSERT INTO project_teams(organization_id,project_id,team_id) VALUES($1,$2,$3)",
        )
        .bind(&request.organization_id)
        .bind(&request.project_id)
        .bind(team_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| runtime("replace project team", error))?;
    }
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&request.project_id),
        None,
        actor,
        projects::UPDATE_PROJECT_OPERATION,
        "project",
        &request.project_id,
        Some(revision),
    )
    .await?;
    let value = load_project_value(&mut tx, &request.organization_id, &request.project_id)
        .await?
        .ok_or_else(|| runtime("load updated project", "project disappeared"))?;
    let response = replay(value)?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::UPDATE_PROJECT_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit update project", error))?;
    Ok(response)
}

pub(crate) async fn archive_project(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::ArchiveProjectRequest,
) -> Result<projects::ArchiveProjectResponse, StorageError> {
    match project_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.project_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin archive project", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::ARCHIVE_PROJECT_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit archive project replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let revision: Option<i64> = sqlx::query_scalar("UPDATE projects SET archived=$4,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND project_id=$2 AND revision=$3 RETURNING revision")
        .bind(&request.organization_id).bind(&request.project_id).bind(expected).bind(request.archived).fetch_optional(&mut *tx).await.map_err(|error| runtime("archive project", error))?;
    let Some(revision) = revision else {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE organization_id=$1 AND project_id=$2)",
        )
        .bind(&request.organization_id)
        .bind(&request.project_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| runtime("classify archive project", error))?;
        return Err(if exists {
            DomainFailure::RevisionConflict
        } else {
            DomainFailure::NotFound
        }
        .into());
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&request.project_id),
        None,
        actor,
        projects::ARCHIVE_PROJECT_OPERATION,
        "project",
        &request.project_id,
        Some(revision),
    )
    .await?;
    let value = load_project_value(&mut tx, &request.organization_id, &request.project_id)
        .await?
        .ok_or_else(|| runtime("load archived project", "project disappeared"))?;
    let response = replay(value)?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::ARCHIVE_PROJECT_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit archive project", error))?;
    Ok(response)
}

fn priority_name(priority: &projects::Priority) -> &'static str {
    match priority {
        projects::Priority::None => "none",
        projects::Priority::Urgent => "urgent",
        projects::Priority::High => "high",
        projects::Priority::Medium => "medium",
        projects::Priority::Low => "low",
    }
}

async fn issue_dependencies_exist(
    pool: &PgPool,
    organization_id: &str,
    project_id: &str,
    team_id: &str,
    cycle_id: Option<&str>,
    milestone_id: Option<&str>,
    parent_issue_id: Option<&str>,
    label_ids: &[String],
) -> Result<Result<(), DomainFailure>, StorageError> {
    let attached: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project_teams WHERE organization_id=$1 AND project_id=$2 AND team_id=$3)")
        .bind(organization_id).bind(project_id).bind(team_id).fetch_one(pool).await.map_err(|error| runtime("validate issue project team", error))?;
    if !attached {
        return Ok(Err(DomainFailure::TeamNotFound));
    }
    if let Some(cycle_id) = cycle_id {
        let cycle: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM cycles WHERE organization_id=$1 AND team_id=$2 AND cycle_id=$3)")
            .bind(organization_id).bind(team_id).bind(cycle_id).fetch_one(pool).await.map_err(|error| runtime("validate issue cycle", error))?;
        if !cycle {
            return Ok(Err(DomainFailure::CycleNotFound));
        }
    }
    if let Some(milestone_id) = milestone_id {
        let milestone: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM milestones WHERE organization_id=$1 AND project_id=$2 AND milestone_id=$3)")
            .bind(organization_id).bind(project_id).bind(milestone_id).fetch_one(pool).await.map_err(|error| runtime("validate issue milestone", error))?;
        if !milestone {
            return Ok(Err(DomainFailure::MilestoneNotFound));
        }
    }
    if let Some(parent_issue_id) = parent_issue_id {
        let parent: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM issues WHERE organization_id=$1 AND project_id=$2 AND issue_id=$3)")
            .bind(organization_id).bind(project_id).bind(parent_issue_id).fetch_one(pool).await.map_err(|error| runtime("validate parent issue", error))?;
        if !parent {
            return Ok(Err(DomainFailure::ParentNotFound));
        }
    }
    if !label_ids.is_empty() {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM labels WHERE organization_id=$1 AND label_id=ANY($2) AND (team_id IS NULL OR team_id=$3)")
            .bind(organization_id).bind(label_ids).bind(team_id).fetch_one(pool).await.map_err(|error| runtime("validate issue labels", error))?;
        if usize::try_from(count).ok() != Some(label_ids.len()) {
            return Ok(Err(DomainFailure::LabelNotFound));
        }
    }
    Ok(Ok(()))
}

pub(crate) async fn create_issue(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::CreateIssueRequest,
) -> Result<projects::CreateIssueResponse, StorageError> {
    match project_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.project_id,
        actor,
    )
    .await?
    {
        None => return Err(DomainFailure::NotFound.into()),
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        Some(true) => {}
    }
    issue_dependencies_exist(
        postgres.pool(),
        &request.organization_id,
        &request.project_id,
        &request.team_id,
        request.cycle_id.as_deref(),
        request.milestone_id.as_deref(),
        request.parent_issue_id.as_deref(),
        &request.label_ids,
    )
    .await??;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin create issue", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::CREATE_ISSUE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit create issue replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let workflow_state_id = resolve_issue_workflow_state(
        &mut tx,
        &request.organization_id,
        &request.team_id,
        request.workflow_state_id.as_deref(),
    )
    .await?;
    let counter = sqlx::query("UPDATE teams SET next_issue_number=next_issue_number+1 WHERE organization_id=$1 AND team_id=$2 RETURNING team_key,next_issue_number-1 AS issue_number")
        .bind(&request.organization_id).bind(&request.team_id).fetch_optional(&mut *tx).await.map_err(|error| runtime("allocate issue identifier", error))?.ok_or(DomainFailure::TeamNotFound)?;
    let team_key: String = counter
        .try_get("team_key")
        .map_err(|error| runtime("decode issue team key", error))?;
    let number: i64 = counter
        .try_get("issue_number")
        .map_err(|error| runtime("decode issue number", error))?;
    let identifier = format!("{team_key}-{number}");
    let inserted = sqlx::query("INSERT INTO issues(issue_id,organization_id,project_id,team_id,identifier,title,description,priority,workflow_state_id,cycle_id,milestone_id,parent_issue_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING")
        .bind(&request.issue_id).bind(&request.organization_id).bind(&request.project_id).bind(&request.team_id).bind(&identifier).bind(request.title.trim()).bind(request.description.as_deref().map(str::trim)).bind(priority_name(&request.priority)).bind(workflow_state_id).bind(&request.cycle_id).bind(&request.milestone_id).bind(&request.parent_issue_id)
        .execute(&mut *tx).await.map_err(|error| runtime("insert issue", error))?;
    if inserted.rows_affected() != 1 {
        return Err(DomainFailure::IdentifierConflict.into());
    }
    sqlx::query("INSERT INTO issue_identifier_aliases(organization_id,identifier,issue_id) VALUES($1,$2,$3)")
        .bind(&request.organization_id).bind(&identifier).bind(&request.issue_id).execute(&mut *tx).await.map_err(|error| runtime("store issue identifier", error))?;
    for label_id in &request.label_ids {
        sqlx::query("INSERT INTO issue_labels(organization_id,issue_id,label_id) VALUES($1,$2,$3)")
            .bind(&request.organization_id)
            .bind(&request.issue_id)
            .bind(label_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| runtime("attach issue label", error))?;
    }
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&request.project_id),
        Some(&request.issue_id),
        actor,
        projects::CREATE_ISSUE_OPERATION,
        "issue",
        &request.issue_id,
        Some(1),
    )
    .await?;
    let value = load_issue_value(&mut tx, &request.organization_id, &request.issue_id)
        .await?
        .ok_or_else(|| runtime("load created issue", "issue disappeared"))?;
    let response = replay(value)?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::CREATE_ISSUE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit create issue", error))?;
    Ok(response)
}

pub(crate) async fn get_issue(
    postgres: &OwnedPostgres,
    actor: &str,
    request: &projects::GetIssueRequest,
) -> Result<projects::GetIssueResponse, StorageError> {
    let issue_id = resolve_issue_id(
        postgres.pool(),
        &request.organization_id,
        &request.issue_ref,
    )
    .await?
    .ok_or(DomainFailure::NotFound)?;
    match issue_visibility(postgres.pool(), &request.organization_id, &issue_id, actor).await? {
        None => return Err(DomainFailure::NotFound.into()),
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        Some(true) => {}
    }
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire issue reader", error))?;
    replay(
        load_issue_value(&mut connection, &request.organization_id, &issue_id)
            .await?
            .ok_or(DomainFailure::NotFound)?,
    )
}

pub(crate) async fn list_issues(
    postgres: &OwnedPostgres,
    actor: &str,
    request: &projects::ListIssuesRequest,
) -> Result<projects::ListIssuesResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let fetch_limit = request.limit + 1;
    let rows = sqlx::query("SELECT i.issue_id,i.row_seq FROM issues i JOIN teams t ON t.organization_id=i.organization_id AND t.team_id=i.team_id WHERE i.organization_id=$1 AND i.row_seq>$2 AND ($3::text IS NULL OR i.project_id=$3) AND ($4::text IS NULL OR i.team_id=$4) AND ($5::text IS NULL OR i.workflow_state_id=$5) AND ($6 OR NOT i.archived) AND (NOT t.private OR EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=t.organization_id AND tm.team_id=t.team_id AND tm.subject=$7 AND tm.active)) AND NOT EXISTS (SELECT 1 FROM project_teams pt JOIN teams attached ON attached.organization_id=pt.organization_id AND attached.team_id=pt.team_id WHERE pt.organization_id=i.organization_id AND pt.project_id=i.project_id AND attached.private AND NOT EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=attached.organization_id AND tm.team_id=attached.team_id AND tm.subject=$7 AND tm.active)) ORDER BY i.row_seq LIMIT $8")
        .bind(&request.organization_id).bind(after).bind(&request.project_id).bind(&request.team_id).bind(&request.workflow_state_id).bind(request.include_archived).bind(actor).bind(fetch_limit)
        .fetch_all(postgres.pool()).await.map_err(|error| runtime("list issues", error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire issue list reader", error))?;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let issue_id: String = row
            .try_get("issue_id")
            .map_err(|error| runtime("decode issue cursor", error))?;
        let row_seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode issue cursor", error))?;
        let value = load_issue_value(&mut connection, &request.organization_id, &issue_id)
            .await?
            .ok_or_else(|| runtime("load listed issue", "issue disappeared"))?;
        items.push(
            serde_json::from_value(value)
                .map_err(|error| runtime("decode issue list item", error))?,
        );
        next_cursor = Some(row_seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(projects::ListIssuesResponse { items, next_cursor })
}

pub(crate) async fn update_issue(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::UpdateIssueRequest,
) -> Result<projects::UpdateIssueResponse, StorageError> {
    let current = sqlx::query(
        "SELECT project_id,team_id FROM issues WHERE organization_id=$1 AND issue_id=$2",
    )
    .bind(&request.organization_id)
    .bind(&request.issue_id)
    .fetch_optional(postgres.pool())
    .await
    .map_err(|error| runtime("read issue for update", error))?
    .ok_or(DomainFailure::NotFound)?;
    let project_id: String = current
        .try_get("project_id")
        .map_err(|error| runtime("decode issue project", error))?;
    let team_id: String = current
        .try_get("team_id")
        .map_err(|error| runtime("decode issue team", error))?;
    match issue_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.issue_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    issue_dependencies_exist(
        postgres.pool(),
        &request.organization_id,
        &project_id,
        &team_id,
        request.cycle_id.as_deref(),
        request.milestone_id.as_deref(),
        request.parent_issue_id.as_deref(),
        &request.label_ids,
    )
    .await??;
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin update issue", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::UPDATE_ISSUE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit update issue replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    if parent_would_create_cycle(
        &mut tx,
        &request.organization_id,
        &request.issue_id,
        request.parent_issue_id.as_deref(),
    )
    .await?
    {
        return Err(DomainFailure::InvalidRequest.into());
    }
    let workflow_state_id = resolve_issue_workflow_state(
        &mut tx,
        &request.organization_id,
        &team_id,
        Some(&request.workflow_state_id),
    )
    .await?;
    let revision: Option<i64> = sqlx::query_scalar("UPDATE issues SET title=$4,description=$5,priority=$6,workflow_state_id=$7,cycle_id=$8,milestone_id=$9,parent_issue_id=$10,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND issue_id=$2 AND revision=$3 RETURNING revision")
        .bind(&request.organization_id).bind(&request.issue_id).bind(expected).bind(request.title.trim()).bind(request.description.as_deref().map(str::trim)).bind(priority_name(&request.priority)).bind(workflow_state_id).bind(&request.cycle_id).bind(&request.milestone_id).bind(&request.parent_issue_id)
        .fetch_optional(&mut *tx).await.map_err(|error| runtime("update issue", error))?;
    let Some(revision) = revision else {
        return Err(DomainFailure::RevisionConflict.into());
    };
    sqlx::query("DELETE FROM issue_labels WHERE organization_id=$1 AND issue_id=$2")
        .bind(&request.organization_id)
        .bind(&request.issue_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| runtime("replace issue labels", error))?;
    for label_id in &request.label_ids {
        sqlx::query("INSERT INTO issue_labels(organization_id,issue_id,label_id) VALUES($1,$2,$3)")
            .bind(&request.organization_id)
            .bind(&request.issue_id)
            .bind(label_id)
            .execute(&mut *tx)
            .await
            .map_err(|error| runtime("replace issue label", error))?;
    }
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&request.issue_id),
        actor,
        projects::UPDATE_ISSUE_OPERATION,
        "issue",
        &request.issue_id,
        Some(revision),
    )
    .await?;
    let value = load_issue_value(&mut tx, &request.organization_id, &request.issue_id)
        .await?
        .ok_or_else(|| runtime("load updated issue", "issue disappeared"))?;
    let response = replay(value)?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::UPDATE_ISSUE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit update issue", error))?;
    Ok(response)
}

pub(crate) async fn move_issue(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::MoveIssueRequest,
) -> Result<projects::MoveIssueResponse, StorageError> {
    let current =
        sqlx::query("SELECT project_id FROM issues WHERE organization_id=$1 AND issue_id=$2")
            .bind(&request.organization_id)
            .bind(&request.issue_id)
            .fetch_optional(postgres.pool())
            .await
            .map_err(|error| runtime("read issue for move", error))?
            .ok_or(DomainFailure::NotFound)?;
    let project_id: String = current
        .try_get("project_id")
        .map_err(|error| runtime("decode moved issue project", error))?;
    match issue_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.issue_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let target = vec![request.team_id.clone()];
    teams_are_visible(postgres.pool(), &request.organization_id, &target, actor).await??;
    issue_dependencies_exist(
        postgres.pool(),
        &request.organization_id,
        &project_id,
        &request.team_id,
        None,
        None,
        None,
        &[],
    )
    .await??;
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin move issue", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::MOVE_ISSUE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit move issue replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let workflow_state_id = resolve_issue_workflow_state(
        &mut tx,
        &request.organization_id,
        &request.team_id,
        Some(&request.workflow_state_id),
    )
    .await?;
    let counter = sqlx::query("UPDATE teams SET next_issue_number=next_issue_number+1 WHERE organization_id=$1 AND team_id=$2 RETURNING team_key,next_issue_number-1 AS issue_number")
        .bind(&request.organization_id).bind(&request.team_id).fetch_optional(&mut *tx).await.map_err(|error| runtime("allocate moved issue identifier", error))?.ok_or(DomainFailure::TeamNotFound)?;
    let team_key: String = counter
        .try_get("team_key")
        .map_err(|error| runtime("decode moved issue team key", error))?;
    let number: i64 = counter
        .try_get("issue_number")
        .map_err(|error| runtime("decode moved issue number", error))?;
    let identifier = format!("{team_key}-{number}");
    let revision: Option<i64> = sqlx::query_scalar("UPDATE issues SET team_id=$4,identifier=$5,workflow_state_id=$6,cycle_id=NULL,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND issue_id=$2 AND revision=$3 RETURNING revision")
        .bind(&request.organization_id).bind(&request.issue_id).bind(expected).bind(&request.team_id).bind(&identifier).bind(workflow_state_id).fetch_optional(&mut *tx).await.map_err(|error| runtime("move issue", error))?;
    let Some(revision) = revision else {
        return Err(DomainFailure::RevisionConflict.into());
    };
    sqlx::query("INSERT INTO issue_identifier_aliases(organization_id,identifier,issue_id) VALUES($1,$2,$3)").bind(&request.organization_id).bind(&identifier).bind(&request.issue_id).execute(&mut *tx).await.map_err(|error| runtime("store moved issue identifier", error))?;
    sqlx::query("DELETE FROM issue_labels il USING labels l WHERE il.organization_id=$1 AND il.issue_id=$2 AND l.organization_id=il.organization_id AND l.label_id=il.label_id AND l.team_id IS NOT NULL AND l.team_id<>$3")
        .bind(&request.organization_id).bind(&request.issue_id).bind(&request.team_id).execute(&mut *tx).await.map_err(|error| runtime("remove incompatible moved issue labels", error))?;
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&request.issue_id),
        actor,
        projects::MOVE_ISSUE_OPERATION,
        "issue",
        &request.issue_id,
        Some(revision),
    )
    .await?;
    let value = load_issue_value(&mut tx, &request.organization_id, &request.issue_id)
        .await?
        .ok_or_else(|| runtime("load moved issue", "issue disappeared"))?;
    let response = replay(value)?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::MOVE_ISSUE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit move issue", error))?;
    Ok(response)
}

pub(crate) async fn archive_issue(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::ArchiveIssueRequest,
) -> Result<projects::ArchiveIssueResponse, StorageError> {
    match issue_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.issue_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin archive issue", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::ARCHIVE_ISSUE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit archive issue replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let row = sqlx::query("UPDATE issues SET archived=$4,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND issue_id=$2 AND revision=$3 RETURNING revision,project_id")
        .bind(&request.organization_id).bind(&request.issue_id).bind(expected).bind(request.archived).fetch_optional(&mut *tx).await.map_err(|error| runtime("archive issue", error))?;
    let Some(row) = row else {
        return Err(DomainFailure::RevisionConflict.into());
    };
    let revision: i64 = row
        .try_get("revision")
        .map_err(|error| runtime("decode archived issue revision", error))?;
    let project_id: String = row
        .try_get("project_id")
        .map_err(|error| runtime("decode archived issue project", error))?;
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&request.issue_id),
        actor,
        projects::ARCHIVE_ISSUE_OPERATION,
        "issue",
        &request.issue_id,
        Some(revision),
    )
    .await?;
    let value = load_issue_value(&mut tx, &request.organization_id, &request.issue_id)
        .await?
        .ok_or_else(|| runtime("load archived issue", "issue disappeared"))?;
    let response = replay(value)?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::ARCHIVE_ISSUE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit archive issue", error))?;
    Ok(response)
}

pub(crate) async fn put_external_link(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &projects::PutExternalLinkRequest,
) -> Result<projects::PutExternalLinkResponse, StorageError> {
    match issue_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.issue_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let project_id: String = sqlx::query_scalar(
        "SELECT project_id FROM issues WHERE organization_id=$1 AND issue_id=$2",
    )
    .bind(&request.organization_id)
    .bind(&request.issue_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("read external link project", error))?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put external link", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        projects::PUT_EXTERNAL_LINK_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit external link replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let inserted = sqlx::query("INSERT INTO issue_external_links(organization_id,issue_id,provider,external_key,url,title) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
        .bind(&request.organization_id).bind(&request.issue_id).bind(&request.provider).bind(&request.external_key).bind(&request.url).bind(&request.title).execute(&mut *tx).await.map_err(|error| runtime("insert external link", error))?;
    let created = inserted.rows_affected() == 1;
    let existing_issue: String = sqlx::query_scalar("SELECT issue_id FROM issue_external_links WHERE organization_id=$1 AND provider=$2 AND external_key=$3 FOR UPDATE")
        .bind(&request.organization_id).bind(&request.provider).bind(&request.external_key).fetch_one(&mut *tx).await.map_err(|error| runtime("read external link", error))?;
    if existing_issue != request.issue_id {
        return Err(DomainFailure::IdentifierConflict.into());
    }
    if !created {
        sqlx::query("UPDATE issue_external_links SET url=$4,title=$5 WHERE organization_id=$1 AND provider=$2 AND external_key=$3")
            .bind(&request.organization_id).bind(&request.provider).bind(&request.external_key).bind(&request.url).bind(&request.title).execute(&mut *tx).await.map_err(|error| runtime("update external link", error))?;
    }
    let response = projects::PutExternalLinkResponse {
        created,
        issue_id: request.issue_id.clone(),
        provider: request.provider.clone(),
        external_key: request.external_key.clone(),
        url: request.url.clone(),
        title: request.title.clone(),
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&request.issue_id),
        actor,
        projects::PUT_EXTERNAL_LINK_OPERATION,
        "external_link",
        &request.external_key,
        None,
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        projects::PUT_EXTERNAL_LINK_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit external link", error))?;
    Ok(response)
}

pub(crate) async fn list_activity(
    postgres: &OwnedPostgres,
    actor: &str,
    request: &projects::ListActivityRequest,
) -> Result<projects::ListActivityResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    if let Some(project_id) = &request.project_id {
        match project_visibility(postgres.pool(), &request.organization_id, project_id, actor)
            .await?
        {
            Some(true) => {}
            Some(false) => return Err(DomainFailure::PrivateTeam.into()),
            None => return Err(DomainFailure::NotFound.into()),
        }
    }
    if let Some(issue_id) = &request.issue_id {
        match issue_visibility(postgres.pool(), &request.organization_id, issue_id, actor).await? {
            Some(true) => {}
            Some(false) => return Err(DomainFailure::PrivateTeam.into()),
            None => return Err(DomainFailure::NotFound.into()),
        }
    }
    let rows = sqlx::query("SELECT a.activity_id,a.organization_id,a.project_id,a.issue_id,a.actor_subject,a.operation,a.entity_kind,a.entity_id,a.revision,a.occurred_at FROM project_activity a WHERE a.organization_id=$1 AND a.activity_id>$2 AND (a.project_id IS NOT NULL OR a.issue_id IS NOT NULL) AND ($3::text IS NULL OR a.project_id=$3) AND ($4::text IS NULL OR a.issue_id=$4) AND (a.project_id IS NULL OR NOT EXISTS (SELECT 1 FROM project_teams pt JOIN teams t ON t.organization_id=pt.organization_id AND t.team_id=pt.team_id WHERE pt.organization_id=a.organization_id AND pt.project_id=a.project_id AND t.private AND NOT EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=t.organization_id AND tm.team_id=t.team_id AND tm.subject=$5 AND tm.active))) AND (a.issue_id IS NULL OR EXISTS (SELECT 1 FROM issues i JOIN teams t ON t.organization_id=i.organization_id AND t.team_id=i.team_id WHERE i.organization_id=a.organization_id AND i.issue_id=a.issue_id AND (NOT t.private OR EXISTS (SELECT 1 FROM team_members tm WHERE tm.organization_id=t.organization_id AND tm.team_id=t.team_id AND tm.subject=$5 AND tm.active)))) ORDER BY a.activity_id LIMIT $6")
        .bind(&request.organization_id).bind(after).bind(&request.project_id).bind(&request.issue_id).bind(actor).bind(request.limit + 1).fetch_all(postgres.pool()).await.map_err(|error| runtime("list activity", error))?;
    let mut items = Vec::new();
    let mut next_cursor = request.after.clone();
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let occurred_at: OffsetDateTime = row
            .try_get("occurred_at")
            .map_err(|error| runtime("decode activity", error))?;
        let activity_id: i64 = row
            .try_get("activity_id")
            .map_err(|error| runtime("decode activity", error))?;
        items.push(projects::Activity {
            activity_id: activity_id.to_string(),
            organization_id: row
                .try_get("organization_id")
                .map_err(|error| runtime("decode activity", error))?,
            project_id: row
                .try_get("project_id")
                .map_err(|error| runtime("decode activity", error))?,
            issue_id: row
                .try_get("issue_id")
                .map_err(|error| runtime("decode activity", error))?,
            actor_subject: row
                .try_get("actor_subject")
                .map_err(|error| runtime("decode activity", error))?,
            operation: row
                .try_get("operation")
                .map_err(|error| runtime("decode activity", error))?,
            entity_kind: row
                .try_get("entity_kind")
                .map_err(|error| runtime("decode activity", error))?,
            entity_id: row
                .try_get("entity_id")
                .map_err(|error| runtime("decode activity", error))?,
            revision: row
                .try_get::<Option<i64>, _>("revision")
                .map_err(|error| runtime("decode activity", error))?
                .map(|value| value.to_string()),
            occurred_at: format_time(occurred_at)?,
        });
        next_cursor = Some(activity_id.to_string());
    }
    Ok(projects::ListActivityResponse { items, next_cursor })
}

async fn load_comment_value(
    connection: &mut PgConnection,
    organization_id: &str,
    comment_id: &str,
) -> Result<Option<Value>, StorageError> {
    let row = sqlx::query("SELECT comment_id,organization_id,issue_id,author_subject,body,deleted,revision,created_at,updated_at FROM comments WHERE organization_id=$1 AND comment_id=$2")
        .bind(organization_id).bind(comment_id).fetch_optional(&mut *connection).await.map_err(|error| runtime("load comment", error))?;
    row.map(|row| {
        let created_at: OffsetDateTime = row.try_get("created_at").map_err(|error| runtime("decode comment", error))?;
        let updated_at: OffsetDateTime = row.try_get("updated_at").map_err(|error| runtime("decode comment", error))?;
        Ok(json!({"comment_id":comment_id,"organization_id":organization_id,"issue_id":row.try_get::<String,_>("issue_id").map_err(|error| runtime("decode comment", error))?,"author_subject":row.try_get::<String,_>("author_subject").map_err(|error| runtime("decode comment", error))?,"body":row.try_get::<String,_>("body").map_err(|error| runtime("decode comment", error))?,"deleted":row.try_get::<bool,_>("deleted").map_err(|error| runtime("decode comment", error))?,"revision":row.try_get::<i64,_>("revision").map_err(|error| runtime("decode comment", error))?.to_string(),"created_at":format_time(created_at)?,"updated_at":format_time(updated_at)?}))
    }).transpose()
}

pub(crate) async fn add_comment(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &collaboration::AddCommentRequest,
) -> Result<collaboration::AddCommentResponse, StorageError> {
    match issue_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.issue_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let project_id: String = sqlx::query_scalar(
        "SELECT project_id FROM issues WHERE organization_id=$1 AND issue_id=$2",
    )
    .bind(&request.organization_id)
    .bind(&request.issue_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("read comment project", error))?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin add comment", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        collaboration::ADD_COMMENT_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit add comment replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let inserted = sqlx::query("INSERT INTO comments(comment_id,organization_id,issue_id,author_subject,body) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
        .bind(&request.comment_id).bind(&request.organization_id).bind(&request.issue_id).bind(actor).bind(request.body.trim()).execute(&mut *tx).await.map_err(|error| runtime("insert comment", error))?;
    if inserted.rows_affected() != 1 {
        return Err(DomainFailure::IdentifierConflict.into());
    }
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&request.issue_id),
        actor,
        collaboration::ADD_COMMENT_OPERATION,
        "comment",
        &request.comment_id,
        Some(1),
    )
    .await?;
    let response = replay(
        load_comment_value(&mut tx, &request.organization_id, &request.comment_id)
            .await?
            .ok_or_else(|| runtime("load comment", "comment disappeared"))?,
    )?;
    complete_command(
        &mut tx,
        caller,
        actor,
        collaboration::ADD_COMMENT_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit add comment", error))?;
    Ok(response)
}

pub(crate) async fn update_comment(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &collaboration::UpdateCommentRequest,
) -> Result<collaboration::UpdateCommentResponse, StorageError> {
    let row = sqlx::query("SELECT c.issue_id,i.project_id,c.author_subject FROM comments c JOIN issues i ON i.organization_id=c.organization_id AND i.issue_id=c.issue_id WHERE c.organization_id=$1 AND c.comment_id=$2")
        .bind(&request.organization_id).bind(&request.comment_id).fetch_optional(postgres.pool()).await.map_err(|error| runtime("read comment for update",error))?.ok_or(DomainFailure::NotFound)?;
    let issue_id: String = row
        .try_get("issue_id")
        .map_err(|error| runtime("decode comment issue", error))?;
    let project_id: String = row
        .try_get("project_id")
        .map_err(|error| runtime("decode comment project", error))?;
    let author: String = row
        .try_get("author_subject")
        .map_err(|error| runtime("decode comment author", error))?;
    if author != actor {
        return Err(DomainFailure::AuthorRequired.into());
    }
    match issue_visibility(postgres.pool(), &request.organization_id, &issue_id, actor).await? {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin update comment", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        collaboration::UPDATE_COMMENT_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit update comment replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let revision:Option<i64>=sqlx::query_scalar("UPDATE comments SET body=$4,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND comment_id=$2 AND revision=$3 AND NOT deleted RETURNING revision").bind(&request.organization_id).bind(&request.comment_id).bind(expected).bind(request.body.trim()).fetch_optional(&mut *tx).await.map_err(|error|runtime("update comment",error))?;
    let Some(revision) = revision else {
        return Err(DomainFailure::RevisionConflict.into());
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&issue_id),
        actor,
        collaboration::UPDATE_COMMENT_OPERATION,
        "comment",
        &request.comment_id,
        Some(revision),
    )
    .await?;
    let response = replay(
        load_comment_value(&mut tx, &request.organization_id, &request.comment_id)
            .await?
            .ok_or_else(|| runtime("load updated comment", "comment disappeared"))?,
    )?;
    complete_command(
        &mut tx,
        caller,
        actor,
        collaboration::UPDATE_COMMENT_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit update comment", error))?;
    Ok(response)
}

pub(crate) async fn delete_comment(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &collaboration::DeleteCommentRequest,
) -> Result<collaboration::DeleteCommentResponse, StorageError> {
    let row=sqlx::query("SELECT c.issue_id,i.project_id,c.author_subject FROM comments c JOIN issues i ON i.organization_id=c.organization_id AND i.issue_id=c.issue_id WHERE c.organization_id=$1 AND c.comment_id=$2").bind(&request.organization_id).bind(&request.comment_id).fetch_optional(postgres.pool()).await.map_err(|error|runtime("read comment for delete",error))?.ok_or(DomainFailure::NotFound)?;
    let issue_id: String = row
        .try_get("issue_id")
        .map_err(|error| runtime("decode comment issue", error))?;
    let project_id: String = row
        .try_get("project_id")
        .map_err(|error| runtime("decode comment project", error))?;
    let author: String = row
        .try_get("author_subject")
        .map_err(|error| runtime("decode comment author", error))?;
    if author != actor {
        return Err(DomainFailure::AuthorRequired.into());
    }
    match issue_visibility(postgres.pool(), &request.organization_id, &issue_id, actor).await? {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin delete comment", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        collaboration::DELETE_COMMENT_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit delete comment replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let revision:Option<i64>=sqlx::query_scalar("UPDATE comments SET body='',deleted=TRUE,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND comment_id=$2 AND revision=$3 RETURNING revision").bind(&request.organization_id).bind(&request.comment_id).bind(expected).fetch_optional(&mut *tx).await.map_err(|error|runtime("delete comment",error))?;
    let Some(revision) = revision else {
        return Err(DomainFailure::RevisionConflict.into());
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&issue_id),
        actor,
        collaboration::DELETE_COMMENT_OPERATION,
        "comment",
        &request.comment_id,
        Some(revision),
    )
    .await?;
    let response = replay(
        load_comment_value(&mut tx, &request.organization_id, &request.comment_id)
            .await?
            .ok_or_else(|| runtime("load deleted comment", "comment disappeared"))?,
    )?;
    complete_command(
        &mut tx,
        caller,
        actor,
        collaboration::DELETE_COMMENT_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit delete comment", error))?;
    Ok(response)
}

pub(crate) async fn list_comments(
    postgres: &OwnedPostgres,
    actor: &str,
    request: &collaboration::ListCommentsRequest,
) -> Result<collaboration::ListCommentsResponse, StorageError> {
    match issue_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.issue_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let after = parse_cursor(&request.after)?;
    let rows=sqlx::query("SELECT comment_id,row_seq FROM comments WHERE organization_id=$1 AND issue_id=$2 AND row_seq>$3 ORDER BY row_seq LIMIT $4").bind(&request.organization_id).bind(&request.issue_id).bind(after).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list comments",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire comment list reader", error))?;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let comment_id: String = row
            .try_get("comment_id")
            .map_err(|error| runtime("decode comment cursor", error))?;
        let row_seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode comment cursor", error))?;
        let value = load_comment_value(&mut connection, &request.organization_id, &comment_id)
            .await?
            .ok_or_else(|| runtime("load listed comment", "comment disappeared"))?;
        items.push(
            serde_json::from_value(value)
                .map_err(|error| runtime("decode comment list item", error))?,
        );
        next_cursor = Some(row_seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(collaboration::ListCommentsResponse { items, next_cursor })
}

fn health_name(value: &collaboration::Health) -> &'static str {
    match value {
        collaboration::Health::OnTrack => "on_track",
        collaboration::Health::AtRisk => "at_risk",
        collaboration::Health::OffTrack => "off_track",
    }
}
fn relation_kind_name(value: &collaboration::Kind) -> &'static str {
    match value {
        collaboration::Kind::Blocks => "blocks",
        collaboration::Kind::BlockedBy => "blocked_by",
        collaboration::Kind::Duplicate => "duplicate",
        collaboration::Kind::Related => "related",
    }
}

pub(crate) async fn create_project_update(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &collaboration::CreateProjectUpdateRequest,
) -> Result<collaboration::CreateProjectUpdateResponse, StorageError> {
    match project_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.project_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin project update", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        collaboration::CREATE_PROJECT_UPDATE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit project update replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let row=sqlx::query("INSERT INTO project_updates(update_id,organization_id,project_id,author_subject,body,health) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING created_at").bind(&request.update_id).bind(&request.organization_id).bind(&request.project_id).bind(actor).bind(request.body.trim()).bind(health_name(&request.health)).fetch_optional(&mut *tx).await.map_err(|error|runtime("insert project update",error))?;
    let Some(row) = row else {
        return Err(DomainFailure::IdentifierConflict.into());
    };
    let created_at: OffsetDateTime = row
        .try_get("created_at")
        .map_err(|error| runtime("decode project update", error))?;
    let response = collaboration::CreateProjectUpdateResponse {
        update_id: request.update_id.clone(),
        organization_id: request.organization_id.clone(),
        project_id: request.project_id.clone(),
        author_subject: actor.to_owned(),
        body: request.body.trim().to_owned(),
        health: request.health.clone(),
        created_at: format_time(created_at)?,
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&request.project_id),
        None,
        actor,
        collaboration::CREATE_PROJECT_UPDATE_OPERATION,
        "project_update",
        &request.update_id,
        None,
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        collaboration::CREATE_PROJECT_UPDATE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit project update", error))?;
    Ok(response)
}

pub(crate) async fn list_project_updates(
    postgres: &OwnedPostgres,
    actor: &str,
    request: &collaboration::ListProjectUpdatesRequest,
) -> Result<collaboration::ListProjectUpdatesResponse, StorageError> {
    match project_visibility(
        postgres.pool(),
        &request.organization_id,
        &request.project_id,
        actor,
    )
    .await?
    {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let after = parse_cursor(&request.after)?;
    let rows=sqlx::query("SELECT update_id,organization_id,project_id,author_subject,body,health,row_seq,created_at FROM project_updates WHERE organization_id=$1 AND project_id=$2 AND row_seq>$3 ORDER BY row_seq LIMIT $4").bind(&request.organization_id).bind(&request.project_id).bind(after).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list project updates",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let created_at: OffsetDateTime = row
            .try_get("created_at")
            .map_err(|error| runtime("decode project update", error))?;
        let row_seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode project update", error))?;
        let value = json!({"update_id":row.try_get::<String,_>("update_id").map_err(|error|runtime("decode project update",error))?,"organization_id":row.try_get::<String,_>("organization_id").map_err(|error|runtime("decode project update",error))?,"project_id":row.try_get::<String,_>("project_id").map_err(|error|runtime("decode project update",error))?,"author_subject":row.try_get::<String,_>("author_subject").map_err(|error|runtime("decode project update",error))?,"body":row.try_get::<String,_>("body").map_err(|error|runtime("decode project update",error))?,"health":row.try_get::<String,_>("health").map_err(|error|runtime("decode project update",error))?,"created_at":format_time(created_at)?});
        items.push(
            serde_json::from_value(value)
                .map_err(|error| runtime("decode project update item", error))?,
        );
        next_cursor = Some(row_seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(collaboration::ListProjectUpdatesResponse { items, next_cursor })
}

pub(crate) async fn add_issue_relation(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &collaboration::AddIssueRelationRequest,
) -> Result<collaboration::AddIssueRelationResponse, StorageError> {
    if request.issue_id == request.related_issue_id {
        return Err(DomainFailure::CannotRelateSelf.into());
    }
    for issue_id in [&request.issue_id, &request.related_issue_id] {
        match issue_visibility(postgres.pool(), &request.organization_id, issue_id, actor).await? {
            Some(true) => {}
            Some(false) => return Err(DomainFailure::PrivateTeam.into()),
            None => return Err(DomainFailure::NotFound.into()),
        }
    }
    let project_id: String = sqlx::query_scalar(
        "SELECT project_id FROM issues WHERE organization_id=$1 AND issue_id=$2",
    )
    .bind(&request.organization_id)
    .bind(&request.issue_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("read relation project", error))?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin add relation", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        collaboration::ADD_ISSUE_RELATION_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit add relation replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let inserted=sqlx::query("INSERT INTO issue_relations(relation_id,organization_id,issue_id,related_issue_id,kind) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING").bind(&request.relation_id).bind(&request.organization_id).bind(&request.issue_id).bind(&request.related_issue_id).bind(relation_kind_name(&request.kind)).execute(&mut *tx).await.map_err(|error|runtime("insert issue relation",error))?;
    if inserted.rows_affected() != 1 {
        return Err(DomainFailure::RelationConflict.into());
    }
    let response = collaboration::AddIssueRelationResponse {
        relation_id: request.relation_id.clone(),
        organization_id: request.organization_id.clone(),
        issue_id: request.issue_id.clone(),
        related_issue_id: request.related_issue_id.clone(),
        kind: request.kind.clone(),
        active: true,
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&request.issue_id),
        actor,
        collaboration::ADD_ISSUE_RELATION_OPERATION,
        "issue_relation",
        &request.relation_id,
        None,
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        collaboration::ADD_ISSUE_RELATION_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit add relation", error))?;
    Ok(response)
}

pub(crate) async fn remove_issue_relation(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &collaboration::RemoveIssueRelationRequest,
) -> Result<collaboration::RemoveIssueRelationResponse, StorageError> {
    let row=sqlx::query("SELECT r.issue_id,r.related_issue_id,r.kind,i.project_id FROM issue_relations r JOIN issues i ON i.organization_id=r.organization_id AND i.issue_id=r.issue_id WHERE r.organization_id=$1 AND r.relation_id=$2").bind(&request.organization_id).bind(&request.relation_id).fetch_optional(postgres.pool()).await.map_err(|error|runtime("read issue relation",error))?.ok_or(DomainFailure::NotFound)?;
    let issue_id: String = row
        .try_get("issue_id")
        .map_err(|error| runtime("decode issue relation", error))?;
    let related_issue_id: String = row
        .try_get("related_issue_id")
        .map_err(|error| runtime("decode issue relation", error))?;
    let kind: String = row
        .try_get("kind")
        .map_err(|error| runtime("decode issue relation", error))?;
    let project_id: String = row
        .try_get("project_id")
        .map_err(|error| runtime("decode issue relation", error))?;
    match issue_visibility(postgres.pool(), &request.organization_id, &issue_id, actor).await? {
        Some(true) => {}
        Some(false) => return Err(DomainFailure::PrivateTeam.into()),
        None => return Err(DomainFailure::NotFound.into()),
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin remove relation", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        collaboration::REMOVE_ISSUE_RELATION_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit remove relation replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    sqlx::query(
        "UPDATE issue_relations SET active=FALSE WHERE organization_id=$1 AND relation_id=$2",
    )
    .bind(&request.organization_id)
    .bind(&request.relation_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| runtime("remove issue relation", error))?;
    let value = json!({"relation_id":request.relation_id,"organization_id":request.organization_id,"issue_id":issue_id,"related_issue_id":related_issue_id,"kind":kind,"active":false});
    let response = replay(value)?;
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&project_id),
        Some(&issue_id),
        actor,
        collaboration::REMOVE_ISSUE_RELATION_OPERATION,
        "issue_relation",
        &request.relation_id,
        None,
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        collaboration::REMOVE_ISSUE_RELATION_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit remove relation", error))?;
    Ok(response)
}

async fn load_team_value(
    connection: &mut PgConnection,
    organization_id: &str,
    team_id: &str,
) -> Result<Option<Value>, StorageError> {
    let row=sqlx::query("SELECT team_id,organization_id,team_key,name,description,private,default_workflow_state_id,revision,created_at,updated_at FROM teams WHERE organization_id=$1 AND team_id=$2").bind(organization_id).bind(team_id).fetch_optional(&mut *connection).await.map_err(|error|runtime("load team",error))?;
    row.map(|row|{let created_at:OffsetDateTime=row.try_get("created_at").map_err(|error|runtime("decode team",error))?;let updated_at:OffsetDateTime=row.try_get("updated_at").map_err(|error|runtime("decode team",error))?;Ok(json!({"team_id":team_id,"organization_id":organization_id,"key":row.try_get::<String,_>("team_key").map_err(|error|runtime("decode team",error))?,"name":row.try_get::<String,_>("name").map_err(|error|runtime("decode team",error))?,"description":row.try_get::<Option<String>,_>("description").map_err(|error|runtime("decode team",error))?,"private":row.try_get::<bool,_>("private").map_err(|error|runtime("decode team",error))?,"default_workflow_state_id":row.try_get::<String,_>("default_workflow_state_id").map_err(|error|runtime("decode team",error))?,"revision":row.try_get::<i64,_>("revision").map_err(|error|runtime("decode team",error))?.to_string(),"created_at":format_time(created_at)?,"updated_at":format_time(updated_at)?}))}).transpose()
}

async fn insert_default_workflow_states(
    connection: &mut PgConnection,
    organization_id: &str,
    team_id: &str,
    default_state_id: &str,
) -> Result<(), StorageError> {
    let defaults = [
        (
            fresh_catalog_id("workflow_state"),
            "Backlog",
            "backlog",
            "#6B7280",
            0,
        ),
        (
            default_state_id.to_owned(),
            "Todo",
            "unstarted",
            "#9CA3AF",
            1,
        ),
        (
            fresh_catalog_id("workflow_state"),
            "In Progress",
            "started",
            "#3B82F6",
            2,
        ),
        (
            fresh_catalog_id("workflow_state"),
            "Done",
            "completed",
            "#10B981",
            3,
        ),
        (
            fresh_catalog_id("workflow_state"),
            "Canceled",
            "canceled",
            "#9CA3AF",
            4,
        ),
    ];
    for (state_id, name, category, color, position) in defaults {
        sqlx::query("INSERT INTO workflow_states(state_id,organization_id,team_id,name,category,color,position) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(state_id)
            .bind(organization_id)
            .bind(team_id)
            .bind(name)
            .bind(category)
            .bind(color)
            .bind(position)
            .execute(&mut *connection)
            .await
            .map_err(|error| runtime("initialize team workflow", error))?;
    }
    Ok(())
}

pub(crate) async fn put_team(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::PutTeamRequest,
) -> Result<admin::PutTeamResponse, StorageError> {
    let key_owner: Option<String> =
        sqlx::query_scalar("SELECT team_id FROM teams WHERE organization_id=$1 AND team_key=$2")
            .bind(&request.organization_id)
            .bind(&request.key)
            .fetch_optional(postgres.pool())
            .await
            .map_err(|error| runtime("validate team key", error))?;
    if key_owner.as_deref().is_some_and(|id| id != request.team_id) {
        return Err(DomainFailure::KeyConflict.into());
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put team", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_TEAM_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit put team replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    ensure_project_status_catalog(&mut tx, &request.organization_id).await?;
    let revision: i64 = if let Some(expected) = &request.expected_revision {
        let state_id = request
            .default_workflow_state_id
            .as_deref()
            .ok_or(DomainFailure::DefaultStateInvalid)?;
        let valid: Option<String> = sqlx::query_scalar("SELECT state_id FROM workflow_states WHERE organization_id=$1 AND team_id=$2 AND state_id=$3 AND category='unstarted' AND NOT archived FOR KEY SHARE")
            .bind(&request.organization_id)
            .bind(&request.team_id)
            .bind(state_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| runtime("validate default workflow", error))?;
        if valid.is_none() {
            return Err(DomainFailure::DefaultStateInvalid.into());
        }
        let expected = parse_revision(expected)?;
        sqlx::query_scalar("UPDATE teams SET team_key=$4,name=$5,description=$6,private=$7,default_workflow_state_id=$8,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND team_id=$2 AND revision=$3 RETURNING revision").bind(&request.organization_id).bind(&request.team_id).bind(expected).bind(&request.key).bind(request.name.trim()).bind(request.description.as_deref().map(str::trim)).bind(request.private).bind(state_id).fetch_optional(&mut *tx).await.map_err(|error|runtime("update team",error))?.ok_or(DomainFailure::RevisionConflict)?
    } else {
        if request.default_workflow_state_id.is_some() {
            return Err(DomainFailure::DefaultStateInvalid.into());
        }
        let default_state_id = fresh_catalog_id("workflow_state");
        let inserted:Option<i64>=sqlx::query_scalar("INSERT INTO teams(team_id,organization_id,team_key,name,description,private,default_workflow_state_id) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING revision").bind(&request.team_id).bind(&request.organization_id).bind(&request.key).bind(request.name.trim()).bind(request.description.as_deref().map(str::trim)).bind(request.private).bind(&default_state_id).fetch_optional(&mut *tx).await.map_err(|error|runtime("insert team",error))?;
        let revision = inserted.ok_or(DomainFailure::KeyConflict)?;
        insert_default_workflow_states(
            &mut tx,
            &request.organization_id,
            &request.team_id,
            &default_state_id,
        )
        .await?;
        revision
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::PUT_TEAM_OPERATION,
        "team",
        &request.team_id,
        Some(revision),
    )
    .await?;
    let response = replay(
        load_team_value(&mut tx, &request.organization_id, &request.team_id)
            .await?
            .ok_or_else(|| runtime("load put team", "team disappeared"))?,
    )?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_TEAM_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit put team", error))?;
    Ok(response)
}

pub(crate) async fn list_teams(
    postgres: &OwnedPostgres,
    request: &admin::ListTeamsRequest,
) -> Result<admin::ListTeamsResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let rows=sqlx::query("SELECT team_id,row_seq FROM teams WHERE organization_id=$1 AND row_seq>$2 ORDER BY row_seq LIMIT $3").bind(&request.organization_id).bind(after).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list teams",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire team list reader", error))?;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let id: String = row
            .try_get("team_id")
            .map_err(|error| runtime("decode team cursor", error))?;
        let seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode team cursor", error))?;
        items.push(
            serde_json::from_value(
                load_team_value(&mut connection, &request.organization_id, &id)
                    .await?
                    .ok_or_else(|| runtime("load listed team", "team disappeared"))?,
            )
            .map_err(|error| runtime("decode team list item", error))?,
        );
        next_cursor = Some(seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(admin::ListTeamsResponse { items, next_cursor })
}

pub(crate) async fn set_team_member(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::SetTeamMemberRequest,
) -> Result<admin::SetTeamMemberResponse, StorageError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM teams WHERE organization_id=$1 AND team_id=$2)",
    )
    .bind(&request.organization_id)
    .bind(&request.team_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("validate team membership", error))?;
    if !exists {
        return Err(DomainFailure::NotFound.into());
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin set team member", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::SET_TEAM_MEMBER_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit team member replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let prior:Option<(bool,i64)>=sqlx::query_as("SELECT active,revision FROM team_members WHERE organization_id=$1 AND team_id=$2 AND subject=$3 FOR UPDATE").bind(&request.organization_id).bind(&request.team_id).bind(&request.subject).fetch_optional(&mut *tx).await.map_err(|error|runtime("read team member",error))?;
    let (changed, revision) = match prior {
        Some((active, revision)) if active == request.active => (false, revision),
        Some((_active, _)) => {
            let revision:i64=sqlx::query_scalar("UPDATE team_members SET active=$4,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND team_id=$2 AND subject=$3 RETURNING revision").bind(&request.organization_id).bind(&request.team_id).bind(&request.subject).bind(request.active).fetch_one(&mut *tx).await.map_err(|error|runtime("update team member",error))?;
            (true, revision)
        }
        None => {
            sqlx::query("INSERT INTO team_members(organization_id,team_id,subject,active) VALUES($1,$2,$3,$4)").bind(&request.organization_id).bind(&request.team_id).bind(&request.subject).bind(request.active).execute(&mut *tx).await.map_err(|error|runtime("insert team member",error))?;
            (true, 1)
        }
    };
    let response = admin::SetTeamMemberResponse {
        team_id: request.team_id.clone(),
        subject: request.subject.clone(),
        active: request.active,
        changed,
        revision: revision.to_string(),
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::SET_TEAM_MEMBER_OPERATION,
        "team_member",
        &request.subject,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::SET_TEAM_MEMBER_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit team member", error))?;
    Ok(response)
}

fn workflow_category(value: &admin::WorkflowCategory) -> &'static str {
    match value {
        admin::WorkflowCategory::Backlog => "backlog",
        admin::WorkflowCategory::Unstarted => "unstarted",
        admin::WorkflowCategory::Started => "started",
        admin::WorkflowCategory::Completed => "completed",
        admin::WorkflowCategory::Canceled => "canceled",
    }
}
fn project_status_category(value: &admin::ProjectStatusCategory) -> &'static str {
    match value {
        admin::ProjectStatusCategory::Backlog => "backlog",
        admin::ProjectStatusCategory::Planned => "planned",
        admin::ProjectStatusCategory::Started => "started",
        admin::ProjectStatusCategory::Paused => "paused",
        admin::ProjectStatusCategory::Completed => "completed",
        admin::ProjectStatusCategory::Canceled => "canceled",
    }
}

async fn load_workflow_state_value(
    connection: &mut PgConnection,
    organization_id: &str,
    state_id: &str,
) -> Result<Option<Value>, StorageError> {
    let row = sqlx::query("SELECT state_id,organization_id,team_id,name,category,color,position,archived,archived_at,revision FROM workflow_states WHERE organization_id=$1 AND state_id=$2")
        .bind(organization_id)
        .bind(state_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| runtime("load workflow state", error))?;
    row.map(|row| {
        let archived_at: Option<OffsetDateTime> = row
            .try_get("archived_at")
            .map_err(|error| runtime("decode workflow state", error))?;
        Ok(json!({
            "state_id": row.try_get::<String,_>("state_id").map_err(|error| runtime("decode workflow state", error))?,
            "organization_id": row.try_get::<String,_>("organization_id").map_err(|error| runtime("decode workflow state", error))?,
            "team_id": row.try_get::<String,_>("team_id").map_err(|error| runtime("decode workflow state", error))?,
            "name": row.try_get::<String,_>("name").map_err(|error| runtime("decode workflow state", error))?,
            "category": row.try_get::<String,_>("category").map_err(|error| runtime("decode workflow state", error))?,
            "color": row.try_get::<String,_>("color").map_err(|error| runtime("decode workflow state", error))?,
            "position": row.try_get::<i32,_>("position").map_err(|error| runtime("decode workflow state", error))?,
            "archived": row.try_get::<bool,_>("archived").map_err(|error| runtime("decode workflow state", error))?,
            "archived_at": archived_at.map(format_time).transpose()?,
            "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode workflow state", error))?.to_string()
        }))
    })
    .transpose()
}

async fn load_project_status_value(
    connection: &mut PgConnection,
    organization_id: &str,
    status_id: &str,
) -> Result<Option<Value>, StorageError> {
    let row = sqlx::query("SELECT status_id,organization_id,name,category,color,position,is_default,archived,archived_at,revision FROM project_statuses WHERE organization_id=$1 AND status_id=$2")
        .bind(organization_id)
        .bind(status_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|error| runtime("load project status", error))?;
    row.map(|row| {
        let archived_at: Option<OffsetDateTime> = row
            .try_get("archived_at")
            .map_err(|error| runtime("decode project status", error))?;
        Ok(json!({
            "status_id": row.try_get::<String,_>("status_id").map_err(|error| runtime("decode project status", error))?,
            "organization_id": row.try_get::<String,_>("organization_id").map_err(|error| runtime("decode project status", error))?,
            "name": row.try_get::<String,_>("name").map_err(|error| runtime("decode project status", error))?,
            "category": row.try_get::<String,_>("category").map_err(|error| runtime("decode project status", error))?,
            "color": row.try_get::<String,_>("color").map_err(|error| runtime("decode project status", error))?,
            "position": row.try_get::<i32,_>("position").map_err(|error| runtime("decode project status", error))?,
            "is_default": row.try_get::<bool,_>("is_default").map_err(|error| runtime("decode project status", error))?,
            "archived": row.try_get::<bool,_>("archived").map_err(|error| runtime("decode project status", error))?,
            "archived_at": archived_at.map(format_time).transpose()?,
            "revision": row.try_get::<i64,_>("revision").map_err(|error| runtime("decode project status", error))?.to_string()
        }))
    })
    .transpose()
}

pub(crate) async fn put_workflow_state(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::PutWorkflowStateRequest,
) -> Result<admin::PutWorkflowStateResponse, StorageError> {
    let team: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM teams WHERE organization_id=$1 AND team_id=$2)",
    )
    .bind(&request.organization_id)
    .bind(&request.team_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("validate workflow team", error))?;
    if !team {
        return Err(DomainFailure::NotFound.into());
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put workflow", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_WORKFLOW_STATE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit workflow replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let revision: i64 = if let Some(expected) = &request.expected_revision {
        let existing_category: String = sqlx::query_scalar("SELECT category FROM workflow_states WHERE organization_id=$1 AND team_id=$2 AND state_id=$3 FOR UPDATE")
            .bind(&request.organization_id)
            .bind(&request.team_id)
            .bind(&request.state_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| runtime("read workflow category", error))?
            .ok_or(DomainFailure::NotFound)?;
        if existing_category != workflow_category(&request.category) {
            return Err(DomainFailure::InvalidRequest.into());
        }
        sqlx::query_scalar("UPDATE workflow_states SET name=$5,category=$6,color=$7,position=$8,revision=revision+1 WHERE organization_id=$1 AND team_id=$2 AND state_id=$3 AND revision=$4 RETURNING revision").bind(&request.organization_id).bind(&request.team_id).bind(&request.state_id).bind(parse_revision(expected)?).bind(request.name.trim()).bind(workflow_category(&request.category)).bind(&request.color).bind(request.position).fetch_optional(&mut *tx).await.map_err(|error|runtime("update workflow",error))?.ok_or(DomainFailure::RevisionConflict)?
    } else {
        sqlx::query_scalar("INSERT INTO workflow_states(state_id,organization_id,team_id,name,category,color,position) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING revision").bind(&request.state_id).bind(&request.organization_id).bind(&request.team_id).bind(request.name.trim()).bind(workflow_category(&request.category)).bind(&request.color).bind(request.position).fetch_optional(&mut *tx).await.map_err(|error|runtime("insert workflow",error))?.ok_or(DomainFailure::KeyConflict)?
    };
    let response = replay(
        load_workflow_state_value(&mut tx, &request.organization_id, &request.state_id)
            .await?
            .ok_or_else(|| runtime("load put workflow state", "workflow state disappeared"))?,
    )?;
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::PUT_WORKFLOW_STATE_OPERATION,
        "workflow_state",
        &request.state_id,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_WORKFLOW_STATE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit workflow", error))?;
    Ok(response)
}

pub(crate) async fn get_workflow_state(
    postgres: &OwnedPostgres,
    request: &admin::GetWorkflowStateRequest,
) -> Result<admin::GetWorkflowStateResponse, StorageError> {
    let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_states WHERE organization_id=$1 AND team_id=$2 AND state_id=$3)")
        .bind(&request.organization_id)
        .bind(&request.team_id)
        .bind(&request.state_id)
        .fetch_one(postgres.pool())
        .await
        .map_err(|error| runtime("validate workflow ownership", error))?;
    if !owned {
        return Err(DomainFailure::NotFound.into());
    }
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire workflow reader", error))?;
    replay(
        load_workflow_state_value(&mut connection, &request.organization_id, &request.state_id)
            .await?
            .ok_or(DomainFailure::NotFound)?,
    )
}

pub(crate) async fn reorder_workflow_states(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::ReorderWorkflowStatesRequest,
) -> Result<admin::ReorderWorkflowStatesResponse, StorageError> {
    if !(1..=100).contains(&request.items.len())
        || request
            .items
            .iter()
            .map(|item| &item.state_id)
            .collect::<BTreeSet<_>>()
            .len()
            != request.items.len()
        || request
            .items
            .iter()
            .map(|item| item.position)
            .collect::<BTreeSet<_>>()
            .len()
            != request.items.len()
        || request
            .items
            .iter()
            .any(|item| !(0..=i64::from(i32::MAX)).contains(&item.position))
    {
        return Err(DomainFailure::InvalidRequest.into());
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin reorder workflow states", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::REORDER_WORKFLOW_STATES_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit workflow reorder replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let current = sqlx::query("SELECT state_id,revision FROM workflow_states WHERE organization_id=$1 AND team_id=$2 AND NOT archived ORDER BY state_id FOR UPDATE")
        .bind(&request.organization_id)
        .bind(&request.team_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| runtime("lock workflow reorder", error))?;
    if current.len() != request.items.len() {
        return Err(DomainFailure::InvalidRequest.into());
    }
    for row in current {
        let state_id: String = row
            .try_get("state_id")
            .map_err(|error| runtime("decode workflow reorder", error))?;
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| runtime("decode workflow reorder", error))?;
        let item = request
            .items
            .iter()
            .find(|item| item.state_id == state_id)
            .ok_or(DomainFailure::InvalidRequest)?;
        if parse_revision(&item.expected_revision)? != revision {
            return Err(DomainFailure::RevisionConflict.into());
        }
    }
    for item in &request.items {
        let updated: Option<i64> = sqlx::query_scalar("UPDATE workflow_states SET position=$5,revision=revision+1 WHERE organization_id=$1 AND team_id=$2 AND state_id=$3 AND revision=$4 AND NOT archived RETURNING revision")
            .bind(&request.organization_id)
            .bind(&request.team_id)
            .bind(&item.state_id)
            .bind(parse_revision(&item.expected_revision)?)
            .bind(item.position)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| runtime("reorder workflow state", error))?;
        if updated.is_none() {
            return Err(DomainFailure::RevisionConflict.into());
        }
    }
    let mut values = Vec::with_capacity(request.items.len());
    let mut ordered = request.items.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|item| item.position);
    for item in ordered {
        values.push(
            load_workflow_state_value(&mut tx, &request.organization_id, &item.state_id)
                .await?
                .ok_or_else(|| runtime("load reordered workflow", "workflow state disappeared"))?,
        );
    }
    let response = replay(json!({"items": values}))?;
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::REORDER_WORKFLOW_STATES_OPERATION,
        "workflow_catalog",
        &request.team_id,
        None,
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::REORDER_WORKFLOW_STATES_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit workflow reorder", error))?;
    Ok(response)
}

pub(crate) async fn list_workflow_states(
    postgres: &OwnedPostgres,
    request: &admin::ListWorkflowStatesRequest,
) -> Result<admin::ListWorkflowStatesResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let rows=sqlx::query("SELECT state_id,row_seq FROM workflow_states WHERE organization_id=$1 AND team_id=$2 AND row_seq>$3 ORDER BY row_seq LIMIT $4").bind(&request.organization_id).bind(&request.team_id).bind(after).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list workflow states",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire workflow list reader", error))?;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode workflow state", error))?;
        let state_id: String = row
            .try_get("state_id")
            .map_err(|error| runtime("decode workflow state", error))?;
        let value = load_workflow_state_value(&mut connection, &request.organization_id, &state_id)
            .await?
            .ok_or_else(|| runtime("load listed workflow state", "workflow state disappeared"))?;
        items.push(
            serde_json::from_value(value)
                .map_err(|error| runtime("decode workflow item", error))?,
        );
        next_cursor = Some(seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(admin::ListWorkflowStatesResponse { items, next_cursor })
}

pub(crate) async fn archive_workflow_state(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::ArchiveWorkflowStateRequest,
) -> Result<admin::ArchiveWorkflowStateResponse, StorageError> {
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin archive workflow state", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::ARCHIVE_WORKFLOW_STATE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit archive workflow replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let team_id: String = sqlx::query_scalar(
        "SELECT team_id FROM workflow_states WHERE organization_id=$1 AND state_id=$2 FOR UPDATE",
    )
    .bind(&request.organization_id)
    .bind(&request.state_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| runtime("lock workflow state", error))?
    .ok_or(DomainFailure::NotFound)?;
    if request.archived {
        let is_default: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM teams WHERE organization_id=$1 AND team_id=$2 AND default_workflow_state_id=$3)")
            .bind(&request.organization_id)
            .bind(&team_id)
            .bind(&request.state_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| runtime("check default workflow state", error))?;
        if is_default {
            return Err(DomainFailure::DefaultStateInvalid.into());
        }
        let referenced: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM issues WHERE organization_id=$1 AND workflow_state_id=$2)",
        )
        .bind(&request.organization_id)
        .bind(&request.state_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| runtime("check workflow references", error))?;
        if referenced {
            return Err(DomainFailure::ActiveReference.into());
        }
    }
    let revision: i64 = sqlx::query_scalar("UPDATE workflow_states SET archived=$4,archived_at=CASE WHEN $4 THEN COALESCE(archived_at,transaction_timestamp()) ELSE NULL END,revision=revision+1 WHERE organization_id=$1 AND state_id=$2 AND revision=$3 RETURNING revision")
        .bind(&request.organization_id)
        .bind(&request.state_id)
        .bind(expected)
        .bind(request.archived)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| runtime("archive workflow state", error))?
        .ok_or(DomainFailure::RevisionConflict)?;
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::ARCHIVE_WORKFLOW_STATE_OPERATION,
        "workflow_state",
        &request.state_id,
        Some(revision),
    )
    .await?;
    let response = replay(
        load_workflow_state_value(&mut tx, &request.organization_id, &request.state_id)
            .await?
            .ok_or_else(|| runtime("load archived workflow", "workflow state disappeared"))?,
    )?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::ARCHIVE_WORKFLOW_STATE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit archive workflow state", error))?;
    Ok(response)
}

pub(crate) async fn delete_workflow_state(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::DeleteWorkflowStateRequest,
) -> Result<admin::DeleteWorkflowStateResponse, StorageError> {
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin delete workflow state", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::DELETE_WORKFLOW_STATE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit workflow delete replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let row = sqlx::query("SELECT archived,revision FROM workflow_states WHERE organization_id=$1 AND team_id=$2 AND state_id=$3 FOR UPDATE")
        .bind(&request.organization_id)
        .bind(&request.team_id)
        .bind(&request.state_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| runtime("lock workflow delete", error))?
        .ok_or(DomainFailure::NotFound)?;
    let archived: bool = row
        .try_get("archived")
        .map_err(|error| runtime("decode workflow delete", error))?;
    let revision: i64 = row
        .try_get("revision")
        .map_err(|error| runtime("decode workflow delete", error))?;
    if revision != expected {
        return Err(DomainFailure::RevisionConflict.into());
    }
    let is_default: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM teams WHERE organization_id=$1 AND team_id=$2 AND default_workflow_state_id=$3)")
        .bind(&request.organization_id)
        .bind(&request.team_id)
        .bind(&request.state_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| runtime("protect default workflow delete", error))?;
    if is_default {
        return Err(DomainFailure::DefaultStateInvalid.into());
    }
    let referenced: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM issues WHERE organization_id=$1 AND team_id=$2 AND workflow_state_id=$3)")
        .bind(&request.organization_id)
        .bind(&request.team_id)
        .bind(&request.state_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| runtime("protect referenced workflow delete", error))?;
    if referenced {
        return Err(DomainFailure::ActiveReference.into());
    }
    if !archived {
        return Err(DomainFailure::InvalidRequest.into());
    }
    let deleted = sqlx::query("DELETE FROM workflow_states WHERE organization_id=$1 AND team_id=$2 AND state_id=$3 AND revision=$4")
        .bind(&request.organization_id)
        .bind(&request.team_id)
        .bind(&request.state_id)
        .bind(expected)
        .execute(&mut *tx)
        .await
        .map_err(|error| runtime("delete workflow state", error))?;
    if deleted.rows_affected() != 1 {
        return Err(DomainFailure::RevisionConflict.into());
    }
    let response = admin::DeleteWorkflowStateResponse {
        organization_id: request.organization_id.clone(),
        team_id: request.team_id.clone(),
        state_id: request.state_id.clone(),
        deleted: true,
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::DELETE_WORKFLOW_STATE_OPERATION,
        "workflow_state",
        &request.state_id,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::DELETE_WORKFLOW_STATE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit workflow delete", error))?;
    Ok(response)
}

pub(crate) async fn put_project_status(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::PutProjectStatusRequest,
) -> Result<admin::PutProjectStatusResponse, StorageError> {
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put project status", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_PROJECT_STATUS_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit project status replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    ensure_project_status_catalog(&mut tx, &request.organization_id).await?;
    let revision: i64 = if let Some(expected) = &request.expected_revision {
        let existing_category: String = sqlx::query_scalar("SELECT category FROM project_statuses WHERE organization_id=$1 AND status_id=$2 FOR UPDATE")
            .bind(&request.organization_id)
            .bind(&request.status_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| runtime("read project status category", error))?
            .ok_or(DomainFailure::NotFound)?;
        if existing_category != project_status_category(&request.category) {
            return Err(DomainFailure::InvalidRequest.into());
        }
        sqlx::query_scalar("UPDATE project_statuses SET name=$4,category=$5,color=$6,position=$7,revision=revision+1 WHERE organization_id=$1 AND status_id=$2 AND revision=$3 RETURNING revision").bind(&request.organization_id).bind(&request.status_id).bind(parse_revision(expected)?).bind(request.name.trim()).bind(project_status_category(&request.category)).bind(&request.color).bind(request.position).fetch_optional(&mut *tx).await.map_err(|error|runtime("update project status",error))?.ok_or(DomainFailure::RevisionConflict)?
    } else {
        sqlx::query_scalar("INSERT INTO project_statuses(status_id,organization_id,name,category,color,position) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING revision").bind(&request.status_id).bind(&request.organization_id).bind(request.name.trim()).bind(project_status_category(&request.category)).bind(&request.color).bind(request.position).fetch_optional(&mut *tx).await.map_err(|error|runtime("insert project status",error))?.ok_or(DomainFailure::KeyConflict)?
    };
    let response = replay(
        load_project_status_value(&mut tx, &request.organization_id, &request.status_id)
            .await?
            .ok_or_else(|| runtime("load put project status", "project status disappeared"))?,
    )?;
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::PUT_PROJECT_STATUS_OPERATION,
        "project_status",
        &request.status_id,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_PROJECT_STATUS_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit project status", error))?;
    Ok(response)
}

pub(crate) async fn get_project_status(
    postgres: &OwnedPostgres,
    request: &admin::GetProjectStatusRequest,
) -> Result<admin::GetProjectStatusResponse, StorageError> {
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin get project status", error))?;
    ensure_project_status_catalog(&mut tx, &request.organization_id).await?;
    let response = replay(
        load_project_status_value(&mut tx, &request.organization_id, &request.status_id)
            .await?
            .ok_or(DomainFailure::NotFound)?,
    )?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit get project status", error))?;
    Ok(response)
}

pub(crate) async fn reorder_project_statuses(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::ReorderProjectStatusesRequest,
) -> Result<admin::ReorderProjectStatusesResponse, StorageError> {
    if !(1..=100).contains(&request.items.len())
        || request
            .items
            .iter()
            .map(|item| &item.status_id)
            .collect::<BTreeSet<_>>()
            .len()
            != request.items.len()
        || request
            .items
            .iter()
            .map(|item| item.position)
            .collect::<BTreeSet<_>>()
            .len()
            != request.items.len()
        || request
            .items
            .iter()
            .any(|item| !(0..=i64::from(i32::MAX)).contains(&item.position))
    {
        return Err(DomainFailure::InvalidRequest.into());
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin reorder project statuses", error))?;
    ensure_project_status_catalog(&mut tx, &request.organization_id).await?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::REORDER_PROJECT_STATUSES_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit project status reorder replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let current = sqlx::query("SELECT status_id,revision FROM project_statuses WHERE organization_id=$1 AND NOT archived ORDER BY status_id FOR UPDATE")
        .bind(&request.organization_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| runtime("lock project status reorder", error))?;
    if current.len() != request.items.len() {
        return Err(DomainFailure::InvalidRequest.into());
    }
    for row in current {
        let status_id: String = row
            .try_get("status_id")
            .map_err(|error| runtime("decode project status reorder", error))?;
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| runtime("decode project status reorder", error))?;
        let item = request
            .items
            .iter()
            .find(|item| item.status_id == status_id)
            .ok_or(DomainFailure::InvalidRequest)?;
        if parse_revision(&item.expected_revision)? != revision {
            return Err(DomainFailure::RevisionConflict.into());
        }
    }
    for item in &request.items {
        let updated: Option<i64> = sqlx::query_scalar("UPDATE project_statuses SET position=$4,revision=revision+1 WHERE organization_id=$1 AND status_id=$2 AND revision=$3 AND NOT archived RETURNING revision")
            .bind(&request.organization_id)
            .bind(&item.status_id)
            .bind(parse_revision(&item.expected_revision)?)
            .bind(item.position)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| runtime("reorder project status", error))?;
        if updated.is_none() {
            return Err(DomainFailure::RevisionConflict.into());
        }
    }
    let mut values = Vec::with_capacity(request.items.len());
    let mut ordered = request.items.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|item| item.position);
    for item in ordered {
        values.push(
            load_project_status_value(&mut tx, &request.organization_id, &item.status_id)
                .await?
                .ok_or_else(|| runtime("load reordered project status", "status disappeared"))?,
        );
    }
    let response = replay(json!({"items": values}))?;
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::REORDER_PROJECT_STATUSES_OPERATION,
        "project_status_catalog",
        &request.organization_id,
        None,
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::REORDER_PROJECT_STATUSES_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit project status reorder", error))?;
    Ok(response)
}

pub(crate) async fn list_project_statuses(
    postgres: &OwnedPostgres,
    request: &admin::ListProjectStatusesRequest,
) -> Result<admin::ListProjectStatusesResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let mut bootstrap = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin project status bootstrap", error))?;
    ensure_project_status_catalog(&mut bootstrap, &request.organization_id).await?;
    bootstrap
        .commit()
        .await
        .map_err(|error| runtime("commit project status bootstrap", error))?;
    let rows=sqlx::query("SELECT status_id,row_seq FROM project_statuses WHERE organization_id=$1 AND row_seq>$2 ORDER BY row_seq LIMIT $3").bind(&request.organization_id).bind(after).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list project statuses",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut connection = postgres
        .pool()
        .acquire()
        .await
        .map_err(|error| runtime("acquire project status list reader", error))?;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode project status", error))?;
        let status_id: String = row
            .try_get("status_id")
            .map_err(|error| runtime("decode project status", error))?;
        let value =
            load_project_status_value(&mut connection, &request.organization_id, &status_id)
                .await?
                .ok_or_else(|| {
                    runtime("load listed project status", "project status disappeared")
                })?;
        items.push(
            serde_json::from_value(value)
                .map_err(|error| runtime("decode project status item", error))?,
        );
        next_cursor = Some(seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(admin::ListProjectStatusesResponse { items, next_cursor })
}

pub(crate) async fn archive_project_status(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::ArchiveProjectStatusRequest,
) -> Result<admin::ArchiveProjectStatusResponse, StorageError> {
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin archive project status", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::ARCHIVE_PROJECT_STATUS_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit archive project status replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    ensure_project_status_catalog(&mut tx, &request.organization_id).await?;
    let is_default: bool = sqlx::query_scalar("SELECT is_default FROM project_statuses WHERE organization_id=$1 AND status_id=$2 FOR UPDATE")
        .bind(&request.organization_id)
        .bind(&request.status_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| runtime("lock project status", error))?
        .ok_or(DomainFailure::NotFound)?;
    if request.archived {
        if is_default {
            return Err(DomainFailure::DefaultStateInvalid.into());
        }
        let referenced: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE organization_id=$1 AND status_id=$2)",
        )
        .bind(&request.organization_id)
        .bind(&request.status_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| runtime("check project status references", error))?;
        if referenced {
            return Err(DomainFailure::ActiveReference.into());
        }
    }
    let revision: i64 = sqlx::query_scalar("UPDATE project_statuses SET archived=$4,archived_at=CASE WHEN $4 THEN COALESCE(archived_at,transaction_timestamp()) ELSE NULL END,revision=revision+1 WHERE organization_id=$1 AND status_id=$2 AND revision=$3 RETURNING revision")
        .bind(&request.organization_id)
        .bind(&request.status_id)
        .bind(expected)
        .bind(request.archived)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| runtime("archive project status", error))?
        .ok_or(DomainFailure::RevisionConflict)?;
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::ARCHIVE_PROJECT_STATUS_OPERATION,
        "project_status",
        &request.status_id,
        Some(revision),
    )
    .await?;
    let response = replay(
        load_project_status_value(&mut tx, &request.organization_id, &request.status_id)
            .await?
            .ok_or_else(|| runtime("load archived project status", "project status disappeared"))?,
    )?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::ARCHIVE_PROJECT_STATUS_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit archive project status", error))?;
    Ok(response)
}

pub(crate) async fn delete_project_status(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::DeleteProjectStatusRequest,
) -> Result<admin::DeleteProjectStatusResponse, StorageError> {
    let expected = parse_revision(&request.expected_revision)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin delete project status", error))?;
    ensure_project_status_catalog(&mut tx, &request.organization_id).await?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::DELETE_PROJECT_STATUS_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit project status delete replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let row = sqlx::query("SELECT is_default,archived,revision FROM project_statuses WHERE organization_id=$1 AND status_id=$2 FOR UPDATE")
        .bind(&request.organization_id)
        .bind(&request.status_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| runtime("lock project status delete", error))?
        .ok_or(DomainFailure::NotFound)?;
    let is_default: bool = row
        .try_get("is_default")
        .map_err(|error| runtime("decode project status delete", error))?;
    let archived: bool = row
        .try_get("archived")
        .map_err(|error| runtime("decode project status delete", error))?;
    let revision: i64 = row
        .try_get("revision")
        .map_err(|error| runtime("decode project status delete", error))?;
    if revision != expected {
        return Err(DomainFailure::RevisionConflict.into());
    }
    if is_default {
        return Err(DomainFailure::DefaultStateInvalid.into());
    }
    let referenced: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE organization_id=$1 AND status_id=$2)",
    )
    .bind(&request.organization_id)
    .bind(&request.status_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|error| runtime("protect referenced project status delete", error))?;
    if referenced {
        return Err(DomainFailure::ActiveReference.into());
    }
    if !archived {
        return Err(DomainFailure::InvalidRequest.into());
    }
    let deleted = sqlx::query(
        "DELETE FROM project_statuses WHERE organization_id=$1 AND status_id=$2 AND revision=$3",
    )
    .bind(&request.organization_id)
    .bind(&request.status_id)
    .bind(expected)
    .execute(&mut *tx)
    .await
    .map_err(|error| runtime("delete project status", error))?;
    if deleted.rows_affected() != 1 {
        return Err(DomainFailure::RevisionConflict.into());
    }
    let response = admin::DeleteProjectStatusResponse {
        organization_id: request.organization_id.clone(),
        status_id: request.status_id.clone(),
        deleted: true,
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::DELETE_PROJECT_STATUS_OPERATION,
        "project_status",
        &request.status_id,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::DELETE_PROJECT_STATUS_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit project status delete", error))?;
    Ok(response)
}

pub(crate) async fn put_label(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::PutLabelRequest,
) -> Result<admin::PutLabelResponse, StorageError> {
    if let Some(team_id) = &request.team_id {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM teams WHERE organization_id=$1 AND team_id=$2)",
        )
        .bind(&request.organization_id)
        .bind(team_id)
        .fetch_one(postgres.pool())
        .await
        .map_err(|error| runtime("validate label team", error))?;
        if !exists {
            return Err(DomainFailure::NotFound.into());
        }
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put label", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_LABEL_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit label replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let revision: i64 = if let Some(expected) = &request.expected_revision {
        sqlx::query_scalar("UPDATE labels SET team_id=$4,name=$5,description=$6,color=$7,revision=revision+1 WHERE organization_id=$1 AND label_id=$2 AND revision=$3 RETURNING revision").bind(&request.organization_id).bind(&request.label_id).bind(parse_revision(expected)?).bind(&request.team_id).bind(request.name.trim()).bind(request.description.as_deref().map(str::trim)).bind(&request.color).fetch_optional(&mut *tx).await.map_err(|error|runtime("update label",error))?.ok_or(DomainFailure::RevisionConflict)?
    } else {
        sqlx::query_scalar("INSERT INTO labels(label_id,organization_id,team_id,name,description,color) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING revision").bind(&request.label_id).bind(&request.organization_id).bind(&request.team_id).bind(request.name.trim()).bind(request.description.as_deref().map(str::trim)).bind(&request.color).fetch_optional(&mut *tx).await.map_err(|error|runtime("insert label",error))?.ok_or(DomainFailure::KeyConflict)?
    };
    let response = admin::PutLabelResponse {
        label_id: request.label_id.clone(),
        organization_id: request.organization_id.clone(),
        team_id: request.team_id.clone(),
        name: request.name.trim().to_owned(),
        description: request
            .description
            .as_deref()
            .map(str::trim)
            .map(ToOwned::to_owned),
        color: request.color.clone(),
        revision: revision.to_string(),
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::PUT_LABEL_OPERATION,
        "label",
        &request.label_id,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_LABEL_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit label", error))?;
    Ok(response)
}

pub(crate) async fn list_labels(
    postgres: &OwnedPostgres,
    request: &admin::ListLabelsRequest,
) -> Result<admin::ListLabelsResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let rows=sqlx::query("SELECT label_id,organization_id,team_id,name,description,color,revision,row_seq FROM labels WHERE organization_id=$1 AND row_seq>$2 AND ($3::text IS NULL OR team_id IS NULL OR team_id=$3) ORDER BY row_seq LIMIT $4").bind(&request.organization_id).bind(after).bind(&request.team_id).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list labels",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode label", error))?;
        let value = json!({"label_id":row.try_get::<String,_>("label_id").map_err(|error|runtime("decode label",error))?,"organization_id":row.try_get::<String,_>("organization_id").map_err(|error|runtime("decode label",error))?,"team_id":row.try_get::<Option<String>,_>("team_id").map_err(|error|runtime("decode label",error))?,"name":row.try_get::<String,_>("name").map_err(|error|runtime("decode label",error))?,"description":row.try_get::<Option<String>,_>("description").map_err(|error|runtime("decode label",error))?,"color":row.try_get::<String,_>("color").map_err(|error|runtime("decode label",error))?,"revision":row.try_get::<i64,_>("revision").map_err(|error|runtime("decode label",error))?.to_string()});
        items.push(
            serde_json::from_value(value).map_err(|error| runtime("decode label item", error))?,
        );
        next_cursor = Some(seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(admin::ListLabelsResponse { items, next_cursor })
}

pub(crate) async fn put_cycle(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::PutCycleRequest,
) -> Result<admin::PutCycleResponse, StorageError> {
    let team: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM teams WHERE organization_id=$1 AND team_id=$2)",
    )
    .bind(&request.organization_id)
    .bind(&request.team_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("validate cycle team", error))?;
    if !team {
        return Err(DomainFailure::NotFound.into());
    }
    let starts_on = Some(request.starts_on.clone());
    let ends_on = Some(request.ends_on.clone());
    let starts_on = parse_date(&starts_on)?.ok_or(DomainFailure::InvalidRequest)?;
    let ends_on = parse_date(&ends_on)?.ok_or(DomainFailure::InvalidRequest)?;
    if ends_on < starts_on {
        return Err(DomainFailure::InvalidRequest.into());
    }
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put cycle", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_CYCLE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit cycle replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let revision: i64 = if let Some(expected) = &request.expected_revision {
        sqlx::query_scalar("UPDATE cycles SET number=$5,name=$6,starts_on=$7,ends_on=$8,revision=revision+1 WHERE organization_id=$1 AND team_id=$2 AND cycle_id=$3 AND revision=$4 RETURNING revision").bind(&request.organization_id).bind(&request.team_id).bind(&request.cycle_id).bind(parse_revision(expected)?).bind(request.number).bind(request.name.as_deref().map(str::trim)).bind(starts_on).bind(ends_on).fetch_optional(&mut *tx).await.map_err(|error|runtime("update cycle",error))?.ok_or(DomainFailure::RevisionConflict)?
    } else {
        sqlx::query_scalar("INSERT INTO cycles(cycle_id,organization_id,team_id,number,name,starts_on,ends_on) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING RETURNING revision").bind(&request.cycle_id).bind(&request.organization_id).bind(&request.team_id).bind(request.number).bind(request.name.as_deref().map(str::trim)).bind(starts_on).bind(ends_on).fetch_optional(&mut *tx).await.map_err(|error|runtime("insert cycle",error))?.ok_or(DomainFailure::KeyConflict)?
    };
    let response = admin::PutCycleResponse {
        cycle_id: request.cycle_id.clone(),
        organization_id: request.organization_id.clone(),
        team_id: request.team_id.clone(),
        number: request.number,
        name: request
            .name
            .as_deref()
            .map(str::trim)
            .map(ToOwned::to_owned),
        starts_on: request.starts_on.clone(),
        ends_on: request.ends_on.clone(),
        revision: revision.to_string(),
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        None,
        None,
        actor,
        admin::PUT_CYCLE_OPERATION,
        "cycle",
        &request.cycle_id,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_CYCLE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit cycle", error))?;
    Ok(response)
}

pub(crate) async fn list_cycles(
    postgres: &OwnedPostgres,
    request: &admin::ListCyclesRequest,
) -> Result<admin::ListCyclesResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let rows=sqlx::query("SELECT cycle_id,organization_id,team_id,number,name,starts_on,ends_on,revision,row_seq FROM cycles WHERE organization_id=$1 AND team_id=$2 AND row_seq>$3 ORDER BY row_seq LIMIT $4").bind(&request.organization_id).bind(&request.team_id).bind(after).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list cycles",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode cycle", error))?;
        let starts_on: Date = row
            .try_get("starts_on")
            .map_err(|error| runtime("decode cycle", error))?;
        let ends_on: Date = row
            .try_get("ends_on")
            .map_err(|error| runtime("decode cycle", error))?;
        let value = json!({"cycle_id":row.try_get::<String,_>("cycle_id").map_err(|error|runtime("decode cycle",error))?,"organization_id":row.try_get::<String,_>("organization_id").map_err(|error|runtime("decode cycle",error))?,"team_id":row.try_get::<String,_>("team_id").map_err(|error|runtime("decode cycle",error))?,"number":row.try_get::<i32,_>("number").map_err(|error|runtime("decode cycle",error))?,"name":row.try_get::<Option<String>,_>("name").map_err(|error|runtime("decode cycle",error))?,"starts_on":starts_on.to_string(),"ends_on":ends_on.to_string(),"revision":row.try_get::<i64,_>("revision").map_err(|error|runtime("decode cycle",error))?.to_string()});
        items.push(
            serde_json::from_value(value).map_err(|error| runtime("decode cycle item", error))?,
        );
        next_cursor = Some(seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(admin::ListCyclesResponse { items, next_cursor })
}

pub(crate) async fn put_milestone(
    postgres: &OwnedPostgres,
    caller: &str,
    actor: &str,
    request: &admin::PutMilestoneRequest,
) -> Result<admin::PutMilestoneResponse, StorageError> {
    let project: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE organization_id=$1 AND project_id=$2)",
    )
    .bind(&request.organization_id)
    .bind(&request.project_id)
    .fetch_one(postgres.pool())
    .await
    .map_err(|error| runtime("validate milestone project", error))?;
    if !project {
        return Err(DomainFailure::NotFound.into());
    }
    let target_date = parse_date(&request.target_date)?;
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin put milestone", error))?;
    match reserve_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_MILESTONE_OPERATION,
        &request.idempotency_key,
        request,
    )
    .await?
    {
        CommandStart::Replay(value) => {
            let response = replay(value)?;
            tx.commit()
                .await
                .map_err(|error| runtime("commit milestone replay", error))?;
            return Ok(response);
        }
        CommandStart::Conflict => return Err(DomainFailure::IdempotencyConflict.into()),
        CommandStart::New => {}
    }
    let revision: i64 = if let Some(expected) = &request.expected_revision {
        sqlx::query_scalar("UPDATE milestones SET name=$5,description=$6,target_date=$7,revision=revision+1 WHERE organization_id=$1 AND project_id=$2 AND milestone_id=$3 AND revision=$4 RETURNING revision").bind(&request.organization_id).bind(&request.project_id).bind(&request.milestone_id).bind(parse_revision(expected)?).bind(request.name.trim()).bind(request.description.as_deref().map(str::trim)).bind(target_date).fetch_optional(&mut *tx).await.map_err(|error|runtime("update milestone",error))?.ok_or(DomainFailure::RevisionConflict)?
    } else {
        sqlx::query_scalar("INSERT INTO milestones(milestone_id,organization_id,project_id,name,description,target_date) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING revision").bind(&request.milestone_id).bind(&request.organization_id).bind(&request.project_id).bind(request.name.trim()).bind(request.description.as_deref().map(str::trim)).bind(target_date).fetch_optional(&mut *tx).await.map_err(|error|runtime("insert milestone",error))?.ok_or(DomainFailure::KeyConflict)?
    };
    let response = admin::PutMilestoneResponse {
        milestone_id: request.milestone_id.clone(),
        organization_id: request.organization_id.clone(),
        project_id: request.project_id.clone(),
        name: request.name.trim().to_owned(),
        description: request
            .description
            .as_deref()
            .map(str::trim)
            .map(ToOwned::to_owned),
        target_date: request.target_date.clone(),
        revision: revision.to_string(),
    };
    append_activity(
        &mut tx,
        &request.organization_id,
        Some(&request.project_id),
        None,
        actor,
        admin::PUT_MILESTONE_OPERATION,
        "milestone",
        &request.milestone_id,
        Some(revision),
    )
    .await?;
    complete_command(
        &mut tx,
        caller,
        actor,
        admin::PUT_MILESTONE_OPERATION,
        &request.idempotency_key,
        &response,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit milestone", error))?;
    Ok(response)
}

pub(crate) async fn list_milestones(
    postgres: &OwnedPostgres,
    request: &admin::ListMilestonesRequest,
) -> Result<admin::ListMilestonesResponse, StorageError> {
    let after = parse_cursor(&request.after)?;
    let rows=sqlx::query("SELECT milestone_id,organization_id,project_id,name,description,target_date,revision,row_seq FROM milestones WHERE organization_id=$1 AND project_id=$2 AND row_seq>$3 ORDER BY row_seq LIMIT $4").bind(&request.organization_id).bind(&request.project_id).bind(after).bind(request.limit+1).fetch_all(postgres.pool()).await.map_err(|error|runtime("list milestones",error))?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|count| count > request.limit);
    let mut items = Vec::new();
    let mut next_cursor = None;
    for row in rows
        .into_iter()
        .take(usize::try_from(request.limit).unwrap_or(0))
    {
        let seq: i64 = row
            .try_get("row_seq")
            .map_err(|error| runtime("decode milestone", error))?;
        let target_date: Option<Date> = row
            .try_get("target_date")
            .map_err(|error| runtime("decode milestone", error))?;
        let value = json!({"milestone_id":row.try_get::<String,_>("milestone_id").map_err(|error|runtime("decode milestone",error))?,"organization_id":row.try_get::<String,_>("organization_id").map_err(|error|runtime("decode milestone",error))?,"project_id":row.try_get::<String,_>("project_id").map_err(|error|runtime("decode milestone",error))?,"name":row.try_get::<String,_>("name").map_err(|error|runtime("decode milestone",error))?,"description":row.try_get::<Option<String>,_>("description").map_err(|error|runtime("decode milestone",error))?,"target_date":format_date(target_date),"revision":row.try_get::<i64,_>("revision").map_err(|error|runtime("decode milestone",error))?.to_string()});
        items.push(
            serde_json::from_value(value)
                .map_err(|error| runtime("decode milestone item", error))?,
        );
        next_cursor = Some(seq.to_string());
    }
    if !has_more {
        next_cursor = None;
    }
    Ok(admin::ListMilestonesResponse { items, next_cursor })
}

pub(crate) async fn collect_export(
    postgres: &OwnedPostgres,
    request: &lenso_capability_data_export_source::CollectExportRequest,
) -> Result<lenso_capability_data_export_source::CollectExportResponse, StorageError> {
    let comments=sqlx::query("SELECT comment_id,issue_id,body,deleted,created_at,updated_at FROM comments WHERE organization_id=$1 AND author_subject=$2 ORDER BY row_seq").bind(&request.scope_id).bind(&request.subject).fetch_all(postgres.pool()).await.map_err(|error|runtime("collect comment export",error))?;
    let mut comment_values = Vec::new();
    for row in comments {
        let created_at: OffsetDateTime = row
            .try_get("created_at")
            .map_err(|error| runtime("decode comment export", error))?;
        let updated_at: OffsetDateTime = row
            .try_get("updated_at")
            .map_err(|error| runtime("decode comment export", error))?;
        comment_values.push(json!({"comment_id":row.try_get::<String,_>("comment_id").map_err(|error|runtime("decode comment export",error))?,"issue_id":row.try_get::<String,_>("issue_id").map_err(|error|runtime("decode comment export",error))?,"body":row.try_get::<String,_>("body").map_err(|error|runtime("decode comment export",error))?,"deleted":row.try_get::<bool,_>("deleted").map_err(|error|runtime("decode comment export",error))?,"created_at":format_time(created_at)?,"updated_at":format_time(updated_at)?}));
    }
    let updates=sqlx::query("SELECT update_id,project_id,body,health,created_at FROM project_updates WHERE organization_id=$1 AND author_subject=$2 ORDER BY row_seq").bind(&request.scope_id).bind(&request.subject).fetch_all(postgres.pool()).await.map_err(|error|runtime("collect project update export",error))?;
    let mut update_values = Vec::new();
    for row in updates {
        let created_at: OffsetDateTime = row
            .try_get("created_at")
            .map_err(|error| runtime("decode project update export", error))?;
        update_values.push(json!({"update_id":row.try_get::<String,_>("update_id").map_err(|error|runtime("decode project update export",error))?,"project_id":row.try_get::<String,_>("project_id").map_err(|error|runtime("decode project update export",error))?,"body":row.try_get::<String,_>("body").map_err(|error|runtime("decode project update export",error))?,"health":row.try_get::<String,_>("health").map_err(|error|runtime("decode project update export",error))?,"created_at":format_time(created_at)?}));
    }
    let activities=sqlx::query("SELECT activity_id,project_id,issue_id,operation,entity_kind,entity_id,revision,occurred_at FROM project_activity WHERE organization_id=$1 AND actor_subject=$2 ORDER BY activity_id").bind(&request.scope_id).bind(&request.subject).fetch_all(postgres.pool()).await.map_err(|error|runtime("collect activity export",error))?;
    let mut activity_values = Vec::new();
    for row in activities {
        let occurred_at: OffsetDateTime = row
            .try_get("occurred_at")
            .map_err(|error| runtime("decode activity export", error))?;
        activity_values.push(json!({"activity_id":row.try_get::<i64,_>("activity_id").map_err(|error|runtime("decode activity export",error))?.to_string(),"project_id":row.try_get::<Option<String>,_>("project_id").map_err(|error|runtime("decode activity export",error))?,"issue_id":row.try_get::<Option<String>,_>("issue_id").map_err(|error|runtime("decode activity export",error))?,"operation":row.try_get::<String,_>("operation").map_err(|error|runtime("decode activity export",error))?,"entity_kind":row.try_get::<String,_>("entity_kind").map_err(|error|runtime("decode activity export",error))?,"entity_id":row.try_get::<String,_>("entity_id").map_err(|error|runtime("decode activity export",error))?,"revision":row.try_get::<Option<i64>,_>("revision").map_err(|error|runtime("decode activity export",error))?.map(|value|value.to_string()),"occurred_at":format_time(occurred_at)?}));
    }
    let payload=serde_json::to_string(&json!({"organization_id":request.scope_id,"subject":request.subject,"comments":comment_values,"project_updates":update_values,"activity":activity_values})).map_err(|error|runtime("serialize Projects export",error))?;
    Ok(lenso_capability_data_export_source::CollectExportResponse {
        items: vec![
            lenso_capability_data_export_source::CollectExportResponseItemsItem {
                item_name: "projects.json".to_owned(),
                media_type: "application/json".to_owned(),
                payload,
            },
        ],
    })
}

pub(crate) async fn apply_retention(
    postgres: &OwnedPostgres,
    request: &lenso_capability_retention_participant::ApplyRetentionRequest,
) -> Result<lenso_capability_retention_participant::ApplyRetentionResponse, StorageError> {
    use lenso_capability_retention_participant::ApplyRetentionRequestMode;
    let mode = match request.mode {
        ApplyRetentionRequestMode::Delete => "delete",
        ApplyRetentionRequestMode::Anonymize => "anonymize",
    };
    let mut tx = postgres
        .pool()
        .begin()
        .await
        .map_err(|error| runtime("begin Projects retention", error))?;
    let existing=sqlx::query("SELECT organization_id,subject,mode,receipt FROM project_retention_receipts WHERE action_id=$1 FOR UPDATE").bind(&request.action_id).fetch_optional(&mut *tx).await.map_err(|error|runtime("read Projects retention replay",error))?;
    if let Some(row) = existing {
        let organization_id: String = row
            .try_get("organization_id")
            .map_err(|error| runtime("decode retention replay", error))?;
        let subject: String = row
            .try_get("subject")
            .map_err(|error| runtime("decode retention replay", error))?;
        let stored_mode: String = row
            .try_get("mode")
            .map_err(|error| runtime("decode retention replay", error))?;
        if organization_id != request.scope_id || subject != request.subject || stored_mode != mode
        {
            return Err(DomainFailure::IdempotencyConflict.into());
        }
        let receipt: String = row
            .try_get("receipt")
            .map_err(|error| runtime("decode retention replay", error))?;
        tx.commit()
            .await
            .map_err(|error| runtime("commit retention replay", error))?;
        return Ok(lenso_capability_retention_participant::ApplyRetentionResponse { receipt });
    }
    let tombstone = format!("privacy:{}", request.action_id);
    if mode == "delete" {
        sqlx::query("UPDATE comments SET author_subject=$3,body='',revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND author_subject=$2").bind(&request.scope_id).bind(&request.subject).bind(&tombstone).execute(&mut *tx).await.map_err(|error|runtime("delete retained comments",error))?;
        sqlx::query("UPDATE project_updates SET author_subject=$3,body='' WHERE organization_id=$1 AND author_subject=$2").bind(&request.scope_id).bind(&request.subject).bind(&tombstone).execute(&mut *tx).await.map_err(|error|runtime("delete retained project updates",error))?;
    } else {
        sqlx::query("UPDATE comments SET author_subject=$3,revision=revision+1,updated_at=transaction_timestamp() WHERE organization_id=$1 AND author_subject=$2").bind(&request.scope_id).bind(&request.subject).bind(&tombstone).execute(&mut *tx).await.map_err(|error|runtime("anonymize comments",error))?;
        sqlx::query("UPDATE project_updates SET author_subject=$3 WHERE organization_id=$1 AND author_subject=$2").bind(&request.scope_id).bind(&request.subject).bind(&tombstone).execute(&mut *tx).await.map_err(|error|runtime("anonymize project updates",error))?;
    }
    sqlx::query("UPDATE project_activity SET actor_subject=$3 WHERE organization_id=$1 AND actor_subject=$2").bind(&request.scope_id).bind(&request.subject).bind(&tombstone).execute(&mut *tx).await.map_err(|error|runtime("anonymize activity",error))?;
    let receipt = format!("projects-retention:{}", request.action_id);
    sqlx::query("INSERT INTO project_retention_receipts(action_id,organization_id,subject,mode,receipt) VALUES($1,$2,$3,$4,$5)").bind(&request.action_id).bind(&request.scope_id).bind(&request.subject).bind(mode).bind(&receipt).execute(&mut *tx).await.map_err(|error|runtime("store retention receipt",error))?;
    tx.commit()
        .await
        .map_err(|error| runtime("commit Projects retention", error))?;
    Ok(lenso_capability_retention_participant::ApplyRetentionResponse { receipt })
}
