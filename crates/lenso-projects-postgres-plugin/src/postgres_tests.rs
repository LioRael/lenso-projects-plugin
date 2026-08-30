use super::*;

use lenso_capability_projects_admin::{PutTeamRequest, PutWorkflowStateRequest, WorkflowCategory};
use lenso_postgres_kit::OwnedPostgres;
use sqlx::AssertSqlSafe;

async fn prepare() -> Option<(String, String, OwnedPostgres)> {
    let Some(database_url) = std::env::var("LENSO_PROJECTS_TEST_DATABASE_URL").ok() else {
        eprintln!("skipping PostgreSQL acceptance; LENSO_PROJECTS_TEST_DATABASE_URL is unset");
        return None;
    };
    let database_name = database_url
        .split('?')
        .next()
        .and_then(|value| value.rsplit('/').next())
        .unwrap_or_default();
    assert!(
        database_name.starts_with("lenso_projects_test"),
        "PostgreSQL acceptance requires a dedicated database whose name starts with lenso_projects_test"
    );
    let schema = format!("projects_test_{}", uuid::Uuid::new_v4().simple());
    ProjectsOperator::setup(&database_url, &schema)
        .await
        .unwrap();
    let postgres =
        OwnedPostgres::prepare(&database_url, schema::schema_plan(schema.clone()).unwrap())
            .await
            .unwrap();
    Some((database_url, schema, postgres))
}

async fn cleanup(database_url: &str, schema: &str, postgres: OwnedPostgres) {
    postgres.pool().close().await;
    let pool = sqlx::PgPool::connect(database_url).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

async fn put_team_and_workflow(
    postgres: &OwnedPostgres,
    team_id: &str,
    key: &str,
    state_id: &str,
) -> admin::PutTeamResponse {
    let team = storage::put_team(
        postgres,
        "admin-api",
        "usr_admin",
        &PutTeamRequest {
            idempotency_key: format!("put-{team_id}"),
            organization_id: "org_acme".to_owned(),
            team_id: team_id.to_owned(),
            key: key.to_owned(),
            name: key.to_owned(),
            description: None,
            private: false,
            default_workflow_state_id: None,
            expected_revision: None,
        },
    )
    .await
    .unwrap();
    storage::put_workflow_state(
        postgres,
        "admin-api",
        "usr_admin",
        &PutWorkflowStateRequest {
            idempotency_key: format!("put-{state_id}"),
            organization_id: "org_acme".to_owned(),
            state_id: state_id.to_owned(),
            team_id: team_id.to_owned(),
            name: "Started".to_owned(),
            category: WorkflowCategory::Started,
            color: "#3366FF".to_owned(),
            position: 1,
            expected_revision: None,
        },
    )
    .await
    .unwrap();
    team
}

#[tokio::test]
async fn concurrent_idempotency_identifier_history_activity_and_restart() {
    let Some((database_url, schema_name, postgres)) = prepare().await else {
        return;
    };
    let eng = put_team_and_workflow(&postgres, "team_eng", "ENG", "state_eng_started").await;
    let _ops = put_team_and_workflow(&postgres, "team_ops", "OPS", "state_ops_started").await;

    let workflow = storage::list_workflow_states(
        &postgres,
        &admin::ListWorkflowStatesRequest {
            organization_id: "org_acme".to_owned(),
            team_id: "team_eng".to_owned(),
            after: None,
            limit: 100,
        },
    )
    .await
    .unwrap();
    assert_eq!(workflow.items.len(), 6);
    let defaults = workflow
        .items
        .iter()
        .filter(|state| state.state_id != "state_eng_started")
        .collect::<Vec<_>>();
    assert_eq!(defaults.len(), 5);
    assert_eq!(
        defaults
            .iter()
            .map(|state| state.name.as_str())
            .collect::<Vec<_>>(),
        vec!["Backlog", "Todo", "In Progress", "Done", "Canceled"]
    );
    assert_eq!(
        defaults
            .iter()
            .find(|state| state.state_id == eng.default_workflow_state_id)
            .unwrap()
            .category,
        WorkflowCategory::Unstarted
    );
    assert!(defaults.iter().all(|state| !state.archived));

    let todo = storage::get_workflow_state(
        &postgres,
        &admin::GetWorkflowStateRequest {
            organization_id: "org_acme".to_owned(),
            team_id: "team_eng".to_owned(),
            state_id: eng.default_workflow_state_id.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(todo.category, WorkflowCategory::Unstarted);
    assert!(matches!(
        storage::get_workflow_state(
            &postgres,
            &admin::GetWorkflowStateRequest {
                organization_id: "org_acme".to_owned(),
                team_id: "team_ops".to_owned(),
                state_id: eng.default_workflow_state_id.clone(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::NotFound))
    ));

    let workflow_reorder_request = admin::ReorderWorkflowStatesRequest {
        idempotency_key: "reorder-eng-workflow".to_owned(),
        organization_id: "org_acme".to_owned(),
        team_id: "team_eng".to_owned(),
        items: workflow
            .items
            .iter()
            .rev()
            .enumerate()
            .map(
                |(position, state)| admin::ReorderWorkflowStatesRequestItemsItem {
                    state_id: state.state_id.clone(),
                    expected_revision: state.revision.clone(),
                    position: i64::try_from(position).unwrap(),
                },
            )
            .collect(),
    };
    let mut duplicate_workflow_positions = workflow_reorder_request.clone();
    duplicate_workflow_positions.idempotency_key = "duplicate-eng-workflow-order".to_owned();
    duplicate_workflow_positions.items[1].position = duplicate_workflow_positions.items[0].position;
    assert!(matches!(
        storage::reorder_workflow_states(
            &postgres,
            "admin-api",
            "usr_admin",
            &duplicate_workflow_positions,
        )
        .await,
        Err(StorageError::Domain(DomainFailure::InvalidRequest))
    ));
    let mut cross_team_workflow = workflow_reorder_request.clone();
    cross_team_workflow.idempotency_key = "cross-team-workflow-order".to_owned();
    cross_team_workflow.items[0].state_id = "state_ops_started".to_owned();
    assert!(matches!(
        storage::reorder_workflow_states(
            &postgres,
            "admin-api",
            "usr_admin",
            &cross_team_workflow,
        )
        .await,
        Err(StorageError::Domain(DomainFailure::InvalidRequest))
    ));
    let mut partial_workflow = workflow_reorder_request.clone();
    partial_workflow.idempotency_key = "partial-eng-workflow-order".to_owned();
    partial_workflow.items.pop();
    assert!(matches!(
        storage::reorder_workflow_states(&postgres, "admin-api", "usr_admin", &partial_workflow,)
            .await,
        Err(StorageError::Domain(DomainFailure::InvalidRequest))
    ));
    let reordered_workflow = storage::reorder_workflow_states(
        &postgres,
        "admin-api",
        "usr_admin",
        &workflow_reorder_request,
    )
    .await
    .unwrap();
    assert_eq!(reordered_workflow.items.len(), 6);
    assert_eq!(
        reordered_workflow,
        storage::reorder_workflow_states(
            &postgres,
            "admin-api",
            "usr_admin",
            &workflow_reorder_request,
        )
        .await
        .unwrap()
    );
    assert!(
        reordered_workflow
            .items
            .iter()
            .enumerate()
            .all(
                |(position, state)| state.position == i64::try_from(position).unwrap()
                    && state.revision == "2"
            )
    );
    let mut stale_workflow_items = reordered_workflow
        .items
        .iter()
        .map(|state| admin::ReorderWorkflowStatesRequestItemsItem {
            state_id: state.state_id.clone(),
            expected_revision: state.revision.clone(),
            position: state.position,
        })
        .collect::<Vec<_>>();
    stale_workflow_items[0].expected_revision = "1".to_owned();
    let unchanged_workflow = reordered_workflow.items[1].clone();
    assert!(matches!(
        storage::reorder_workflow_states(
            &postgres,
            "admin-api",
            "usr_admin",
            &admin::ReorderWorkflowStatesRequest {
                idempotency_key: "stale-eng-workflow-reorder".to_owned(),
                organization_id: "org_acme".to_owned(),
                team_id: "team_eng".to_owned(),
                items: stale_workflow_items,
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::RevisionConflict))
    ));
    let after_stale_workflow = storage::get_workflow_state(
        &postgres,
        &admin::GetWorkflowStateRequest {
            organization_id: "org_acme".to_owned(),
            team_id: "team_eng".to_owned(),
            state_id: unchanged_workflow.state_id.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(after_stale_workflow.revision, unchanged_workflow.revision);
    assert_eq!(after_stale_workflow.position, unchanged_workflow.position);
    let default_workflow_revision = reordered_workflow
        .items
        .iter()
        .find(|state| state.state_id == eng.default_workflow_state_id)
        .unwrap()
        .revision
        .clone();
    let custom_workflow_revision = reordered_workflow
        .items
        .iter()
        .find(|state| state.state_id == "state_eng_started")
        .unwrap()
        .revision
        .clone();

    let statuses = storage::list_project_statuses(
        &postgres,
        &admin::ListProjectStatusesRequest {
            organization_id: "org_acme".to_owned(),
            after: None,
            limit: 100,
        },
    )
    .await
    .unwrap();
    assert_eq!(statuses.items.len(), 6);
    let default_status = statuses
        .items
        .iter()
        .find(|status| status.is_default)
        .unwrap();
    assert_eq!(default_status.name, "Backlog");
    assert_eq!(
        default_status.category,
        admin::ProjectStatusCategory::Backlog
    );

    let project = storage::create_project(
        &postgres,
        "projects-api",
        "usr_admin",
        &projects::CreateProjectRequest {
            idempotency_key: "create-roadmap".to_owned(),
            organization_id: "org_acme".to_owned(),
            project_id: "project_roadmap".to_owned(),
            name: "Roadmap".to_owned(),
            summary: None,
            lead_team_id: "team_eng".to_owned(),
            team_ids: vec!["team_eng".to_owned(), "team_ops".to_owned()],
            status_id: None,
            milestone_id: None,
            start_date: None,
            target_date: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(project.revision, "1");
    assert_eq!(project.status_id, default_status.status_id);
    assert!(project.completed_at.is_none());
    assert!(project.canceled_at.is_none());

    let completed_status = storage::put_project_status(
        &postgres,
        "admin-api",
        "usr_admin",
        &admin::PutProjectStatusRequest {
            idempotency_key: "put-shipped-status".to_owned(),
            organization_id: "org_acme".to_owned(),
            status_id: "status_shipped".to_owned(),
            name: "Shipped".to_owned(),
            category: admin::ProjectStatusCategory::Completed,
            color: "#00AA66".to_owned(),
            position: 10,
            expected_revision: None,
        },
    )
    .await
    .unwrap();
    let fetched_completed = storage::get_project_status(
        &postgres,
        &admin::GetProjectStatusRequest {
            organization_id: "org_acme".to_owned(),
            status_id: completed_status.status_id.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        fetched_completed.category,
        admin::ProjectStatusCategory::Completed
    );
    let statuses_for_reorder = storage::list_project_statuses(
        &postgres,
        &admin::ListProjectStatusesRequest {
            organization_id: "org_acme".to_owned(),
            after: None,
            limit: 100,
        },
    )
    .await
    .unwrap();
    let status_reorder_request = admin::ReorderProjectStatusesRequest {
        idempotency_key: "reorder-project-statuses".to_owned(),
        organization_id: "org_acme".to_owned(),
        items: statuses_for_reorder
            .items
            .iter()
            .rev()
            .enumerate()
            .map(
                |(position, status)| admin::ReorderProjectStatusesRequestItemsItem {
                    status_id: status.status_id.clone(),
                    expected_revision: status.revision.clone(),
                    position: i64::try_from(position).unwrap(),
                },
            )
            .collect(),
    };
    let reordered_statuses = storage::reorder_project_statuses(
        &postgres,
        "admin-api",
        "usr_admin",
        &status_reorder_request,
    )
    .await
    .unwrap();
    assert_eq!(reordered_statuses.items.len(), 7);
    assert_eq!(
        reordered_statuses,
        storage::reorder_project_statuses(
            &postgres,
            "admin-api",
            "usr_admin",
            &status_reorder_request,
        )
        .await
        .unwrap()
    );
    assert!(
        reordered_statuses
            .items
            .iter()
            .enumerate()
            .all(
                |(position, status)| status.position == i64::try_from(position).unwrap()
                    && status.revision == "2"
            )
    );
    let mut stale_status_items = reordered_statuses
        .items
        .iter()
        .map(|status| admin::ReorderProjectStatusesRequestItemsItem {
            status_id: status.status_id.clone(),
            expected_revision: status.revision.clone(),
            position: status.position,
        })
        .collect::<Vec<_>>();
    stale_status_items[0].expected_revision = "1".to_owned();
    let unchanged_status = reordered_statuses.items[1].clone();
    assert!(matches!(
        storage::reorder_project_statuses(
            &postgres,
            "admin-api",
            "usr_admin",
            &admin::ReorderProjectStatusesRequest {
                idempotency_key: "stale-project-status-reorder".to_owned(),
                organization_id: "org_acme".to_owned(),
                items: stale_status_items,
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::RevisionConflict))
    ));
    let after_stale_status = storage::get_project_status(
        &postgres,
        &admin::GetProjectStatusRequest {
            organization_id: "org_acme".to_owned(),
            status_id: unchanged_status.status_id.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(after_stale_status.revision, unchanged_status.revision);
    assert_eq!(after_stale_status.position, unchanged_status.position);
    let completed_status_revision = reordered_statuses
        .items
        .iter()
        .find(|status| status.status_id == completed_status.status_id)
        .unwrap()
        .revision
        .clone();
    let default_status_revision = reordered_statuses
        .items
        .iter()
        .find(|status| status.status_id == default_status.status_id)
        .unwrap()
        .revision
        .clone();
    let planned_status = reordered_statuses
        .items
        .iter()
        .find(|status| status.name == "Planned")
        .unwrap()
        .clone();
    let completed_project = storage::update_project(
        &postgres,
        "projects-api",
        "usr_admin",
        &projects::UpdateProjectRequest {
            idempotency_key: "complete-roadmap".to_owned(),
            organization_id: "org_acme".to_owned(),
            project_id: "project_roadmap".to_owned(),
            expected_revision: "1".to_owned(),
            name: "Roadmap".to_owned(),
            summary: None,
            lead_team_id: "team_eng".to_owned(),
            team_ids: vec!["team_eng".to_owned(), "team_ops".to_owned()],
            status_id: completed_status.status_id.clone(),
            milestone_id: None,
            start_date: None,
            target_date: None,
        },
    )
    .await
    .unwrap();
    assert!(completed_project.completed_at.is_some());
    assert!(completed_project.canceled_at.is_none());

    let stale_project_update = storage::update_project(
        &postgres,
        "projects-api",
        "usr_admin",
        &projects::UpdateProjectRequest {
            idempotency_key: "stale-roadmap-update".to_owned(),
            organization_id: "org_acme".to_owned(),
            project_id: "project_roadmap".to_owned(),
            expected_revision: "1".to_owned(),
            name: "Stale Roadmap".to_owned(),
            summary: None,
            lead_team_id: "team_eng".to_owned(),
            team_ids: vec!["team_eng".to_owned(), "team_ops".to_owned()],
            status_id: completed_project.status_id.clone(),
            milestone_id: None,
            start_date: None,
            target_date: None,
        },
    )
    .await;
    assert!(matches!(
        stale_project_update,
        Err(StorageError::Domain(DomainFailure::RevisionConflict))
    ));

    let referenced_status_archive = storage::archive_project_status(
        &postgres,
        "admin-api",
        "usr_admin",
        &admin::ArchiveProjectStatusRequest {
            idempotency_key: "archive-shipped-status".to_owned(),
            organization_id: "org_acme".to_owned(),
            status_id: completed_status.status_id.clone(),
            expected_revision: completed_status_revision.clone(),
            archived: true,
        },
    )
    .await;
    assert!(matches!(
        referenced_status_archive,
        Err(StorageError::Domain(DomainFailure::ActiveReference))
    ));
    assert!(matches!(
        storage::delete_project_status(
            &postgres,
            "admin-api",
            "usr_admin",
            &admin::DeleteProjectStatusRequest {
                idempotency_key: "delete-shipped-status".to_owned(),
                organization_id: "org_acme".to_owned(),
                status_id: completed_status.status_id.clone(),
                expected_revision: completed_status_revision.clone(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::ActiveReference))
    ));

    let request = projects::CreateIssueRequest {
        idempotency_key: "create-first".to_owned(),
        organization_id: "org_acme".to_owned(),
        issue_id: "issue_first".to_owned(),
        project_id: "project_roadmap".to_owned(),
        team_id: "team_eng".to_owned(),
        title: "First issue".to_owned(),
        description: None,
        priority: projects::Priority::High,
        workflow_state_id: None,
        cycle_id: None,
        milestone_id: None,
        parent_issue_id: None,
        label_ids: vec![],
    };
    let (first, replay) = tokio::join!(
        storage::create_issue(&postgres, "projects-api", "usr_admin", &request),
        storage::create_issue(&postgres, "projects-api", "usr_admin", &request)
    );
    let first = first.unwrap();
    let replay = replay.unwrap();
    assert_eq!(first, replay);
    assert_eq!(first.identifier, "ENG-1");
    assert_eq!(first.workflow_state_id, eng.default_workflow_state_id);
    let mut conflicting_replay = request.clone();
    conflicting_replay.title = "A different command".to_owned();
    assert!(matches!(
        storage::create_issue(&postgres, "projects-api", "usr_admin", &conflicting_replay).await,
        Err(StorageError::Domain(DomainFailure::IdempotencyConflict))
    ));

    let default_archive = storage::archive_workflow_state(
        &postgres,
        "admin-api",
        "usr_admin",
        &admin::ArchiveWorkflowStateRequest {
            idempotency_key: "archive-default-eng".to_owned(),
            organization_id: "org_acme".to_owned(),
            state_id: eng.default_workflow_state_id.clone(),
            expected_revision: default_workflow_revision.clone(),
            archived: true,
        },
    )
    .await;
    assert!(matches!(
        default_archive,
        Err(StorageError::Domain(DomainFailure::DefaultStateInvalid))
    ));

    let status_archive = storage::archive_project_status(
        &postgres,
        "admin-api",
        "usr_admin",
        &admin::ArchiveProjectStatusRequest {
            idempotency_key: "archive-default-status".to_owned(),
            organization_id: "org_acme".to_owned(),
            status_id: default_status.status_id.clone(),
            expected_revision: default_status_revision.clone(),
            archived: true,
        },
    )
    .await;
    assert!(matches!(
        status_archive,
        Err(StorageError::Domain(DomainFailure::DefaultStateInvalid))
    ));
    assert!(matches!(
        storage::delete_workflow_state(
            &postgres,
            "admin-api",
            "usr_admin",
            &admin::DeleteWorkflowStateRequest {
                idempotency_key: "delete-default-eng".to_owned(),
                organization_id: "org_acme".to_owned(),
                team_id: "team_eng".to_owned(),
                state_id: eng.default_workflow_state_id.clone(),
                expected_revision: default_workflow_revision.clone(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::DefaultStateInvalid))
    ));
    assert!(matches!(
        storage::delete_project_status(
            &postgres,
            "admin-api",
            "usr_admin",
            &admin::DeleteProjectStatusRequest {
                idempotency_key: "delete-default-status".to_owned(),
                organization_id: "org_acme".to_owned(),
                status_id: default_status.status_id.clone(),
                expected_revision: default_status_revision.clone(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::DefaultStateInvalid))
    ));
    assert!(matches!(
        storage::delete_project_status(
            &postgres,
            "admin-api",
            "usr_admin",
            &admin::DeleteProjectStatusRequest {
                idempotency_key: "delete-active-planned".to_owned(),
                organization_id: "org_acme".to_owned(),
                status_id: planned_status.status_id.clone(),
                expected_revision: planned_status.revision.clone(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::InvalidRequest))
    ));
    let archived_planned = storage::archive_project_status(
        &postgres,
        "admin-api",
        "usr_admin",
        &admin::ArchiveProjectStatusRequest {
            idempotency_key: "archive-planned-status".to_owned(),
            organization_id: "org_acme".to_owned(),
            status_id: planned_status.status_id.clone(),
            expected_revision: planned_status.revision.clone(),
            archived: true,
        },
    )
    .await
    .unwrap();
    let delete_planned_request = admin::DeleteProjectStatusRequest {
        idempotency_key: "delete-planned-status".to_owned(),
        organization_id: "org_acme".to_owned(),
        status_id: planned_status.status_id.clone(),
        expected_revision: archived_planned.revision,
    };
    let deleted_planned = storage::delete_project_status(
        &postgres,
        "admin-api",
        "usr_admin",
        &delete_planned_request,
    )
    .await
    .unwrap();
    assert!(deleted_planned.deleted);
    assert_eq!(
        deleted_planned,
        storage::delete_project_status(
            &postgres,
            "admin-api",
            "usr_admin",
            &delete_planned_request,
        )
        .await
        .unwrap()
    );
    assert!(matches!(
        storage::get_project_status(
            &postgres,
            &admin::GetProjectStatusRequest {
                organization_id: "org_acme".to_owned(),
                status_id: planned_status.status_id.clone(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::NotFound))
    ));

    let moved = storage::move_issue(
        &postgres,
        "projects-api",
        "usr_admin",
        &projects::MoveIssueRequest {
            idempotency_key: "move-first".to_owned(),
            organization_id: "org_acme".to_owned(),
            issue_id: "issue_first".to_owned(),
            expected_revision: "1".to_owned(),
            team_id: "team_ops".to_owned(),
            workflow_state_id: "state_ops_started".to_owned(),
        },
    )
    .await
    .unwrap();
    assert_eq!(moved.identifier, "OPS-1");
    assert_eq!(moved.previous_identifiers, vec!["ENG-1"]);

    let detach_issue_team = storage::update_project(
        &postgres,
        "projects-api",
        "usr_admin",
        &projects::UpdateProjectRequest {
            idempotency_key: "detach-active-issue-team".to_owned(),
            organization_id: "org_acme".to_owned(),
            project_id: "project_roadmap".to_owned(),
            expected_revision: completed_project.revision.clone(),
            name: "Roadmap".to_owned(),
            summary: None,
            lead_team_id: "team_eng".to_owned(),
            team_ids: vec!["team_eng".to_owned()],
            status_id: completed_project.status_id.clone(),
            milestone_id: None,
            start_date: None,
            target_date: None,
        },
    )
    .await;
    assert!(matches!(
        detach_issue_team,
        Err(StorageError::Domain(DomainFailure::ActiveReference))
    ));

    let referenced_workflow_archive = storage::archive_workflow_state(
        &postgres,
        "admin-api",
        "usr_admin",
        &admin::ArchiveWorkflowStateRequest {
            idempotency_key: "archive-referenced-ops".to_owned(),
            organization_id: "org_acme".to_owned(),
            state_id: "state_ops_started".to_owned(),
            expected_revision: "1".to_owned(),
            archived: true,
        },
    )
    .await;
    assert!(matches!(
        referenced_workflow_archive,
        Err(StorageError::Domain(DomainFailure::ActiveReference))
    ));
    assert!(matches!(
        storage::delete_workflow_state(
            &postgres,
            "admin-api",
            "usr_admin",
            &admin::DeleteWorkflowStateRequest {
                idempotency_key: "delete-referenced-ops".to_owned(),
                organization_id: "org_acme".to_owned(),
                team_id: "team_ops".to_owned(),
                state_id: "state_ops_started".to_owned(),
                expected_revision: "1".to_owned(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::ActiveReference))
    ));

    let archived_workflow = storage::archive_workflow_state(
        &postgres,
        "admin-api",
        "usr_admin",
        &admin::ArchiveWorkflowStateRequest {
            idempotency_key: "archive-unused-eng".to_owned(),
            organization_id: "org_acme".to_owned(),
            state_id: "state_eng_started".to_owned(),
            expected_revision: custom_workflow_revision.clone(),
            archived: true,
        },
    )
    .await
    .unwrap();
    assert!(archived_workflow.archived);
    let archived_state_issue = storage::create_issue(
        &postgres,
        "projects-api",
        "usr_admin",
        &projects::CreateIssueRequest {
            idempotency_key: "create-with-archived-state".to_owned(),
            organization_id: "org_acme".to_owned(),
            issue_id: "issue_archived_state".to_owned(),
            project_id: "project_roadmap".to_owned(),
            team_id: "team_eng".to_owned(),
            title: "Must fail closed".to_owned(),
            description: None,
            priority: projects::Priority::None,
            workflow_state_id: Some("state_eng_started".to_owned()),
            cycle_id: None,
            milestone_id: None,
            parent_issue_id: None,
            label_ids: vec![],
        },
    )
    .await;
    assert!(matches!(
        archived_state_issue,
        Err(StorageError::Domain(DomainFailure::WorkflowStateNotFound))
    ));

    let delete_workflow_request = admin::DeleteWorkflowStateRequest {
        idempotency_key: "delete-unused-eng".to_owned(),
        organization_id: "org_acme".to_owned(),
        team_id: "team_eng".to_owned(),
        state_id: "state_eng_started".to_owned(),
        expected_revision: archived_workflow.revision.clone(),
    };
    let deleted_workflow = storage::delete_workflow_state(
        &postgres,
        "admin-api",
        "usr_admin",
        &delete_workflow_request,
    )
    .await
    .unwrap();
    assert!(deleted_workflow.deleted);
    assert_eq!(
        deleted_workflow,
        storage::delete_workflow_state(
            &postgres,
            "admin-api",
            "usr_admin",
            &delete_workflow_request,
        )
        .await
        .unwrap()
    );
    assert!(matches!(
        storage::get_workflow_state(
            &postgres,
            &admin::GetWorkflowStateRequest {
                organization_id: "org_acme".to_owned(),
                team_id: "team_eng".to_owned(),
                state_id: "state_eng_started".to_owned(),
            },
        )
        .await,
        Err(StorageError::Domain(DomainFailure::NotFound))
    ));

    let old_lookup = storage::get_issue(
        &postgres,
        "usr_admin",
        &projects::GetIssueRequest {
            organization_id: "org_acme".to_owned(),
            issue_ref: "ENG-1".to_owned(),
        },
    )
    .await
    .unwrap();
    assert_eq!(old_lookup.issue_id, "issue_first");
    assert_eq!(old_lookup.identifier, "OPS-1");

    let activity = storage::list_activity(
        &postgres,
        "usr_admin",
        &projects::ListActivityRequest {
            organization_id: "org_acme".to_owned(),
            project_id: Some("project_roadmap".to_owned()),
            issue_id: None,
            after: None,
            limit: 100,
        },
    )
    .await
    .unwrap();
    assert!(activity.items.len() >= 3);
    assert!(activity.items.windows(2).all(|pair| {
        pair[0].activity_id.parse::<i64>().unwrap() < pair[1].activity_id.parse::<i64>().unwrap()
    }));
    let checkpoint = activity.next_cursor.unwrap();
    assert_eq!(
        checkpoint,
        activity.items.last().unwrap().activity_id,
        "the durable cursor advances even when the page reaches the current end"
    );
    let resumed = storage::list_activity(
        &postgres,
        "usr_admin",
        &projects::ListActivityRequest {
            organization_id: "org_acme".to_owned(),
            project_id: Some("project_roadmap".to_owned()),
            issue_id: None,
            after: Some(checkpoint.clone()),
            limit: 100,
        },
    )
    .await
    .unwrap();
    assert!(resumed.items.is_empty());
    assert_eq!(resumed.next_cursor, Some(checkpoint));

    postgres.pool().close().await;
    let restarted = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.clone()).unwrap(),
    )
    .await
    .unwrap();
    let after_restart = storage::get_issue(
        &restarted,
        "usr_admin",
        &projects::GetIssueRequest {
            organization_id: "org_acme".to_owned(),
            issue_ref: "issue_first".to_owned(),
        },
    )
    .await
    .unwrap();
    assert_eq!(after_restart.identifier, "OPS-1");

    cleanup(&database_url, &schema_name, restarted).await;
}
