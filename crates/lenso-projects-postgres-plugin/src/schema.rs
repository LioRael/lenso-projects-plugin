use lenso_postgres_kit::{Migration, PlanError, SchemaPlan, sql_migrations};

const MIGRATIONS: &[Migration] =
    sql_migrations![(1, "create-projects", "migrations/001_create_projects.sql",)];

pub(crate) fn schema_plan(schema: impl Into<std::sync::Arc<str>>) -> Result<SchemaPlan, PlanError> {
    SchemaPlan::new(schema, MIGRATIONS)
}

#[cfg(test)]
mod tests {
    use super::MIGRATIONS;

    #[test]
    fn migration_encodes_core_linear_like_invariants() {
        let sql = MIGRATIONS[0].sql();
        assert!(
            sql.contains(
                "PRIMARY KEY (caller_instance, actor_subject, operation, idempotency_key)"
            )
        );
        assert!(sql.contains("UNIQUE (organization_id, team_key)"));
        assert!(sql.contains("next_issue_number BIGINT NOT NULL"));
        assert!(sql.contains("default_workflow_state_id TEXT NOT NULL"));
        assert!(sql.contains("project_workspaces"));
        assert!(sql.contains("project_statuses_one_default"));
        assert!(sql.contains("archived_at TIMESTAMPTZ"));
        assert!(sql.contains("completed_at TIMESTAMPTZ"));
        assert!(sql.contains("canceled_at TIMESTAMPTZ"));
        assert!(sql.contains("issue_identifier_aliases"));
        assert!(sql.contains("FOREIGN KEY (organization_id, team_id, workflow_state_id)"));
        assert!(sql.contains("FOREIGN KEY (organization_id, project_id, team_id)"));
        assert!(sql.contains("FOREIGN KEY (organization_id, project_id, milestone_id)"));
        assert!(sql.contains("project_activity"));
        assert!(sql.contains("GENERATED ALWAYS AS IDENTITY"));
    }
}
