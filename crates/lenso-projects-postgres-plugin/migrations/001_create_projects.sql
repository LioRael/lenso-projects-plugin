CREATE TABLE project_commands (
    caller_instance TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request JSONB NOT NULL,
    response JSONB,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (caller_instance, actor_subject, operation, idempotency_key),
    CHECK ((response IS NULL AND completed_at IS NULL) OR (response IS NOT NULL AND completed_at IS NOT NULL))
);

CREATE TABLE teams (
    team_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    team_key TEXT NOT NULL CHECK (team_key ~ '^[A-Z][A-Z0-9]{1,9}$'),
    name TEXT NOT NULL,
    description TEXT,
    private BOOLEAN NOT NULL,
    default_workflow_state_id TEXT NOT NULL,
    next_issue_number BIGINT NOT NULL DEFAULT 1 CHECK (next_issue_number > 0),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (organization_id, team_id),
    UNIQUE (organization_id, team_key)
);

CREATE TABLE team_members (
    organization_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (organization_id, team_id, subject),
    FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, team_id) ON DELETE CASCADE
);

CREATE TABLE workflow_states (
    state_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('backlog','unstarted','started','completed','canceled')),
    color TEXT NOT NULL,
    position INTEGER NOT NULL,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    UNIQUE (organization_id, team_id, state_id),
    UNIQUE (organization_id, team_id, name),
    CHECK ((archived AND archived_at IS NOT NULL) OR (NOT archived AND archived_at IS NULL)),
    FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, team_id) ON DELETE CASCADE
);

ALTER TABLE teams ADD CONSTRAINT teams_default_workflow_state_fk
    FOREIGN KEY (organization_id, team_id, default_workflow_state_id)
    REFERENCES workflow_states(organization_id, team_id, state_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE project_workspaces (
    organization_id TEXT PRIMARY KEY,
    default_project_status_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE project_statuses (
    status_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('backlog','planned','started','paused','completed','canceled')),
    color TEXT NOT NULL,
    position INTEGER NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    UNIQUE (organization_id, status_id),
    UNIQUE (organization_id, name),
    CHECK ((archived AND archived_at IS NOT NULL) OR (NOT archived AND archived_at IS NULL)),
    CHECK (NOT is_default OR category = 'backlog'),
    CHECK (NOT is_default OR NOT archived)
);

CREATE UNIQUE INDEX project_statuses_one_default
    ON project_statuses (organization_id)
    WHERE is_default;

ALTER TABLE project_workspaces ADD CONSTRAINT project_workspaces_default_status_fk
    FOREIGN KEY (organization_id, default_project_status_id)
    REFERENCES project_statuses(organization_id, status_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE projects (
    project_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    name TEXT NOT NULL,
    summary TEXT,
    lead_team_id TEXT NOT NULL,
    status_id TEXT NOT NULL,
    milestone_id TEXT,
    starts_on DATE,
    target_date DATE,
    completed_at TIMESTAMPTZ,
    canceled_at TIMESTAMPTZ,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (organization_id, project_id),
    CHECK (completed_at IS NULL OR canceled_at IS NULL),
    FOREIGN KEY (organization_id, lead_team_id) REFERENCES teams(organization_id, team_id),
    FOREIGN KEY (organization_id, status_id) REFERENCES project_statuses(organization_id, status_id)
);

CREATE TABLE project_teams (
    organization_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    PRIMARY KEY (organization_id, project_id, team_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, project_id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, team_id)
);

CREATE TABLE cycles (
    cycle_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    number INTEGER NOT NULL CHECK (number > 0),
    name TEXT,
    starts_on DATE NOT NULL,
    ends_on DATE NOT NULL CHECK (ends_on >= starts_on),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    UNIQUE (organization_id, team_id, cycle_id),
    UNIQUE (organization_id, team_id, number),
    FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, team_id) ON DELETE CASCADE
);

CREATE TABLE milestones (
    milestone_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    target_date DATE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    UNIQUE (organization_id, project_id, milestone_id),
    UNIQUE (organization_id, project_id, name),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, project_id) ON DELETE CASCADE
);

ALTER TABLE projects ADD CONSTRAINT projects_milestone_fk
    FOREIGN KEY (organization_id, project_id, milestone_id)
    REFERENCES milestones(organization_id, project_id, milestone_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE labels (
    label_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    team_id TEXT,
    name TEXT NOT NULL,
    description TEXT,
    color TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    UNIQUE (organization_id, label_id),
    UNIQUE NULLS NOT DISTINCT (organization_id, team_id, name),
    FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, team_id)
);

CREATE TABLE issues (
    issue_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    identifier TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    priority TEXT NOT NULL CHECK (priority IN ('none','urgent','high','medium','low')),
    workflow_state_id TEXT NOT NULL,
    cycle_id TEXT,
    milestone_id TEXT,
    parent_issue_id TEXT,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (organization_id, issue_id),
    UNIQUE (organization_id, identifier),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, project_id),
    FOREIGN KEY (organization_id, team_id) REFERENCES teams(organization_id, team_id),
    FOREIGN KEY (organization_id, project_id, team_id)
        REFERENCES project_teams(organization_id, project_id, team_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (organization_id, team_id, workflow_state_id) REFERENCES workflow_states(organization_id, team_id, state_id),
    FOREIGN KEY (organization_id, team_id, cycle_id) REFERENCES cycles(organization_id, team_id, cycle_id),
    FOREIGN KEY (organization_id, project_id, milestone_id) REFERENCES milestones(organization_id, project_id, milestone_id),
    FOREIGN KEY (organization_id, parent_issue_id) REFERENCES issues(organization_id, issue_id),
    CHECK (parent_issue_id IS NULL OR parent_issue_id <> issue_id)
);

CREATE TABLE issue_identifier_aliases (
    organization_id TEXT NOT NULL,
    identifier TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (organization_id, identifier),
    FOREIGN KEY (organization_id, issue_id) REFERENCES issues(organization_id, issue_id) ON DELETE CASCADE
);

CREATE TABLE issue_labels (
    organization_id TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (organization_id, issue_id, label_id),
    FOREIGN KEY (organization_id, issue_id) REFERENCES issues(organization_id, issue_id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, label_id) REFERENCES labels(organization_id, label_id)
);

CREATE TABLE comments (
    comment_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    author_subject TEXT NOT NULL,
    body TEXT NOT NULL,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (organization_id, comment_id),
    FOREIGN KEY (organization_id, issue_id) REFERENCES issues(organization_id, issue_id) ON DELETE CASCADE
);

CREATE TABLE project_updates (
    update_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    author_subject TEXT NOT NULL,
    body TEXT NOT NULL,
    health TEXT NOT NULL CHECK (health IN ('on_track','at_risk','off_track')),
    row_seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (organization_id, update_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, project_id) ON DELETE CASCADE
);

CREATE TABLE issue_relations (
    relation_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    related_issue_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('blocks','blocked_by','duplicate','related')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (organization_id, relation_id),
    UNIQUE (organization_id, issue_id, related_issue_id, kind),
    FOREIGN KEY (organization_id, issue_id) REFERENCES issues(organization_id, issue_id) ON DELETE CASCADE,
    FOREIGN KEY (organization_id, related_issue_id) REFERENCES issues(organization_id, issue_id) ON DELETE CASCADE,
    CHECK (issue_id <> related_issue_id)
);

CREATE TABLE issue_external_links (
    organization_id TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    external_key TEXT NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (organization_id, provider, external_key),
    UNIQUE (organization_id, issue_id, provider, external_key),
    FOREIGN KEY (organization_id, issue_id) REFERENCES issues(organization_id, issue_id) ON DELETE CASCADE
);

CREATE TABLE project_activity (
    activity_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    organization_id TEXT NOT NULL,
    project_id TEXT,
    issue_id TEXT,
    actor_subject TEXT NOT NULL,
    operation TEXT NOT NULL,
    entity_kind TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    revision BIGINT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, project_id),
    FOREIGN KEY (organization_id, issue_id) REFERENCES issues(organization_id, issue_id)
);

CREATE TABLE project_retention_receipts (
    action_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('delete','anonymize')),
    receipt TEXT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE INDEX projects_list_idx ON projects(organization_id, row_seq);
CREATE INDEX project_teams_team_idx ON project_teams(organization_id, team_id, project_id);
CREATE INDEX issues_list_idx ON issues(organization_id, row_seq);
CREATE INDEX issues_project_idx ON issues(organization_id, project_id, row_seq);
CREATE INDEX issues_team_idx ON issues(organization_id, team_id, row_seq);
CREATE INDEX comments_issue_idx ON comments(organization_id, issue_id, row_seq);
CREATE INDEX updates_project_idx ON project_updates(organization_id, project_id, row_seq);
CREATE INDEX activity_scope_idx ON project_activity(organization_id, activity_id);
CREATE INDEX activity_project_idx ON project_activity(organization_id, project_id, activity_id);
CREATE INDEX activity_issue_idx ON project_activity(organization_id, issue_id, activity_id);
