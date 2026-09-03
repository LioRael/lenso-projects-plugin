# Plugin card

## User job

Plan work from projects into stable team issue identifiers, coordinate through
comments and updates, and preserve a trustworthy activity trail under
concurrent edits.

## Ownership and deletion boundary

The Plugin owns Teams and their local visibility membership, workflow states,
project statuses, Projects, project-to-Team membership, Issues and identifier
history, Cycles, Milestones, Labels, comments, project updates, issue
relations, external links, mutation receipts, and activity records.

Deleting the Plugin removes all of those facts. It does not remove
Organizations, canonical subjects, Organization membership, RBAC policy,
credentials, or external GitHub objects.

## Capabilities

Provides:

- `lenso.projects@1`
- `lenso.projects-collaboration@1`
- `lenso.projects-admin@1`
- `lenso.data-export-source@1`
- `lenso.retention-participant@1`

Requires exactly one provider of:

- `lenso.organization-membership@1`
- `lenso.access-control@1`
- `lenso.secrets@1`

The separate `lenso.projects.agent-tools` adapter provides
`lenso.agent.tool-provider@2` and requires exactly one provider of both
`lenso.projects@1` and `lenso.projects-collaboration@1`. It owns only Agent
catalog and argument/result adaptation. Removing it removes the Agent surface
without removing Projects facts or changing the business Capabilities.

## Authorization

Every product operation first admits an immutable caller Instance, verifies an
Auth-issued Actor Assertion for the exact Capability and operation, checks
active Organization membership, and asks Access Control for the operation's
permission. Private-Team visibility is then enforced from Plugin-owned Team
membership. Governance operations use a separate caller allowlist.

## Consistency

Mutable aggregates have positive revisions. Mutations use
`expected_revision`; commands are idempotent under
`(caller_instance, actor_subject, operation, idempotency_key)`. Issue
identifiers are allocated monotonically per Team and all old identifiers remain
readable after a move. Transactions append an activity record before commit.

## Default catalogs

Team creation and its five default Issue workflow states commit atomically.
The Team default always identifies its own unarchived `unstarted` state. An
Issue create request with no explicit state uses that exact ID. State category
is stable; name, color, and position can change under revision/CAS. A default
or referenced state cannot be archived. The catalog exposes put/get/list,
archive/unarchive, hard delete, and an atomic full-active-catalog reorder. Every
reorder entry carries its own expected revision; duplicate IDs or positions,
cross-Team entries, partial catalogs, or one stale entry roll back the batch.
Hard delete is limited to archived, non-default, unreferenced states.

The Organization Project Status catalog is independent from Team workflows.
It initializes six statuses and marks Backlog as the non-archivable default.
A Project create request with no explicit status uses that ID. Entering a
`completed` or `canceled` category sets the corresponding timestamp; leaving
it clears the timestamp. Project Status exposes the same CRUD, archive, and
atomic full-active-catalog reorder guarantees at Organization scope. Referenced
or default statuses cannot be archived, and only archived, non-default,
unreferenced statuses can be hard deleted.

## Integration boundary

`project_activity.activity_id` is an immutable, monotonic PostgreSQL cursor.
`list_activity(after)` returns only committed visible Project/Issue changes and
always returns the latest durable checkpoint, even at the current end. An
integration should checkpoint it, re-read the referenced aggregate by stable
ID, write its association through `put_external_link`, and retry mutations with
the same caller, actor, operation, and idempotency key. Collection `row_seq`
cursors are pagination only and are not a change feed.

Activity allocation is serialized through a transaction-scoped, schema-local
commit gate. Therefore a returned cursor cannot leapfrog a lower uncommitted
activity ID. The checkpoint is scoped to the same Organization, actor, and
visibility grants; a newly granted private Team requires an intentional
backfill from that Team rather than replaying an old actor-scoped cursor.

This boundary is provider-neutral. A GitHub sync Plugin can consume it without
Projects owning GitHub tokens, installations, webhooks, repositories, or API
types.
