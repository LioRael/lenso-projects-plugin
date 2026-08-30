# Lenso Projects Plugin

PostgreSQL-owned, Linear-like project planning for Lenso.

The repository publishes three portable contracts and one linked native
Plugin. The backend covers Team-scoped workflow, multi-Team Projects, stable
Issue identifiers with historical lookup, Cycles, Milestones, Labels,
relations, comments, project updates, external links, revision-safe mutation,
keyset pagination, and a transactional activity stream.

The Plugin performs final authorization: signed Actor Assertions are checked
for the exact operation, Organization Membership and independent RBAC are
consulted at invocation time, and private Team visibility is enforced locally.

Creating a Team atomically creates Backlog, Todo, In Progress, Done, and
Canceled Issue workflow states; Todo is its required unarchived `unstarted`
default. Each Organization receives a separate Project Status catalog with
Backlog, Planned, In Progress, Paused, Completed, and Canceled; Backlog is the
default. Names and ordering remain editable, while stable IDs and categories
drive behavior. Default, archived, cross-Team, and actively referenced catalog
entries fail closed. Both catalogs provide get/list, revision-safe put,
archive/unarchive, protected hard delete, and atomic full-active-catalog reorder
with per-entry CAS.

External synchronizers consume the integration-neutral boundary: use
`list_activity` with its durable monotonic high-water cursor, then re-read an
Issue by stable `issue_id`; historical display identifiers continue to resolve.
`put_external_link(provider, external_key)` records the remote association.
Activity allocation is commit-ordered so a checkpoint cannot skip a lower
uncommitted record. Cursors are scoped to the same actor visibility. No GitHub
client, credential, webhook, or repository fact is owned here.

See [the Plugin card](docs/plugin-card.md) for boundaries and invariants.

## Verification

```sh
lenso-contract-codegen check crates/lenso-capability-projects/capability.json --rust crates/lenso-capability-projects/src/generated.rs
lenso-contract-codegen check crates/lenso-capability-projects-collaboration/capability.json --rust crates/lenso-capability-projects-collaboration/src/generated.rs
lenso-contract-codegen check crates/lenso-capability-projects-admin/capability.json --rust crates/lenso-capability-projects-admin/src/generated.rs
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
./scripts/check-repository-boundary.sh
```

The concurrency/restart acceptance test requires a dedicated database:

```sh
LENSO_PROJECTS_TEST_DATABASE_URL=postgres://localhost/lenso_projects_test \
  cargo test --locked -p lenso-projects-postgres-plugin \
  --features postgres-acceptance concurrent_idempotency_identifier_history_activity_and_restart
```
