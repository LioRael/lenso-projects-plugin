# Issue assignment

Projects Collaboration 1.1 adds `get_issue_assignee` and `set_issue_assignee`.
Each Issue has one nullable subject assignment. Existing Issue request and
response shapes remain unchanged; assignment shares the Issue revision.

The provider authorizes reads with `projects.read` and writes with
`projects.write`, verifies active organization membership, and checks that the
selected member can see the Issue in private Teams and Projects. Caller-supplied
identity never replaces the authenticated actor assertion. Mutations require an
expected revision and idempotency key, and append an activity event.

Postgres migration 002 adds the nullable column. Subject export includes assigned
Issue IDs; subject retention clears assignments and advances affected revisions.
The Agent adapter exposes the same typed operations and authorization boundary.

Console and Web changes require this additive contract and provider version.
Local acceptance uses source patches; publish the contract, provider and tool
adapter before releasing dependent Web and Console packages. No registry release
is implied by the local acceptance results.
