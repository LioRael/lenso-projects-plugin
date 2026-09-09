# ADR 0001: Publish static Agent Tool descriptions independently of login

Status: accepted

Agent prepares its immutable Tool routes before a user connects a business account.
Making all descriptions depend on login produces an empty initial route table;
restarting to discover Tools also discards the in-memory connection.

The removable Projects Agent Web Plugin therefore exposes a separate public
`GET /projects/agent/manifest` endpoint. It projects the bound Projects Tool
adapter's static names, descriptions, schemas and scheduling classifications.
App composition must bind the Projects adapter, whose catalog contains no user,
resource or permission data. The authenticated catalog route remains available.

This endpoint conveys no authorization. Every execution still authenticates its
selected credential and checks its exact operation audience, Organization
membership, scoped permissions and private Team visibility. Neither discovery nor
Agent Profile approval widens a delegated grant. Secret or per-user descriptions
must never be added to this public projection; such a future requirement needs a
separate authenticated discovery design.
