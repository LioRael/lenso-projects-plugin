# Projects Agent HTTP ingress

This removable linked Plugin exposes one bound Projects Tool Provider through
`lenso.http.endpoint@1`. Removing it removes the Agent HTTP routes, while Projects
and its existing Tool Provider continue to work. It owns transport projection only;
Account Auth owns sessions, and Projects retains membership, RBAC, private Team,
revision and idempotency enforcement.

Bind its Auth requirement to the application's Account Auth provider and its Tool
Provider requirement to `lenso.projects.agent-tools`. The Web ingress must select a
session credential from its configured native-client authorization header policy;
the endpoint does not accept an actor or credential in JSON. Cookie clients remain
subject to the Web ingress's CSRF policy. Authentication runs for every catalog and
execution request, so revoked or expired sessions cannot reuse an earlier assertion.

- `GET /projects/agent/manifest` publicly returns only static Tool descriptions and
  schemas from the bound Projects Tools adapter, with no user or resource data. This
  allows Agent Generation preparation before login. It grants no execution access.
- `GET /projects/agent/tools` returns the generated Tool Provider catalog.
- `POST /projects/agent/tools/execute` accepts only `name` and `arguments_json`.
  `arguments_json` is a JSON-encoded string matching the selected Tool schema.
- Success returns the generated Tool Provider execution response. Domain failures
  preserve their generated error body with HTTP 400, 403, 404, 413 or 422; runtime
  failures remain runtime failures for the enclosing HTTP adapter.
- Responses use `Cache-Control: no-store`. Request and response bodies are bounded.
  No operation is automatically retried and no subject is supplied by the client.

A child Account grant must contain the exact underlying Projects/Collaboration
operation audiences. The Tool adapter preserves the authenticated invocation context
when invoking those contracts. The grant cannot replace final resource authorization.

The integration test runs the generated endpoint and generated Auth/Tool clients in
a native Kernel with signed test assertions. It covers rejected credentials, user-only
identity, actor forwarding, extra JSON fields, revocation and honest runtime failures.
It is not a real browser-login or live-model acceptance test. Agent-side connection
custody and the production business App assembly still need to consume these routes.
