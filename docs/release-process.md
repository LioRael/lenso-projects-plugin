# Release process

Releases are produced only from a clean `main` branch by
`.github/workflows/release-plz.yml`. The workflow uses GitHub OIDC Trusted
Publishing and deliberately has no crates.io token fallback.

Before enabling automatic publishing, allocate each crate name once on
crates.io, then configure Trusted Publishing for owner `LioRael`, repository
`lenso-projects-plugin`, workflow `release-plz.yml`, with no GitHub
environment restriction.

Publish order is: `lenso-capability-projects`,
`lenso-capability-projects-collaboration`, `lenso-capability-projects-admin`,
then `lenso-projects-postgres-plugin`.
