#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-capability-projects\nlenso-capability-projects-admin\nlenso-capability-projects-collaboration\nlenso-projects-postgres-plugin'
actual_crates="$({
  find crates -mindepth 1 -maxdepth 1 -type d -exec basename {} \;
} | LC_ALL=C sort)"

if [[ "$actual_crates" != "$expected_crates" ]]; then
  printf 'unexpected crate ownership:\n%s\n' "$actual_crates" >&2
  exit 1
fi

if rg -n \
  '^[[:space:]]*lenso-(contracts|module-auth|platform-(core|http|module|runtime|testing))[[:space:]]*=' \
  Cargo.toml crates --glob 'Cargo.toml'; then
  printf 'legacy Lenso dependency returned\n' >&2
  exit 1
fi

if rg -n \
  'sqlx|postgres|lenso-postgres-kit|lenso-capability-secrets|lenso-capability-access-control' \
  crates/lenso-capability-projects*/Cargo.toml crates/lenso-capability-projects*/src \
  --glob '!**/generated.rs'; then
  printf 'portable Projects Capability gained an implementation dependency\n' >&2
  exit 1
fi

if rg -ni \
  'octocrab|github[_-](client|installation|repository)|CREATE TABLE github' \
  crates --glob '*.rs' --glob '*.sql' --glob '*.json'; then
  printf 'Projects directly coupled to GitHub instead of its external-link/activity boundary\n' >&2
  exit 1
fi

if rg -n 'not_supported|not supported' crates --glob '*.rs'; then
  printf 'declared operation contains a not-supported stub\n' >&2
  exit 1
fi

printf 'repository boundary is Projects-owned and integration-neutral\n'
