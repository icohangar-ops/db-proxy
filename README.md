# db-proxy

`db-proxy` is a Rust REST gateway for the CockroachDB-backed Cubiczan financial app portfolio. It preserves the original Forge-friendly JSON API while moving the runtime from Python/FastAPI to a compiled Axum service with stronger startup behavior, lower memory overhead, and no baked-in database password.

## What it exposes

- `GET /health` - checks configured CockroachDB databases and returns `healthy` or `degraded`
- `GET /api/databases` - lists configured databases, tables, column counts, and row counts
- `GET /api/{database}/tables` - lists tables and column metadata for one database
- `GET /api/{database}/{table}?limit=50&offset=0` - paginated table rows
- `GET /api/{database}/{table}/{record_id}` - fetches one row by detected primary key
- `POST /api/{database}/{table}` - inserts a row from `{ "data": { ... } }`
- `GET /api/market-radar` - market radar aggregate with mock fallback
- `GET /api/finance-cockpit` - finance cockpit aggregate with mock fallback
- `GET /api/decision-brief/{id}` - decision brief aggregate with mock fallback
- `GET /api/battery-erp-dashboard` - battery ERP aggregate with mock fallback

## Configuration

| Variable | Default | Notes |
| --- | --- | --- |
| `PORT` | `8080` | HTTP listen port |
| `API_KEY` | unset | When set, protected routes require `X-API-Key` |
| `COCKROACH_HOST` | Cubiczan Cockroach Cloud host | Database host |
| `COCKROACH_PORT` | `26257` | Database port |
| `COCKROACH_USER` | `cubiczan` | Database user |
| `COCKROACH_PASSWORD` | unset | Required for live DB access |
| `COCKROACH_SSL` | `require` | Use `disable` only for local development |

No secrets are committed. `COCKROACH_PASSWORD` must be supplied through the deployment environment or secret manager.

## Local development

```bash
cargo fmt
cargo test
cargo run
```

With Docker:

```bash
docker compose up --build
```

Protected route example:

```bash
curl -H "X-API-Key: $API_KEY" http://localhost:8080/api/databases
```

## Rust rewrite notes

The Rust version keeps the external API stable while adding:

- strict database/table/column identifier validation before dynamic SQL is generated
- TLS-enabled CockroachDB access through `tokio-postgres`
- graceful degradation for dashboard aggregate endpoints when live database access is unavailable
- bounded pagination (`limit` is clamped to `1..=1000`)
- compiled binary deployment for Docker, Railway, Render, and Fly

## CHP Governance

This repository is hardened with the [Consensus Hardening Protocol (CHP)](https://codeberg.org/cubiczan/consensus-hardening-protocol), Cubiczan's decision-governance layer for multi-agent AI systems.

### Protocol Layers

- **R0 Gate**: All decisions must pass Solvable, Scoped, Valid, Worth_it checks
- **Foundation Disclosure**: 1-3 weakest assumptions, 1-2 invalidation conditions, 1 key vulnerability
- **Adversarial Layer**: Mandatory challenge at Phase 0 and implementation review
- **State Machine**: EXPLORING -> PROVISIONAL -> PROVISIONAL_LOCK -> LOCKED
- **Third-Party Validation**: Independent CONFIRM/REJECT before lock

### Domain Configuration

- **Category**: Tools / Utilities
- **Foundation Threshold**: 70
- **CFO Accuracy Guard**: Disabled

### Compliance Artifacts

| File | Purpose |
| --- | --- |
| `.chp/STATE_MACHINE.md` | Decision state transitions |
| `.chp/R0_CONFIG.yaml` | Domain-calibrated thresholds |
| `.chp/ADVERSARIAL_PROMPTS.md` | Standardized challenge templates |
| `.chp/CHP_COMPLIANCE.md` | Compliance tracking and audit trail |

### CHP Version

cognitive-mesh-orchestrator 0.1.0 | [Protocol Docs](https://codeberg.org/cubiczan/consensus-hardening-protocol)
