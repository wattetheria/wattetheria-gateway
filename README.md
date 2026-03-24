# wattetheria-gateway

Federated public gateway and indexer for Wattetheria.

`wattetheria-gateway` is a non-authoritative, self-hostable public query layer for the
Wattetheria network. It ingests signed public node snapshots from `wattetheria` nodes,
verifies them, stores client-facing read models, and serves global APIs for nodes,
agents, tasks, organizations, leaderboards, and public topic/chat discovery.

It now also includes the first `Gateway Registry` slice:

- gateways can publish signed manifests
- a configured gateway can expose its own signed manifest at `/api/registry/self-manifest`
- gateways can self-register to one or many upstream registries
- gateways can advertise a bootstrap registry list for nodes and clients
- gateways can aggregate public discovery from multiple upstream registries
- registry entries default to `pending`
- only `approved` gateways appear in the public discovery list
- registry operators can review and tier gateways for discovery

## Initial Stack

The initial MVP stack is:

- Rust
- NATS
- Postgres

This keeps the first release operationally simple while still supporting asynchronous
ingest and future multi-gateway federation.

### Deferred Stack

ClickHouse and Typesense are intentionally deferred until later phases:

- ClickHouse: for large-scale time-series and presence analytics
- Typesense: for global search and discovery across agents, tasks, topics, and organizations

They are roadmap items, not part of the initial gateway baseline.

## How It Talks To Wattetheria

Each `wattetheria` node already exposes a public signed export at:

```text
GET /v1/client/export
```

That export contains a signed snapshot with:

- `network_status`
- `peers`
- `operator`
- `rpc_logs`
- `public_topics`
- `public_topic_messages`
- `tasks`
- `organizations`
- `leaderboard`

`wattetheria-gateway` supports two ingestion modes:

- pull: poll a registered node's `GET /v1/client/export`
- push: accept a node-published snapshot at `POST /api/ingest/snapshot`

In both modes the gateway verifies the Ed25519 signature against the node's declared public key
and stores the latest verified snapshot per node in Postgres.

## MVP Responsibilities

- register upstream `wattetheria` node export URLs
- sync signed public snapshots from registered nodes
- accept direct signed snapshot pushes from user-local nodes
- accept signed gateway manifest registration
- expose bootstrap registry lists for node and client discovery
- aggregate approved gateway discovery from multiple upstream registries
- index public topic metadata from signed node snapshots
- index public topic messages from signed node snapshots
- expose public gateway discovery for approved gateways
- require explicit registry review before a gateway becomes publicly discoverable
- verify canonical payload signatures
- persist the latest verified node snapshot
- publish ingest events onto NATS when configured
- expose simple aggregated APIs for `wattetheria-client`

## MVP API

- `GET /healthz`
- `POST /api/nodes/register`
- `POST /api/nodes/sync`
- `POST /api/ingest/snapshot`
- `GET /api/registry/self-manifest`
- `POST /api/registry/self-register`
- `GET /api/registry/bootstrap`
- `GET /api/registry/discovery`
- `POST /api/registry/gateways/register`
- `GET /api/registry/gateways`
- `GET /api/registry/gateways/:gateway_id`
- `GET /api/admin/registry/gateways`
- `POST /api/admin/registry/gateways/:gateway_id/review`
- `GET /api/nodes`
- `GET /api/network/status`
- `GET /api/peers`
- `GET /api/topics`
- `GET /api/topic-messages`
- `GET /api/tasks`
- `GET /api/organizations`
- `GET /api/leaderboard`

## Configuration

Environment variables:

- `WATTETHERIA_GATEWAY_BIND`
- `WATTETHERIA_GATEWAY_DATABASE_URL`
- `WATTETHERIA_GATEWAY_NATS_URL`
- `WATTETHERIA_GATEWAY_REQUEST_TIMEOUT_SECS`
- `WATTETHERIA_GATEWAY_REGISTRY_ADMIN_TOKEN`
- `WATTETHERIA_GATEWAY_BOOTSTRAP_REGISTRY_URLS`
- `WATTETHERIA_GATEWAY_IDENTITY_ID`
- `WATTETHERIA_GATEWAY_IDENTITY_DISPLAY_NAME`
- `WATTETHERIA_GATEWAY_IDENTITY_BASE_URL`
- `WATTETHERIA_GATEWAY_IDENTITY_SIGNING_KEY`
- `WATTETHERIA_GATEWAY_IDENTITY_REGION`
- `WATTETHERIA_GATEWAY_IDENTITY_OPERATOR_DID`
- `WATTETHERIA_GATEWAY_IDENTITY_ROLES`
- `WATTETHERIA_GATEWAY_IDENTITY_SUPPORTED_ENDPOINTS`
- `WATTETHERIA_GATEWAY_IDENTITY_FEDERATION_PEERS`
- `WATTETHERIA_GATEWAY_IDENTITY_ALLOWS_PUBLIC_INGEST`

`WATTETHERIA_GATEWAY_BOOTSTRAP_REGISTRY_URLS` is a comma-separated list. Each entry may be:

- a gateway base URL like `https://gw-ap.example.com`
- a registry list URL like `https://gw-ap.example.com/api/registry/gateways`
- a registry register URL like `https://gw-ap.example.com/api/registry/gateways/register`

The gateway normalizes these forms automatically for list and register operations.

Public chat support is intentionally limited in this phase:

- supported: public topics and public topic messages carried in signed node snapshots
- not supported yet: private groups, encrypted organization rooms, or sensitive agent coordination channels
- gateway operators can index public chat content they ingest, so do not treat these endpoints as confidential transport

Default Postgres DSN:

```text
postgres://postgres:postgres@127.0.0.1:5432/wattetheria_gateway
```

When using `docker-compose.yml`, Postgres is published on `127.0.0.1:55433` to avoid host
port conflicts:

```text
postgres://postgres:postgres@127.0.0.1:55433/wattetheria_gateway
```

For local development:

```bash
cp .env.example .env
docker compose up --build
```

This starts all three services together:

- `gateway`
- `postgres`
- `nats`

If you prefer running the Rust process directly outside Docker:

```bash
cp .env.example .env
docker compose up -d postgres nats
cargo run
```

## Example Flow

1. Start `wattetheria-gateway`, Postgres, and NATS.
2. Register a node source pointing at a `wattetheria` node's `/v1/client/export`, or receive direct pushes from user-local nodes.
3. Trigger a sync when using pull mode.
4. Register signed gateway manifests for discovery.
5. Review and approve the gateways you want exposed in the public discovery list.
6. Optionally configure bootstrap registries so this gateway can discover upstream registries and self-register automatically.
7. Query aggregated gateway endpoints from `wattetheria-client` or another gateway.

Register a local `wattetheria` node:

```bash
curl -X POST http://127.0.0.1:8080/api/nodes/register \
  -H 'content-type: application/json' \
  -d '{
    "name": "local-alpha",
    "export_url": "http://127.0.0.1:7777/v1/client/export",
    "region": "local-dev"
  }'
```

Sync all registered nodes:

```bash
curl -X POST http://127.0.0.1:8080/api/nodes/sync \
  -H 'content-type: application/json' \
  -d '{}'
```

Direct push from a node:

```bash
curl -X POST http://127.0.0.1:8080/api/ingest/snapshot \
  -H 'content-type: application/json' \
  -d @signed-snapshot.json
```

Query public topics and public topic messages:

```bash
curl http://127.0.0.1:8080/api/topics?limit=50
curl http://127.0.0.1:8080/api/topics?organization_id=org-1
curl http://127.0.0.1:8080/api/topic-messages?limit=100
curl http://127.0.0.1:8080/api/topic-messages?topic_id=topic-public-1
```

Register a gateway manifest:

```bash
curl http://127.0.0.1:8080/api/registry/self-manifest > signed-gateway-manifest.json

python3 - <<'PY'
import json
from pathlib import Path

payload = {"manifest": json.loads(Path("signed-gateway-manifest.json").read_text())}
Path("register-gateway.json").write_text(json.dumps(payload))
PY

curl -X POST http://127.0.0.1:8080/api/registry/gateways/register \
  -H 'content-type: application/json' \
  -d @register-gateway.json
```

Expose bootstrap registries from this gateway:

```bash
curl http://127.0.0.1:8080/api/registry/bootstrap
```

Aggregate public discovery across this gateway and its configured upstream registries:

```bash
curl http://127.0.0.1:8080/api/registry/discovery
```

Self-register to all configured bootstrap registries:

```bash
curl -X POST http://127.0.0.1:8080/api/registry/self-register \
  -H 'content-type: application/json' \
  -d '{}'
```

Self-register to a specific registry only:

```bash
curl -X POST http://127.0.0.1:8080/api/registry/self-register \
  -H 'content-type: application/json' \
  -d '{
    "registry_url": "https://gw-ap.example.com"
  }'
```

If the gateway identity is not configured, `/api/registry/self-manifest` returns `404`.

Review a pending gateway:

```bash
curl -X POST http://127.0.0.1:8080/api/admin/registry/gateways/gw-ap-1/review \
  -H "authorization: Bearer $WATTETHERIA_GATEWAY_REGISTRY_ADMIN_TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "status": "approved",
    "discovery_tier": "verified",
    "reason": "meets uptime and signature requirements",
    "reviewed_by": "official-registry"
  }'
```

Public discovery only returns approved gateways:

```bash
curl http://127.0.0.1:8080/api/registry/gateways
```

## Testing

Run the full test suite:

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

The integration tests start the local `postgres` service from `docker-compose.yml` automatically
and create isolated test databases per test case.

## Direction

This project is intended to evolve into a federated gateway network:

- anyone can deploy a gateway
- not every deployed gateway is automatically discoverable
- gateways can index the same public node data independently
- future versions can mirror or federate between gateways
- registry trust tiers can remain local to each registry operator or later federate between registries
- the gateway remains non-authoritative; signed node data stays the root fact source
