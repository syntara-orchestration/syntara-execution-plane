# EP Read Endpoints

EP exposes read-only HTTP endpoints for inspecting the resources it manages. These are distinct from the command system, which handles all state mutations via `POST /v1/commands`. Reads are proxied through Syntara (AO), which enforces access control and surfaces a curated subset of EP fields at `/api/execution_plane/v1/`.

## Resources exposed via the AO facade

### Work Items

AO endpoint: `GET /api/execution_plane/v1/work_items`

| Field | Type |
|---|---|
| `id` | UUID |
| `status` | string |
| `result` | object \| null |
| `created_at` | datetime |
| `claimed_at` | datetime \| null |
| `completed_at` | datetime \| null |

Work item statuses: `pending`, `claimed`, `dispatched`, `cancel_requested`, `reconciliation_required`, `completed`, `failed`, `cancelled`.

### Execution Targets

AO endpoint: `GET /api/execution_plane/v1/execution_targets`

| Field | Type |
|---|---|
| `id` | UUID |
| `cluster_id` | UUID |
| `name` | string |
| `backend_type` | string |
| `placement` | `{"type": "kubernetes", ...}` or `{"type": "rhel"}` |
| `status` | string |
| `enabled` | bool |
| `is_default` | bool |
| `labels` | `{string: string}` |
| `created_at` | datetime |
| `last_ran_at` | datetime \| null |

EP also has a `/v1/clusters` endpoint that is not currently proxied through AO.

## Pagination

EP list endpoints mirror Syntara's own pagination contract — AO passes EP cursors through unchanged. See the [AO API response format standard](https://github.com/syntara-orchestration/syntara/blob/devel/backend/docs/standards/api-response-format.md) for cursor structure, stable ordering guarantees, and the N+1 fetch pattern.

Query parameters: `limit` (1–100, default 20), `cursor`, `sort` (`field` or `-field` for descending, default `-created_at`), `include_total`.

Sortable fields for work items: `created_at`, `status`, `resource_cleanup_status`, `claimed_at`, `completed_at`, `id`.

```
GET /api/execution_plane/v1/work_items?limit=50&sort=status
GET /api/execution_plane/v1/work_items?limit=20&cursor=<token from previous response>
```

## OpenAPI spec

The read endpoints described here and the command ingestion endpoint (`POST /v1/commands`) share a single spec file committed at [`openapi.yaml`](../openapi.yaml) — there is no separate spec for reads vs. commands. The spec is generated automatically by FastAPI from route decorators and Pydantic type annotations.
