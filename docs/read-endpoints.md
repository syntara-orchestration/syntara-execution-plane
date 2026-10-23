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

Work item statuses: `pending`, `claimed`, `dispatched`, `running`, `cancel_requested`, `reconciliation_required`, `completed`, `failed`, `cancelled`.

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
Cluster reads include the binding's `is_default` desired-state field.

## Pagination

EP list endpoints use Syntara's pagination response envelope. The AO facade can forward EP cursors unchanged. See the [AO API response format standard](https://github.com/syntara-orchestration/syntara/blob/devel/backend/docs/standards/api-response-format.md) for cursor structure, stable ordering guarantees, and the N+1 fetch pattern.

Query parameters: `limit` (1–100, default 20), `cursor`, `sort` (`field` or `-field` for descending), `include_total`.

Work items default to `-created_at` (newest first). Sortable fields are `created_at`, `status`, `resource_cleanup_status`, and `id`. Nullable timestamps `claimed_at` and `completed_at` are not sortable.

Clusters and execution targets default to `created_at` (oldest first). All sortable fields accept an optional `-` prefix for descending order:

| Endpoint | Sortable fields |
|---|---|
| `/v1/work-items` | `id`, `created_at`, `status`, `resource_cleanup_status` |
| `/v1/execution-targets` | `id`, `created_at`, `name`, `endpoint`, `status`, `backend_type`, `cluster_id`, `enabled`, `is_default` |
| `/v1/clusters` | `id`, `created_at`, `name`, `endpoint`, `status`, `enabled`, `namespace`, `desired_revision`, `observed_revision`, `updated_at` |

Nullable fields are filterable but not sortable. Unsupported sorts return HTTP 422. Every sort uses the resource UUID as a tie-breaker, including boolean and revision-number sorts.

Follow `next` or `prev` to navigate in either direction. Keep the same filters, `sort`, and `limit` when following a cursor; its sort field and direction must match the requested sort (including the endpoint default when `sort` is omitted). A missing page in either direction is represented by a null cursor. Totals are computed only when `include_total=true` and count all matching resources owned by the authenticated client, independently of the cursor and page limit.

Cursors are opaque tokens; callers should forward them unchanged. EP continues to emit base64-encoded JSON objects containing string fields `id`, `created_at`, `direction`, `sort_field`, `sort_direction`, and `sort_value`. When sorting by `created_at`, EP also accepts Syntara's cursor format without `sort_value` and uses `created_at` as the anchor. Other sorts require `sort_value`. Invalid or otherwise incomplete cursors, including timestamps without a timezone, return HTTP 422. Unknown cursor fields are ignored.

```
GET /v1/work-items?limit=50&sort=status
GET /v1/work-items?limit=20&cursor=<token from previous response>
```

## Filtering

EP ports `syntara.core.utils.filters.parse_filters` / `apply_filters` and `syntara.core.utils.labels` with origin references in the implementation docstrings. Filters use Syntara's bracket syntax (`id[in]`, not `id__in`). Unknown fields, unsupported operators, invalid typed values, and attempts to filter credentials or client ownership return HTTP 422.

| Endpoint | Filterable fields |
|---|---|
| `/v1/work-items` | `id`, `status`, `created_at`, `claimed_at`, `completed_at`, `resource_cleanup_status`, `resource_cleanup_error` |
| `/v1/execution-targets` | All sortable fields above, plus `last_ran_at`, `status_message`, and `labels[key]` |
| `/v1/clusters` | All sortable fields above, plus `cluster_id`, `status_message`, and `labels[key]` |

`/v1/clusters` lists cluster bindings: its public `id` filters/sorts the `source_integration_id` column, and `desired_revision` maps to `revision`. Its nullable `cluster_id` is the separate physical cluster identifier.

| Syntax | Meaning |
|---|---|
| `field=value` or `field[eq]=value` | Equality |
| `id[in]=<uuid1>,<uuid2>` | Match any listed UUID |
| `status=pending,failed` | Match either value |
| `name[contains]=worker` | Case-insensitive literal substring; `%` and `_` are escaped |
| `name[starts_with]=prod` | Case-insensitive literal prefix |
| `created_at[gte]=2025-06-01&created_at[lt]=2025-07-01` | Range using `gt`, `gte`, `lt`, or `lte` |
| `claimed_at[isnull]=true` | Null check; `false` checks non-null |
| `enabled=false` | Typed boolean equality |
| `labels[env]=prod&labels[region]=west` | Match both label values |
| `labels[env]=` | Label key exists, including when its stored value is empty |

Comma-separated values within the same field/operator group use OR; different fields or operators use AND. Empty segments in `in` lists are ignored; an entirely empty list matches no rows. UUID values are validated before querying. Booleans accept `true`/`false`, `1`/`0`, `yes`/`no`, and `on`/`off`. Dates/timestamps use ISO 8601; filter values without a timezone are interpreted as UTC. Text operators apply to string columns, and booleans support equality, membership, and null checks.

Filters run in PostgreSQL before pagination and are applied identically to `total`. Caller-supplied IDs always remain restricted to the authenticated client's resources.

```text
GET /v1/execution-targets?id[in]=<uuid1>,<uuid2>&enabled=true&sort=name&include_total=true
GET /v1/clusters?labels[env]=prod&desired_revision[gte]=2&sort=-updated_at
GET /v1/work-items?status[in]=pending,failed&claimed_at[isnull]=true&limit=20
```

## OpenAPI spec

The read endpoints described here and the command ingestion endpoint (`POST /v1/commands`) share a single spec file committed at [`openapi.yaml`](../openapi.yaml) — there is no separate spec for reads vs. commands. The spec is generated automatically by FastAPI from route decorators and Pydantic type annotations.
