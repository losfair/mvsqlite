# mvstore Admin API

The admin API is served on a separate address configured via the `--admin-api` flag. It is disabled when the server runs in read-only mode (`--read-only`).

All endpoints that accept a request body expect `application/json`.

---

## POST `/api/create_namespace`

Create a new namespace.

**Request body:**

| Field          | Type   | Required | Description                        |
| -------------- | ------ | -------- | ---------------------------------- |
| `key`          | string | yes      | Namespace key                      |
| `overlay_base` | object | no       | Overlay base (`ns_id`, `snapshot_version`) |

**Responses:**

| Status | Description              |
| ------ | ------------------------ |
| 201    | Namespace created        |
| 422    | Key already exists       |
| 400    | Other error              |

---

## POST `/api/rename_namespace`

Rename an existing namespace.

**Request body:**

| Field     | Type   | Required | Description      |
| --------- | ------ | -------- | ---------------- |
| `old_key` | string | yes      | Current key      |
| `new_key` | string | yes      | New key          |

**Responses:**

| Status | Description              |
| ------ | ------------------------ |
| 200    | Renamed                  |
| 404    | Old key does not exist   |
| 422    | New key already exists   |
| 400    | Other error              |

---

## POST `/api/delete_namespace`

Delete a namespace and all of its data.

**Request body:**

| Field | Type   | Required | Description    |
| ----- | ------ | -------- | -------------- |
| `key` | string | yes      | Namespace key  |

**Responses:**

| Status | Description              |
| ------ | ------------------------ |
| 200    | Deleted                  |
| 404    | Key does not exist       |
| 400    | Other error              |

---

## POST `/api/truncate_namespace`

Truncate versions in a namespace older than a given version. Returns a streaming response with progress updates.

**Request body:**

| Field            | Type   | Required | Description                                   |
| ---------------- | ------ | -------- | --------------------------------------------- |
| `key`            | string | yes      | Namespace key                                 |
| `before_version` | string | yes      | Hex-encoded version; versions older than this are removed |
| `apply`          | bool   | no       | If `false` (default), dry-run only            |

**Responses:**

| Status | Description                                                       |
| ------ | ----------------------------------------------------------------- |
| 200    | Streaming body with progress numbers, ending in `DONE` or `ERROR` |
| 403    | Rejected by `MVSTORE_TRUNCATE_MIN_AGE_SECONDS` policy             |
| 404    | Key does not exist                                                |

**Global policy: `MVSTORE_TRUNCATE_MIN_AGE_SECONDS`**

If the server is started with `MVSTORE_TRUNCATE_MIN_AGE_SECONDS=N` (default
`0`, disabled), every request — including dry-runs — must satisfy
`before_version <= V`, where `V` is the FDB version recorded by the
timekeeper at the most recent wall-clock instant strictly before
`now - N` seconds. Requests violating the policy are rejected with `403` and
an error message in the body. The mapping is consulted via the same
`time2version` index served by `GET /time2version`.

---

## POST `/api/delete_unreferenced_content`

Delete content pages in a namespace that are not referenced by any version. Returns a streaming response with progress updates.

**Request body:**

| Field   | Type   | Required | Description                        |
| ------- | ------ | -------- | ---------------------------------- |
| `key`   | string | yes      | Namespace key                      |
| `apply` | bool   | no       | If `false` (default), dry-run only |

**Responses:**

| Status | Description                                                       |
| ------ | ----------------------------------------------------------------- |
| 200    | Streaming body with progress lines, ending in `DONE` or `ERROR`   |
| 400    | Key does not exist                                                |

---

## GET `/api/list_namespace`

List all namespaces, optionally filtered by a key prefix. Returns a streaming response of newline-delimited JSON objects.

**Query parameters:**

| Param    | Type   | Required | Description                    |
| -------- | ------ | -------- | ------------------------------ |
| `prefix` | string | no       | Filter namespaces by key prefix |

**Response:**

Streaming newline-delimited JSON. Each line:

```json
{"nskey": "mydb", "nsid": "0a1b2c3d4e5f6a7b8c9d"}
```

---

## POST `/api/stat_namespace`

Return metadata about a single namespace.

**Request body:**

| Field | Type   | Required | Description    |
| ----- | ------ | -------- | -------------- |
| `key` | string | yes      | Namespace key  |

**Responses:**

| Status | Description        |
| ------ | ------------------ |
| 200    | JSON (see below)   |
| 404    | Key does not exist |

**200 response body:**

```json
{
  "nskey": "mydb",
  "nsid": "0a1b2c3d4e5f6a7b8c9d",
  "metadata": {
    "lock": null,
    "overlay_base": null
  }
}
```

The `metadata` object may contain:

- `lock` — if set, an object with `snapshot_version`, `owner`, `nonce`, and `rolling_back` fields.
- `overlay_base` — if set, an object with `ns_id` and `snapshot_version` fields.

---

## GET `/fdb/status`

Return the raw FoundationDB cluster status JSON (the `\xff\xff/status/json` special key).

**Responses:**

| Status | Description                          |
| ------ | ------------------------------------ |
| 200    | FoundationDB status JSON             |
| 404    | Status key not available             |
