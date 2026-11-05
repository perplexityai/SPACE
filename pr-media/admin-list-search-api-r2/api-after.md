# Admin API request/response pairs from the API with the change (#1523's round-1 head)

Synthetic fixtures on the local Kind stack (console-loop seed `loop`, one synthetic warm-pool policy on template `seed`). Responses are trimmed to the fields this change concerns; `<absent>` marks a field the server did not send.

## Search by the console label (last 8 hex digits) of `loop-running`, paged

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=0fdf5873` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ad27-cad7-7287-9de2-4f6d0fdf5873",
      "name": "loop-running",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "root@local",
      "pool_template": null,
      "pool_id": null,
      "status": "started"
    }
  ],
  "next_cursor": null
}
```

## Mixed-case name search, paged

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=LOOP-Suspended` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ad27-f2d2-7370-903a-12910668328a",
      "name": "loop-suspended",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "root@local",
      "pool_template": null,
      "pool_id": null,
      "status": "suspended"
    }
  ],
  "next_cursor": null
}
```

## The same label search on the unpaged list

`GET /admin/api/v1/sandboxes?limit=25&search=0fdf5873` → **200**

```json
[
  {
    "id": "01a1ad27-cad7-7287-9de2-4f6d0fdf5873",
    "name": "loop-running",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "root@local",
    "pool_template": null,
    "pool_id": null,
    "status": "started"
  }
]
```

## A NUL in search

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=a%00b` → **400**

```json
{
  "error": "search must not contain a NUL character"
}
```

## A two-character search

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=ab` → **400**

```json
{
  "error": "search must be at least 3 characters"
}
```

## A search padded with spaces

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=%20%20LOOP-Suspended%20%20` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ad27-f2d2-7370-903a-12910668328a",
      "name": "loop-suspended",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "root@local",
      "pool_template": null,
      "pool_id": null,
      "status": "suspended"
    }
  ],
  "next_cursor": null
}
```

## An undashed ID start (the first 12 hex digits of `loop-running`)

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=01a1ad27cad7` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ad27-cad7-7287-9de2-4f6d0fdf5873",
      "name": "loop-running",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "root@local",
      "pool_template": null,
      "pool_id": null,
      "status": "started"
    }
  ],
  "next_cursor": null
}
```

## A name continuing past the text with U+10FFFF (synthetic tombstone `edge` U+10FFFF `tail`)

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=EDGE&lifecycle=deleted` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ad2c-5f3f-799a-9f7b-b174271dbc80",
      "name": "edge\udbff\udffftail",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "root@local",
      "pool_template": null,
      "pool_id": null,
      "status": "deleted"
    }
  ],
  "next_cursor": null
}
```

## Single sandbox read

`GET /admin/api/v1/sandboxes/sbx_01a1ad27-cad7-7287-9de2-4f6d0fdf5873` → **200**

```json
{
  "id": "01a1ad27-cad7-7287-9de2-4f6d0fdf5873",
  "name": "loop-running",
  "resource_type": "sandbox",
  "user_id": "00000000-0000-0000-0000-000000000010",
  "user_email": "root@local",
  "pool_template": null,
  "pool_id": null,
  "status": "started"
}
```

## Single read of a sandbox with a federated owner (synthetic)

`GET /admin/api/v1/sandboxes/sbx_01a1ad28-6316-7ca2-9b7f-121f4fb167ae` → **200**

```json
{
  "id": "01a1ad28-6316-7ca2-9b7f-121f4fb167ae",
  "name": "federated-owned",
  "resource_type": "sandbox",
  "user_id": "01a1ad28-6316-7932-80be-0dd50aa5cc5b",
  "user_email": null,
  "pool_template": null,
  "pool_id": null,
  "status": "deleted"
}
```

## Single read of the Kubernetes warm-pool slot

`GET /admin/api/v1/sandboxes/sbx_01a1ad28-7138-77f4-8a39-5191407c3bbf` → **200**

```json
{
  "id": "01a1ad28-7138-77f4-8a39-5191407c3bbf",
  "name": null,
  "resource_type": "pool_slot",
  "user_id": null,
  "user_email": null,
  "pool_template": null,
  "pool_id": "01a1ad28-7130-7be6-9b98-21684002d1ab",
  "status": "started"
}
```

## Unfiltered first page, limit 1: one sandbox row, then the node-reported rows (the warm-pool slot)

`GET /admin/api/v1/sandboxes?limit=1&cursor=` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ad2c-5f3f-799a-9f7b-b174271dbc80",
      "name": "edge\udbff\udffftail",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "root@local",
      "pool_template": null,
      "pool_id": null,
      "status": "deleted"
    },
    {
      "id": "01a1ad28-7138-77f4-8a39-5191407c3bbf",
      "name": null,
      "resource_type": "pool_slot",
      "user_id": null,
      "user_email": null,
      "pool_template": null,
      "pool_id": "01a1ad28-7130-7be6-9b98-21684002d1ab",
      "status": "started"
    }
  ],
  "next_cursor": "<cursor>"
}
```

## Spaced-version counts (resource types)

`GET /admin/api/v1/sandboxes/spaced-versions` → **200**

```json
[
  {
    "template": "seed",
    "spaced_version": "0.8.1",
    "status": "started",
    "resource_type": "sandbox",
    "count": 2
  },
  {
    "template": "seed",
    "spaced_version": "0.8.1",
    "status": "started",
    "resource_type": "pool_slot",
    "count": 1
  },
  {
    "template": "seed",
    "spaced_version": null,
    "status": "paused",
    "resource_type": "sandbox",
    "count": 1
  },
  {
    "template": "seed",
    "spaced_version": null,
    "status": "stopped",
    "resource_type": "sandbox",
    "count": 1
  },
  {
    "template": "seed",
    "spaced_version": null,
    "status": "suspended",
    "resource_type": "sandbox",
    "count": 1
  }
]
```
