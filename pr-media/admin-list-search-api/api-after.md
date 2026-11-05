# 02 api-backend: admin API request/response pairs (branch API)

Synthetic fixtures on the local Kind stack (console-loop seed `loop`, one synthetic warm-pool policy on template `seed`). Responses are trimmed to the fields this change concerns; `<absent>` marks a field the server did not send.

## Search by the console label (last 8 hex digits) of `loop-running`, paged

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=4ba0eb91` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ac73-8825-73e4-95a3-83084ba0eb91",
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
      "id": "01a1ac73-b008-73dc-b17b-322ebff06721",
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

`GET /admin/api/v1/sandboxes?limit=25&search=4ba0eb91` → **200**

```json
[
  {
    "id": "01a1ac73-8825-73e4-95a3-83084ba0eb91",
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

## Single sandbox read

`GET /admin/api/v1/sandboxes/sbx_01a1ac73-8825-73e4-95a3-83084ba0eb91` → **200**

```json
{
  "id": "01a1ac73-8825-73e4-95a3-83084ba0eb91",
  "name": "loop-running",
  "resource_type": "sandbox",
  "user_id": "00000000-0000-0000-0000-000000000010",
  "user_email": "root@local",
  "pool_template": null,
  "pool_id": null,
  "status": "started"
}
```

## Unfiltered first page, limit 1: one sandbox row, then the node-reported rows (the warm-pool slot)

`GET /admin/api/v1/sandboxes?limit=1&cursor=` → **200**

```json
{
  "sandboxes": [
    {
      "id": "01a1ac79-7b53-74d3-b30b-035e14863097",
      "name": "filler-30",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "root@local",
      "pool_template": null,
      "pool_id": null,
      "status": "deleted"
    },
    {
      "id": "01a1ac79-7ab7-723d-ab3d-aaf4048f71c1",
      "name": null,
      "resource_type": "pool_slot",
      "user_id": null,
      "user_email": null,
      "pool_template": null,
      "pool_id": "01a1ac79-7a9b-78a0-ad7e-3b1b81389946",
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
