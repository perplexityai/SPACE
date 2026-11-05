# 04: the same admin API requests answered by the API without this change (main's API code at 0c63a488, which only adds the index migrations)

Synthetic fixtures on the local Kind stack (console-loop seed `loop`, one synthetic warm-pool policy on template `seed`). Responses are trimmed to the fields this change concerns; `<absent>` marks a field the server did not send.

## Search by the console label (last 8 hex digits) of `loop-running`, paged

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=4ba0eb91` → **400**

```json
{
  "error": "unknown query parameter \"search\""
}
```

## Mixed-case name search, paged

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=LOOP-Suspended` → **400**

```json
{
  "error": "unknown query parameter \"search\""
}
```

## The same label search on the unpaged list

`GET /admin/api/v1/sandboxes?limit=25&search=4ba0eb91` → **200**

```json
[
  {
    "id": "01a1ac79-7b53-74d3-b30b-035e14863097",
    "name": "filler-30",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-76f7-9c69-4bf6354c716f",
    "name": "filler-29",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-7be2-8aae-b443b0dcf9d6",
    "name": "filler-28",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-7f3a-b8ea-1d0fa4ac7ae8",
    "name": "filler-27",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-75ab-bbf3-8cd36bdf3810",
    "name": "filler-26",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-70ee-937d-28fbebb9e13a",
    "name": "filler-25",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-7eca-936f-4154fa592b90",
    "name": "filler-24",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-7068-9118-4a1eceb9c740",
    "name": "filler-23",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-74b0-b443-abaca01d39a5",
    "name": "filler-22",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b53-7593-a664-4057da0cc10e",
    "name": "filler-21",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7eb2-80d2-1639a334dd37",
    "name": "filler-20",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7191-864f-ca56c90f4c25",
    "name": "filler-19",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-73f2-a0b5-ab519a7cfbc3",
    "name": "filler-18",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7e37-b05a-24074bda09b7",
    "name": "filler-17",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7c13-9b57-f4296afd5d94",
    "name": "filler-16",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7a39-8324-894d3708dada",
    "name": "filler-15",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7975-9bde-04db51c9e52c",
    "name": "filler-14",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7ad7-b958-1d00b123d993",
    "name": "filler-13",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-725e-8d4b-ae2fc8946d55",
    "name": "filler-12",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-78a2-ac80-3f7e06fab601",
    "name": "filler-11",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7de4-bbb4-e8d680857897",
    "name": "filler-10",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7c0d-a2a3-78c3555058e4",
    "name": "filler-09",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7370-a51f-df14405972b3",
    "name": "filler-08",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-77ae-b047-c4da948e9a42",
    "name": "filler-07",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7b52-7b5b-a149-bd46e3372d90",
    "name": "filler-06",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ac79-7ab7-723d-ab3d-aaf4048f71c1",
    "name": null,
    "resource_type": "observed_local",
    "user_id": null,
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "started"
  }
]
```

## A NUL in search

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=a%00b` → **400**

```json
{
  "error": "unknown query parameter \"search\""
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
  "user_email": "<absent>",
  "pool_template": null,
  "pool_id": "<absent>",
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
      "user_email": "<absent>",
      "pool_template": null,
      "pool_id": "<absent>",
      "status": "deleted"
    },
    {
      "id": "01a1ac79-7ab7-723d-ab3d-aaf4048f71c1",
      "name": null,
      "resource_type": "observed_local",
      "user_id": null,
      "user_email": "<absent>",
      "pool_template": null,
      "pool_id": "<absent>",
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
    "resource_type": "observed_local",
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
