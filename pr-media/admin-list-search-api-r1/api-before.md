# Admin API request/response pairs from the API without the change (migration-only head 81164ddc)

Synthetic fixtures on the local Kind stack (console-loop seed `loop`, one synthetic warm-pool policy on template `seed`). Responses are trimmed to the fields this change concerns; `<absent>` marks a field the server did not send.

## Search by the console label (last 8 hex digits) of `loop-running`, paged

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=0fdf5873` → **400**

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

`GET /admin/api/v1/sandboxes?limit=25&search=0fdf5873` → **200**

```json
[
  {
    "id": "01a1ad2c-5f3f-799a-9f7b-b174271dbc80",
    "name": "edge\udbff\udffftail",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-6316-7ca2-9b7f-121f4fb167ae",
    "name": "federated-owned",
    "resource_type": "sandbox",
    "user_id": "01a1ad28-6316-7932-80be-0dd50aa5cc5b",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-76bd-9e03-c7a14db96740",
    "name": "filler-30",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7f48-94b3-4c8137a21c75",
    "name": "filler-29",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7228-bcd0-10079f05dcd1",
    "name": "filler-28",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7030-b7f9-db67e6dcaaa6",
    "name": "filler-27",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7ce9-98a9-e21bd531d2d4",
    "name": "filler-26",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-775c-b26a-abda22304194",
    "name": "filler-25",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7ca6-ae55-f4af76309184",
    "name": "filler-24",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7199-922d-e1c94b3a0100",
    "name": "filler-23",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7d92-a28d-96350e76f977",
    "name": "filler-22",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7e52-a941-352fda9cb477",
    "name": "filler-21",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-713b-8736-c8d3a1fa31a2",
    "name": "filler-20",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-76af-81a5-258077e333a8",
    "name": "filler-19",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7414-bcd7-f5f27060605b",
    "name": "filler-18",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7ad2-a280-04b67d3eef30",
    "name": "filler-17",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-763f-b46a-468b80ddca9a",
    "name": "filler-16",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-705e-ab73-b0b4dab90b79",
    "name": "filler-15",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-78bb-905f-7abce85285b0",
    "name": "filler-14",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-73e4-98b3-01664e66aab5",
    "name": "filler-13",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-707e-85dd-2a7fe52f7d92",
    "name": "filler-12",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7e9a-9eee-0f2b4430f787",
    "name": "filler-11",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-73c2-b2f4-eaf3db05bffb",
    "name": "filler-10",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-7a14-89e6-ba7dc4c4d461",
    "name": "filler-09",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-625c-78c5-8738-fe3d43a82f93",
    "name": "filler-08",
    "resource_type": "sandbox",
    "user_id": "00000000-0000-0000-0000-000000000010",
    "user_email": "<absent>",
    "pool_template": null,
    "pool_id": "<absent>",
    "status": "deleted"
  },
  {
    "id": "01a1ad28-7138-77f4-8a39-5191407c3bbf",
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

## A two-character search

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=ab` → **400**

```json
{
  "error": "unknown query parameter \"search\""
}
```

## A search padded with spaces

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=%20%20LOOP-Suspended%20%20` → **400**

```json
{
  "error": "unknown query parameter \"search\""
}
```

## An undashed ID start (the first 12 hex digits of `loop-running`)

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=01a1ad27cad7` → **400**

```json
{
  "error": "unknown query parameter \"search\""
}
```

## A name continuing past the text with U+10FFFF (synthetic tombstone `edge` U+10FFFF `tail`)

`GET /admin/api/v1/sandboxes?limit=25&cursor=&search=EDGE&lifecycle=deleted` → **400**

```json
{
  "error": "unknown query parameter \"search\""
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
  "user_email": "<absent>",
  "pool_template": null,
  "pool_id": "<absent>",
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
  "user_email": "<absent>",
  "pool_template": null,
  "pool_id": "<absent>",
  "status": "deleted"
}
```

## Single read of the Kubernetes warm-pool slot

`GET /admin/api/v1/sandboxes/sbx_01a1ad28-7138-77f4-8a39-5191407c3bbf` → **200**

```json
{
  "id": "01a1ad28-7138-77f4-8a39-5191407c3bbf",
  "name": null,
  "resource_type": "observed_local",
  "user_id": null,
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
      "id": "01a1ad2c-5f3f-799a-9f7b-b174271dbc80",
      "name": "edge\udbff\udffftail",
      "resource_type": "sandbox",
      "user_id": "00000000-0000-0000-0000-000000000010",
      "user_email": "<absent>",
      "pool_template": null,
      "pool_id": "<absent>",
      "status": "deleted"
    },
    {
      "id": "01a1ad28-7138-77f4-8a39-5191407c3bbf",
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
