# Admin create requester, before and after

## Before (main, 849b8b2c3)

Synthetic fixtures from the console loop's seed. The administrator is the
Kind stack's `root@local` (`00000000-0000-0000-0000-000000000010`); the owner is the seeded
tenant member `loop-acme-dev@example.test` (`01a1ac77-7f76-7085-a04a-ebbd68fc3104`).

Request:

```http
POST /admin/api/v1/sandboxes
Idempotency-Key: <random UUID>
Content-Type: application/json

{
  "org_id": "01a1ac77-7c19-7711-9faf-52a527232833",
  "user_id": "01a1ac77-7f76-7085-a04a-ebbd68fc3104",
  "sandbox": {
    "name": "loop-requested-by-before",
    "template": "seed:latest",
    "runtime": "fc"
  }
}
```

Response `202`:

```json
{
  "operation_id": "op_01a1ac7c-6ded-7350-8853-60e62f7b813a",
  "sandbox_id": "sbx_01a1ac7c-6dec-732b-a5b4-cbe4de94930a"
}
```

Then `GET /admin/api/v1/operations/op_01a1ac7c-6ded-7350-8853-60e62f7b813a`, selected fields:

```json
{
  "id": "op_01a1ac7c-6ded-7350-8853-60e62f7b813a",
  "kind": "create_sandbox",
  "state": "queued",
  "actor_user_id": "01a1ac77-7f76-7085-a04a-ebbd68fc3104",
  "actor_email": "loop-acme-dev@example.test",
  "requested_by_user_id": null,
  "target_id": "sbx_01a1ac7c-6dec-732b-a5b4-cbe4de94930a"
}
```

The create's operation then reached `succeeded`; the sandbox
was deleted afterwards (`:force-delete` answered 202, its operation
`succeeded`).

## Admin create, after (this branch, cec266459)

Synthetic fixtures from the console loop's seed. The administrator is the
Kind stack's `root@local` (`00000000-0000-0000-0000-000000000010`); the owner is the seeded
tenant member `loop-acme-dev@example.test` (`01a1ac77-7f76-7085-a04a-ebbd68fc3104`).

Request:

```http
POST /admin/api/v1/sandboxes
Idempotency-Key: <random UUID>
Content-Type: application/json

{
  "org_id": "01a1ac77-7c19-7711-9faf-52a527232833",
  "user_id": "01a1ac77-7f76-7085-a04a-ebbd68fc3104",
  "sandbox": {
    "name": "loop-requested-by-after",
    "template": "seed:latest",
    "runtime": "fc"
  }
}
```

Response `202`:

```json
{
  "operation_id": "op_01a1ac80-6f3d-774a-8d04-a9c6eb971443",
  "sandbox_id": "sbx_01a1ac80-6f3a-70c4-b882-fbd61b82b207"
}
```

Then `GET /admin/api/v1/operations/op_01a1ac80-6f3d-774a-8d04-a9c6eb971443`, selected fields:

```json
{
  "id": "op_01a1ac80-6f3d-774a-8d04-a9c6eb971443",
  "kind": "create_sandbox",
  "state": "running",
  "actor_user_id": "01a1ac77-7f76-7085-a04a-ebbd68fc3104",
  "actor_email": "loop-acme-dev@example.test",
  "requested_by_user_id": "00000000-0000-0000-0000-000000000010",
  "target_id": "sbx_01a1ac80-6f3a-70c4-b882-fbd61b82b207"
}
```

The create's operation then reached `succeeded`; the sandbox
was deleted afterwards (`:force-delete` answered 202, its operation
`succeeded`).
