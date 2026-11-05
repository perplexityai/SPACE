# ID-start search on #1523's round-2 head (1da9f69e), Kind, loop seed

All 5 seeded sandboxes' IDs start with `01a1`.

- `GET /admin/api/v1/sandboxes?limit=25&cursor=&search=01a1` → **200**, []
- `GET /admin/api/v1/sandboxes?limit=25&cursor=&search=01a1ad6` → **200**, []
- `GET /admin/api/v1/sandboxes?limit=25&cursor=&search=01a1ad69` → **200**, ['loop-paused', 'loop-running']
- `GET /admin/api/v1/sandboxes?limit=25&cursor=&search=sbx_01a1-ad69` → **200**, ['loop-paused', 'loop-running']
- `GET /admin/api/v1/sandboxes?limit=25&cursor=&search=a%20` → **400**, {'error': 'search must be at least 3 characters'}
