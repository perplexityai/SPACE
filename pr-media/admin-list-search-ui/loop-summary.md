# Console loop run 2026-11-05T18-04-48

Console: http://localhost:3000

## 1. login-page — ok

URL: http://localhost:3000/login  
Screenshot: 001-login-page.png

- ok: the login form renders

## 2. login — ok

URL: http://localhost:3000/dashboard  
Screenshot: 002-login.png

- ok: signing in lands on the dashboard

## 3. fixtures — ok

URL: http://localhost:3000/dashboard  
Screenshot: 003-fixtures.png

- ok: seeded org managing exists
- ok: seeded org acme exists
- ok: seeded org globex exists
- ok: seeded org globex-research exists
- ok: seeded user loop-ops-admin@example.test exists
- ok: seeded user loop-ops-viewer@example.test exists
- ok: seeded user loop-acme-admin@example.test exists
- ok: seeded user loop-acme-dev@example.test exists
- ok: seeded user loop-globex-dev@example.test exists
- ok: seeded user loop-research-dev@example.test exists
- ok: seeded API key native exists
- ok: seeded API key ecw exists
- ok: seeded sandbox running exists (started)
- ok: seeded sandbox paused exists (paused)
- ok: seeded sandbox suspended exists (suspended)
- ok: seeded sandbox stopped exists (stopped)
- ok: seeded sandbox acme-running exists (started)
- ok: seeded sandbox deleted is recorded

## 4. route-dashboard — ok

URL: http://localhost:3000/dashboard  
Screenshot: 004-route-dashboard.png

- ok: the menu lists Overview
- ok: /dashboard renders content

## 5. route-orgs — ok

URL: http://localhost:3000/dashboard/orgs  
Screenshot: 005-route-orgs.png

- ok: the menu lists Organizations
- ok: /dashboard/orgs renders content

## 6. route-users — ok

URL: http://localhost:3000/dashboard/users  
Screenshot: 006-route-users.png

- ok: the menu lists Users
- ok: /dashboard/users renders content

## 7. route-api-keys — ok

URL: http://localhost:3000/dashboard/api-keys  
Screenshot: 007-route-api-keys.png

- ok: the menu lists API Keys
- ok: /dashboard/api-keys renders content

## 8. route-sandboxes — ok

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 008-route-sandboxes.png

- ok: the menu lists Sandboxes
- ok: /dashboard/sandboxes renders content

## 9. route-credential-rules — ok

URL: http://localhost:3000/dashboard/credential-rules  
Screenshot: 009-route-credential-rules.png

- ok: the menu lists Credential Rules
- ok: /dashboard/credential-rules renders content

## 10. route-templates — ok

URL: http://localhost:3000/dashboard/templates  
Screenshot: 010-route-templates.png

- ok: the menu lists Templates
- ok: /dashboard/templates renders content

## 11. route-nodes — ok

URL: http://localhost:3000/dashboard/nodes  
Screenshot: 011-route-nodes.png

- ok: the menu lists Nodes
- ok: /dashboard/nodes renders content

## 12. orgs-list — ok

URL: http://localhost:3000/dashboard/orgs  
Screenshot: 012-orgs-list.png

- ok: the organizations list shows Acme (loop)
- ok: the organizations list shows Globex (loop)
- ok: the organizations list shows Globex Research (loop)

## 13. org-detail-managing — ok

URL: http://localhost:3000/dashboard/orgs/00000000-0000-0000-0000-000000000001  
Screenshot: 013-org-detail-managing.png

- ok: the organization page shows its users

## 14. org-detail-acme — ok

URL: http://localhost:3000/dashboard/orgs/01a1ac73-8200-7036-b120-fe778344cef0  
Screenshot: 014-org-detail-acme.png

- ok: the organization page shows its users

## 15. org-detail-globex — ok

URL: http://localhost:3000/dashboard/orgs/01a1ac73-8280-7686-ade1-97bbb411bd21  
Screenshot: 015-org-detail-globex.png

- ok: the organization page shows its users

## 16. org-detail-globex-research — ok

URL: http://localhost:3000/dashboard/orgs/01a1ac73-8284-7069-b4e0-6732865ec815  
Screenshot: 016-org-detail-globex-research.png

- ok: the organization page shows its users

## 17. org-detail-unknown — events

URL: http://localhost:3000/dashboard/orgs/00000000-0000-0000-0000-00000000dead  
Screenshot: 017-org-detail-unknown.png

- **http** GET http://localhost:3000/admin/api/v1/orgs/00000000-0000-0000-0000-00000000dead -> 404
  `{"error":"resource not found"}`
- **http** GET http://localhost:3000/admin/api/v1/orgs/00000000-0000-0000-0000-00000000dead/master-key -> 404
  `{"error":"resource not found"}`
- ok: an unknown organization shows a not-found or error state

## 18. org-create-delete — ok

URL: http://localhost:3000/dashboard/orgs  
Screenshot: 018-org-create-delete.png

- ok: a created organization appears in the list
- ok: a deleted organization disappears
- toast success: Organization created
- toast success: Organization deleted

## 19. users-list — ok

URL: http://localhost:3000/dashboard/users  
Screenshot: 019-users-list.png

- ok: the users list opens on the signed-in admin's organization

## 20. users-switch-org — ok

URL: http://localhost:3000/dashboard/users  
Screenshot: 020-users-switch-org.png

- ok: switching organization lists that organization's users

## 21. user-detail-ops-admin — ok

URL: http://localhost:3000/dashboard/users/01a1ac73-828b-7022-9742-804b08143d82  
Screenshot: 021-user-detail-ops-admin.png

- ok: the user page shows the user

## 22. user-detail-acme-dev — ok

URL: http://localhost:3000/dashboard/users/01a1ac73-8549-77a0-afec-7ce24ab89bf7  
Screenshot: 022-user-detail-acme-dev.png

- ok: the user page shows the user

## 23. user-create-role-delete — ok

URL: http://localhost:3000/dashboard/users  
Screenshot: 023-user-create-role-delete.png

- ok: a created user appears in the list
- ok: the role change persists (organization_admin)
- ok: a deleted user disappears
- toast success: User created
- toast success: Role updated
- toast success: User deleted

## 24. api-keys-list — ok

URL: http://localhost:3000/dashboard/api-keys  
Screenshot: 024-api-keys-list.png

- ok: the API keys list shows the seeded key

## 25. api-key-create-revoke — ok

URL: http://localhost:3000/dashboard/api-keys  
Screenshot: 025-api-key-create-revoke.png

- ok: the create response carries the new key's ID
- ok: the create response's secret is replaced before the page sees it
- ok: the dialog shows the placeholder, not a key
- ok: a created key appears in the list
- ok: a revoked key disappears
- toast success: API key created
- toast success: API key revoked

## 26. sandboxes-list — ok

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 026-sandboxes-list.png

- ok: the sandbox list shows sandboxes (36 rows)

## 27. sandboxes-search-name — ok

URL: http://localhost:3000/dashboard/sandboxes?id=loop-suspended  
Screenshot: 027-sandboxes-search-name.png

- ok: searching a seeded name finds one row (1)

## 28. sandboxes-search-id — ok

URL: http://localhost:3000/dashboard/sandboxes?id=sbx_01a1ac73-8825-73e4-95a3-83084ba0eb91  
Screenshot: 028-sandboxes-search-id.png

- ok: searching a full sbx_ ID finds the sandbox

## 29. sandboxes-search-label — ok

URL: http://localhost:3000/dashboard/sandboxes?id=4ba0eb91&page_size=25  
Screenshot: 029-sandboxes-search-label.png

- the server answered "4ba0eb91" as search
- ok: searching the ID column's label finds the sandbox through the server

## 30. sandboxes-search-name-case — ok

URL: http://localhost:3000/dashboard/sandboxes?id=LOOP-SUSPENDED&page_size=25  
Screenshot: 030-sandboxes-search-name-case.png

- the server answered "LOOP-SUSPENDED" as search
- ok: searching LOOP-SUSPENDED finds loop-suspended alone

## 31. sandboxes-user-email — ok

URL: http://localhost:3000/dashboard/sandboxes?page_size=25&org=01a1ac73-8200-7036-b120-fe778344cef0  
Screenshot: 031-sandboxes-user-email.png

- ok: the User column shows the owner's email

## 32. sandboxes-create-form — ok

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 032-sandboxes-create-form.png

- ok: the create form offers no runtimes SPACE does not implement

## 33. sandbox-detail-datadog — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-8825-73e4-95a3-83084ba0eb91  
Screenshot: 033-sandbox-detail-datadog.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-8825-73e4-95a3-83084ba0eb91/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-4789-7148-b73b-163558f455dd","retryable":false,"details":[]}`
- no Datadog logs button (SPACE_UI_DATADOG_LOGS_URL unset)

## 34. sandbox-detail-running — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-8825-73e4-95a3-83084ba0eb91  
Screenshot: 034-sandbox-detail-running.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-8825-73e4-95a3-83084ba0eb91/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-5859-766b-99f8-5c3b9b9f266e","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (started)

## 35. sandbox-detail-paused — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-90ad-77f7-a936-d43d328b08fc  
Screenshot: 035-sandbox-detail-paused.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-90ad-77f7-a936-d43d328b08fc/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-6937-75b2-bb19-fcc3c0497093","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (paused)

## 36. sandbox-detail-suspended — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-b008-73dc-b17b-322ebff06721  
Screenshot: 036-sandbox-detail-suspended.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-b008-73dc-b17b-322ebff06721/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-7a1f-76b8-956a-ec351f407f1e","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (suspended)

## 37. sandbox-detail-stopped — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-cf5e-721b-a222-aa258a03a76f  
Screenshot: 037-sandbox-detail-stopped.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-cf5e-721b-a222-aa258a03a76f/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-8af0-7636-9576-1d7b4011460b","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (stopped)

## 38. sandbox-detail-acme-running — ok

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-eeb1-7382-9117-0ea7bdb783b7  
Screenshot: 038-sandbox-detail-acme-running.png

- ok: the sandbox page loads
- ok: the page shows the API's status (started)

## 39. sandbox-detail-deleted — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-fe5d-76ef-8f45-fa63683a0eeb  
Screenshot: 039-sandbox-detail-deleted.png

- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1ac73-fe5d-76ef-8f45-fa63683a0eeb -> 404
  `{"error":"Sandbox not found"}`
- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1ac73-fe5d-76ef-8f45-fa63683a0eeb -> 404
  `{"error":"Sandbox not found"}`
- ok: a deleted sandbox shows a not-found state

## 40. sandbox-create — ok

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 040-sandbox-create.png

- ok: creating a sandbox from the form produces one
- toast success: Sandbox created
4ff51393

## 41. sandbox-pause — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393  
Screenshot: 041-sandbox-pause.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-db9a-760d-b1b7-49f066f48e6d","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-e93b-7159-afe8-fe180444a0f9","retryable":false,"details":[]}`
- ok: pause reaches paused (paused)
- toast success: pause queued
- toast success: Sandbox pause completed

## 42. sandbox-resume — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393  
Screenshot: 042-sandbox-resume.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-f0a8-70a0-89c6-6d09e7018c03","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace5-fe4d-711b-adf5-5065b5d18805","retryable":false,"details":[]}`
- ok: resume reaches started or running (started)
- toast success: resume queued
- toast success: Sandbox resume completed

## 43. sandbox-suspend — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393  
Screenshot: 043-sandbox-suspend.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-05b0-778b-a3ee-9d80571cf3ee","retryable":false,"details":[]}`
- ok: suspend reaches suspended (suspended)
- toast success: suspend queued

## 44. sandbox-restore — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393  
Screenshot: 044-sandbox-restore.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-1aeb-730b-bd2b-43cc6063f15b","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-2bbd-7151-97c3-44263e9213b6","retryable":false,"details":[]}`
- ok: restore reaches started or running (started)
- toast success: restore queued
- toast success: Sandbox restore completed

## 45. sandbox-stop — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393  
Screenshot: 045-sandbox-stop.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-3044-7067-9abd-22ac449fd84f","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-3f98-7116-8a41-8f4b4e994283","retryable":false,"details":[]}`
- ok: stop reaches stopped (stopped)
- toast success: stop queued
- toast success: Sandbox stop completed

## 46. sandbox-start — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393  
Screenshot: 046-sandbox-start.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-4718-7221-8510-ef9316c40213","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-7464-74fb-844e-60c85f3b475d","retryable":false,"details":[]}`
- ok: start reaches started or running (started)
- toast success: start queued
- toast success: Sandbox start completed

## 47. sandbox-delete — events

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 047-sandbox-delete.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace6-7bc0-777d-bc8b-d75f21c48171","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1ace5-cc0f-7174-846c-74384ff51393 -> 404
  `{"error":"Sandbox not found"}`
- ok: deleting a sandbox returns to the list
- toast success: Sandbox delete queued
- toast success: Sandbox deleted

## 48. templates — ok

URL: http://localhost:3000/dashboard/templates  
Screenshot: 048-templates.png

- ok: the templates page lists the seed template

## 49. nodes — ok

URL: http://localhost:3000/dashboard/nodes  
Screenshot: 049-nodes.png

- ok: the nodes page lists worker nodes

## 50. role-acme-admin — ok

URL: http://localhost:3000/dashboard  
Screenshot: 050-role-acme-admin.png

- ok: a tenant organization admin can sign in

## 51. role-ops-viewer — ok

URL: http://localhost:3000/dashboard  
Screenshot: 051-role-ops-viewer.png

- ok: a member of the managing organization can sign in

## 52. logout — ok

URL: http://localhost:3000/login  
Screenshot: 052-logout.png

- ok: signing out lands on the login page
