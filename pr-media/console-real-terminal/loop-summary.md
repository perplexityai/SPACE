# Console loop run 2026-11-05T16-54-21

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

URL: http://localhost:3000/dashboard/orgs/01a1ac73-7d96-76eb-bd00-8e2def767a60  
Screenshot: 014-org-detail-acme.png

- ok: the organization page shows its users

## 15. org-detail-globex — ok

URL: http://localhost:3000/dashboard/orgs/01a1ac73-7d9c-7485-b6b3-c4193810f2a5  
Screenshot: 015-org-detail-globex.png

- ok: the organization page shows its users

## 16. org-detail-globex-research — ok

URL: http://localhost:3000/dashboard/orgs/01a1ac73-7da0-74e6-9fa3-1b80acea0dec  
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

URL: http://localhost:3000/dashboard/users/01a1ac73-7e16-72a2-afe7-bfb3baf113ed  
Screenshot: 021-user-detail-ops-admin.png

- ok: the user page shows the user

## 22. user-detail-acme-dev — ok

URL: http://localhost:3000/dashboard/users/01a1ac73-80d3-731e-af47-5d620ece8597  
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

- ok: the sandbox list shows sandboxes (6 rows)

## 27. sandboxes-search-name — ok

URL: http://localhost:3000/dashboard/sandboxes?id=loop-suspended  
Screenshot: 027-sandboxes-search-name.png

- ok: searching a seeded name finds one row (1)

## 28. sandboxes-search-id — ok

URL: http://localhost:3000/dashboard/sandboxes?id=sbx_01a1ac73-83a6-77e3-9f92-7d854f27436c  
Screenshot: 028-sandboxes-search-id.png

- ok: searching a full sbx_ ID finds the sandbox

## 29. sandbox-detail-running — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-83a6-77e3-9f92-7d854f27436c  
Screenshot: 029-sandbox-detail-running.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-83a6-77e3-9f92-7d854f27436c/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-4610-777a-82c1-273f4ddbb962","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (started)

## 30. sandbox-detail-paused — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-8c39-7293-b907-cafc3d94444c  
Screenshot: 030-sandbox-detail-paused.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-8c39-7293-b907-cafc3d94444c/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-56e9-71bc-aced-086782b97252","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (paused)

## 31. sandbox-detail-suspended — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-ab92-75d2-a3d3-e15e5e1ef3cc  
Screenshot: 031-sandbox-detail-suspended.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-ab92-75d2-a3d3-e15e5e1ef3cc/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-67f5-7702-af38-09e84fd55840","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (suspended)

## 32. sandbox-detail-stopped — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-caea-77f3-a369-ce65d588bf6c  
Screenshot: 032-sandbox-detail-stopped.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-caea-77f3-a369-ce65d588bf6c/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-78d1-71c2-8414-3bf82163e4b0","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (stopped)

## 33. sandbox-detail-acme-running — ok

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-ea3b-7657-83cc-d252fd6ff5f4  
Screenshot: 033-sandbox-detail-acme-running.png

- ok: the sandbox page loads
- ok: the page shows the API's status (started)

## 34. sandbox-detail-deleted — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-f9e8-72bf-a002-efe2087e5252  
Screenshot: 034-sandbox-detail-deleted.png

- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1ac73-f9e8-72bf-a002-efe2087e5252 -> 404
  `{"error":"Sandbox not found"}`
- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1ac73-f9e8-72bf-a002-efe2087e5252 -> 404
  `{"error":"Sandbox not found"}`
- ok: a deleted sandbox shows a not-found state

## 35. sandbox-create — ok

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 035-sandbox-create.png

- ok: creating a sandbox from the form produces one
- toast success: Sandbox created
bc299bce

## 36. sandbox-pause — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce  
Screenshot: 036-sandbox-pause.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-d304-7582-adbc-b8e3ddbc821b","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-e0ba-768e-bbd0-5f140fc16e8f","retryable":false,"details":[]}`
- ok: pause reaches paused (paused)
- toast success: pause queued
- toast success: Sandbox pause completed

## 37. sandbox-resume — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce  
Screenshot: 037-sandbox-resume.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-e822-70a0-a789-f78098f8df4a","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-f5db-739b-b006-4c32bc30096b","retryable":false,"details":[]}`
- ok: resume reaches started or running (started)
- toast success: resume queued
- toast success: Sandbox resume completed

## 38. sandbox-suspend — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce  
Screenshot: 038-sandbox-suspend.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca4-fd31-7677-82aa-cf7b712516d6","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-1478-7377-b661-dae4d02a6a38","retryable":false,"details":[]}`
- ok: suspend reaches suspended (suspended)
- toast success: suspend queued
- toast success: Sandbox suspend completed

## 39. sandbox-restore — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce  
Screenshot: 039-sandbox-restore.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-1a34-77db-ac88-e6879e9f2548","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-2b17-737a-83ae-7b80b553d982","retryable":false,"details":[]}`
- ok: restore reaches started or running (started)
- toast success: restore queued
- toast success: Sandbox restore completed

## 40. sandbox-stop — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce  
Screenshot: 040-sandbox-stop.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-2f4c-745c-bad4-5a9920ac4f02","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-3e8b-700d-999f-a3bbc54d87d8","retryable":false,"details":[]}`
- ok: stop reaches stopped (stopped)
- toast success: stop queued
- toast success: Sandbox stop completed

## 41. sandbox-start — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce  
Screenshot: 041-sandbox-start.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-45fa-710c-85af-5a9447d6d3f5","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-701e-74d4-8c26-d9a4d29536bb","retryable":false,"details":[]}`
- ok: start reaches started or running (started)
- toast success: start queued

## 42. sandbox-delete — events

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 042-sandbox-delete.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-72c9-74b7-adda-17ea9d595d1e","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1aca4-c47d-7119-81d1-2884bc299bce -> 404
  `{"error":"Sandbox not found"}`
- ok: deleting a sandbox returns to the list
- toast success: Sandbox delete queued
- toast success: Sandbox deleted

## 43. terminal-running — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-83a6-77e3-9f92-7d854f27436c  
Screenshot: 043-terminal-running.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1ac73-83a6-77e3-9f92-7d854f27436c/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1aca5-9839-72ad-82ec-ecd9bf78938a","retryable":false,"details":[]}`
- ok: the Terminal card holds a terminal emulator
- ok: Connect attaches a session (attached · term_01a1aca5-a69b-7400-… Clear Disconnect)
- ok: the terminal has focus once attached
- ok: a command runs and prints
- ok: ls --color draws colored text (24 spans)
- ok: the up arrow recalls the last command
- ok: Tab completes a command name
- ok: Ctrl-C stops sleep 100
- ok: top draws its process table
- ok: q quits top back to the shell
- ok: the shell reports whether vi exists
- the sandbox has no vi; the vi check did not run
- ok: the shell reports its size
- ok: the shell reports its size after the resize
- ok: a narrower window narrows the PTY (144x28 → 88x28)
- ok: Disconnect closes the session
- ok: Connect attaches a session (attached · term_01a1aca5-c2d5-754c-… Clear Disconnect)
- ok: a second Connect opens a new shell

## 44. terminal-acme-running — ok

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1ac73-ea3b-7657-83cc-d252fd6ff5f4  
Screenshot: 044-terminal-acme-running.png

- ok: the Terminal card holds a terminal emulator
- ok: another organization's sandbox shows the admin-route notice
- ok: Connect attaches a session (attached · term_01a1aca5-d9e3-772a-… Clear Disconnect)
- ok: the terminal has focus once attached
- ok: a command runs and prints
- ok: ls --color draws colored text (24 spans)
- ok: the up arrow recalls the last command
- ok: Tab completes a command name
- ok: Ctrl-C stops sleep 100
- ok: Disconnect closes the session
- ok: Connect attaches a session (attached · term_01a1aca5-e8b9-7631-… Clear Disconnect)
- ok: a second Connect opens a new shell

## 45. templates — ok

URL: http://localhost:3000/dashboard/templates  
Screenshot: 045-templates.png

- ok: the templates page lists the seed template

## 46. nodes — ok

URL: http://localhost:3000/dashboard/nodes  
Screenshot: 046-nodes.png

- ok: the nodes page lists worker nodes

## 47. role-acme-admin — ok

URL: http://localhost:3000/dashboard  
Screenshot: 047-role-acme-admin.png

- ok: a tenant organization admin can sign in

## 48. role-ops-viewer — ok

URL: http://localhost:3000/dashboard  
Screenshot: 048-role-ops-viewer.png

- ok: a member of the managing organization can sign in

## 49. logout — ok

URL: http://localhost:3000/login  
Screenshot: 049-logout.png

- ok: signing out lands on the login page
