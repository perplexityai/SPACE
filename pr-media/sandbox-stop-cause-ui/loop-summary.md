# Console loop run 2026-11-05T17-58-06

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
- ok: seeded sandbox suspended exists (deleted)
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

URL: http://localhost:3000/dashboard/orgs/01a1accb-e318-7458-ba97-aeaefedd8a29  
Screenshot: 014-org-detail-acme.png

- ok: the organization page shows its users

## 15. org-detail-globex — ok

URL: http://localhost:3000/dashboard/orgs/01a1accb-e31e-7792-818b-9e12657042c6  
Screenshot: 015-org-detail-globex.png

- ok: the organization page shows its users

## 16. org-detail-globex-research — ok

URL: http://localhost:3000/dashboard/orgs/01a1accb-e322-75af-a68b-d2be394c5b0e  
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

URL: http://localhost:3000/dashboard/users/01a1accb-e329-76be-a065-85c1f48d4624  
Screenshot: 021-user-detail-ops-admin.png

- ok: the user page shows the user

## 22. user-detail-acme-dev — ok

URL: http://localhost:3000/dashboard/users/01a1accb-e5ea-7005-9459-9e062cc4e5fb  
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

- ok: the sandbox list shows sandboxes (8 rows)

## 27. sandboxes-search-name — ok

URL: http://localhost:3000/dashboard/sandboxes?id=loop-suspended  
Screenshot: 027-sandboxes-search-name.png

- ok: searching a seeded name finds one row (1)

## 28. sandboxes-search-id — ok

URL: http://localhost:3000/dashboard/sandboxes?id=sbx_01a1accb-e8c0-778c-b8eb-7ebd280fefd0  
Screenshot: 028-sandboxes-search-id.png

- ok: searching a full sbx_ ID finds the sandbox

## 29. sandbox-detail-running — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1accb-e8c0-778c-b8eb-7ebd280fefd0  
Screenshot: 029-sandbox-detail-running.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1accb-e8c0-778c-b8eb-7ebd280fefd0/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acde-cd35-7623-8d54-cc2fa0db870e","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (started)

## 30. sandbox-detail-paused — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1accb-f144-706d-92d3-d815a9a5bf33  
Screenshot: 030-sandbox-detail-paused.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1accb-f144-706d-92d3-d815a9a5bf33/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acde-de0d-7620-8d57-61f0121345ec","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (paused)

## 31. sandbox-detail-suspended — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1accc-1099-7177-aad0-6600f1f01989  
Screenshot: 031-sandbox-detail-suspended.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1accc-1099-7177-aad0-6600f1f01989/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acde-eeda-77a2-a90a-58bbff86a9c0","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (deleted)

## 32. sandbox-detail-stopped — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1accc-2ff2-73fb-88f5-f4a779833ea7  
Screenshot: 032-sandbox-detail-stopped.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1accc-2ff2-73fb-88f5-f4a779833ea7/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acde-ffa0-7303-983c-eb11258e2f21","retryable":false,"details":[]}`
- ok: the sandbox page loads
- ok: the page shows the API's status (stopped)

## 33. sandbox-detail-acme-running — ok

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1accc-4f40-75b4-82a2-aa14a951a011  
Screenshot: 033-sandbox-detail-acme-running.png

- ok: the sandbox page loads
- ok: the page shows the API's status (started)

## 34. sandbox-detail-deleted — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1accc-5ef3-716b-b414-e499eb41de6e  
Screenshot: 034-sandbox-detail-deleted.png

- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1accc-5ef3-716b-b414-e499eb41de6e -> 404
  `{"error":"Sandbox not found"}`
- **http** GET http://localhost:3000/admin/api/v1/sandboxes/sbx_01a1accc-5ef3-716b-b414-e499eb41de6e -> 404
  `{"error":"Sandbox not found"}`
- ok: a deleted sandbox shows a not-found state

## 35. sandbox-stop-cause — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1acdc-1d96-70c1-a4cb-815b2ad3b4ee  
Screenshot: 035-sandbox-stop-cause.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1accc-2ff2-73fb-88f5-f4a779833ea7/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-3d3a-77de-88e1-f5454d93391d","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdc-1d96-70c1-a4cb-815b2ad3b4ee/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-5cf7-7006-bd14-143412fcccc2","retryable":false,"details":[]}`
- ok: a requested stop reads as a plain stop
- ok: the list marks a sandbox stopped unexpectedly (VM exited)
- ok: the detail page says why it stopped (Stopped unexpectedly: VM exited at 11/5/2026, 5:57:59 PM)

## 36. sandbox-create — ok

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 036-sandbox-create.png

- ok: creating a sandbox from the form produces one
- toast success: Sandbox created
3264f431

## 37. sandbox-pause — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431  
Screenshot: 037-sandbox-pause.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-967b-7434-83ff-2b35cb120290","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-a418-7469-8a57-8f0f7a7f8119","retryable":false,"details":[]}`
- ok: pause reaches paused (paused)
- toast success: pause queued
- toast success: Sandbox pause completed

## 38. sandbox-resume — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431  
Screenshot: 038-sandbox-resume.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-ab89-77ac-9070-eb7e97aa2d3d","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-b91f-7176-ae1f-ba381f305163","retryable":false,"details":[]}`
- ok: resume reaches started or running (started)
- toast success: resume queued
- toast success: Sandbox resume completed

## 39. sandbox-suspend — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431  
Screenshot: 039-sandbox-suspend.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-c086-7011-b1c2-2e677994ba57","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-ce2c-71c0-8e25-2847e04aef89","retryable":false,"details":[]}`
- ok: suspend reaches suspended (suspended)
- toast success: suspend queued
- toast success: Sandbox suspend completed

## 40. sandbox-restore — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431  
Screenshot: 040-sandbox-restore.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-d59c-7024-816c-ce1e0bcd0611","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-e341-71a3-9625-51b22e5e9c8e","retryable":false,"details":[]}`
- ok: restore reaches started or running (started)
- toast success: restore queued
- toast success: Sandbox restore completed

## 41. sandbox-stop — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431  
Screenshot: 041-sandbox-stop.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-eaae-73d2-8b5c-dd92ceb257c7","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1acdf-f9f0-73ca-97a8-d6da07d89165","retryable":false,"details":[]}`
- ok: stop reaches stopped (stopped)
- toast success: stop queued
- toast success: Sandbox stop completed

## 42. sandbox-start — events

URL: http://localhost:3000/dashboard/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431  
Screenshot: 042-sandbox-start.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace0-0167-746a-99d2-1bda734a6e5b","retryable":false,"details":[]}`
- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace0-1245-7330-8686-f5ae1a591ba6","retryable":false,"details":[]}`
- ok: start reaches started or running (started)
- toast success: start queued
- toast success: Sandbox start completed

## 43. sandbox-delete — events

URL: http://localhost:3000/dashboard/sandboxes  
Screenshot: 043-sandbox-delete.png

- **http** GET http://localhost:3000/api/v1/sandboxes/sbx_01a1acdf-87e5-7205-a69c-4bb03264f431/snapshots -> 404
  `{"code":"not_found","message":"route not found","request_id":"req_01a1ace0-168f-7072-b0fa-7f5a3c642fb4","retryable":false,"details":[]}`
- ok: deleting a sandbox returns to the list
- toast success: Sandbox delete queued
- toast success: Sandbox deleted

## 44. templates — ok

URL: http://localhost:3000/dashboard/templates  
Screenshot: 044-templates.png

- ok: the templates page lists the seed template

## 45. nodes — ok

URL: http://localhost:3000/dashboard/nodes  
Screenshot: 045-nodes.png

- ok: the nodes page lists worker nodes

## 46. role-acme-admin — ok

URL: http://localhost:3000/dashboard  
Screenshot: 046-role-acme-admin.png

- ok: a tenant organization admin can sign in

## 47. role-ops-viewer — ok

URL: http://localhost:3000/dashboard  
Screenshot: 047-role-ops-viewer.png

- ok: a member of the managing organization can sign in

## 48. logout — ok

URL: http://localhost:3000/login  
Screenshot: 048-logout.png

- ok: signing out lands on the login page
