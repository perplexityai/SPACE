# Admin UI

Run `npm ci` and `npm run dev` for local development, or `npm run build`
and `npm start` for a production build. Run `npm test` for the
dashboard URL validation tests.

## API proxy routing

`NEXT_PUBLIC_API_URL` selects the backend for `/api/*` and `/auth/*`, defaulting
to `http://localhost:8080`. Admin requests under `/admin/*` use the same backend
unless `NEXT_PUBLIC_ADMIN_API_URL` is set to a separate admin API origin. An
unset or empty admin override preserves the existing fallback.

Restart the development server after changing these values. For production,
set them before `npm run build`; changing the running server's environment does
not update rewrites already included in a built artifact.

## Sandbox dashboard link

Set `SPACE_UI_DATADOG_DASHBOARD_URL` on the running UI server to enable the
sandbox's Debug Dashboard button. The URL must use HTTPS and contain no
username or password. An unset or invalid value hides only the button; invalid
values produce a server warning without logging the URL. This is runtime
configuration: the same built image can serve different deployments without
rebuilding browser assets. The URL is
visible to UI users, so do not put secrets in its query parameters either.

The dashboard should expose `sandbox_id` and `sandbox_pod_name` template
variables. The link fills those variables and the sandbox's creation-to-now
time window; unrelated query parameters in the configured URL are preserved.
The Datadog logs link remains available independently.
