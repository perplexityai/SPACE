# Admin UI

Run `npm ci` and `npm run dev` for local development, or `npm run build`
and `npm start` for a production build. Run `npm test` for the
dashboard URL validation tests.

## API proxy routing

`NEXT_PUBLIC_API_URL` selects the backend for `/api/*` and `/auth/*`, defaulting
to `http://localhost:8080`. Admin requests under `/admin/*` use the same backend
unless `NEXT_PUBLIC_ADMIN_API_URL` is set to a separate admin API origin. An
unset or empty admin override preserves the existing fallback.

## Curiosity theme

`src/lib/curiosity-theme.ts` provides a standalone light/dark Ant Design preset
and page/sidebar surfaces. `createCuriosityTheme` accepts the resolved dark-mode
preference and an optional organization primary color. An unset color uses the
preset accent; a configured color derives matching interaction and link states
without replacing status colors or surfaces. Table density remains page-owned.

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
