import { parseDashboardUrl } from '@/lib/dashboard-url';
import SandboxDetailPage from './sandbox-detail';

export const dynamic = 'force-dynamic';

// An HTTPS URL without credentials from the environment, or null; a value
// that is set but invalid is reported without logging it.
function configuredLink(name: string, hides: string): string | null {
  const configured = process.env[name];
  const url = parseDashboardUrl(configured);
  if (configured?.trim() && !url) {
    console.warn(`${name} must be an HTTPS URL without credentials; hiding ${hides}`);
  }
  return url;
}

export default function Page() {
  return (
    <SandboxDetailPage
      dashboardUrl={configuredLink('SPACE_UI_DATADOG_DASHBOARD_URL', 'the dashboard link')}
      logsUrl={configuredLink('SPACE_UI_DATADOG_LOGS_URL', 'the logs link')}
    />
  );
}
