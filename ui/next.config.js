/** @type {import('next').NextConfig} */
const defaultDataPlaneUrl =
  process.env.NODE_ENV === 'development' ? 'http://localhost:8090' : 'http://space-data-plane:8090';

const nextConfig = {
  output: 'standalone',
  experimental: {
    proxyTimeout: 600000,
  },
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080'}/api/:path*`,
      },
      {
        source: '/auth/:path*',
        destination: `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080'}/auth/:path*`,
      },
      {
        source: '/admin/:path*',
        destination: `${process.env.NEXT_PUBLIC_ADMIN_API_URL || process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080'}/admin/:path*`,
      },
      // Data-plane session endpoints — process/terminal/file-watch event
      // streams + websocket attaches. The api-server returns these as
      // relative `/v1/sessions/<id>/{events,attach,proxy}` URLs; the UI
      // resolves them against window.location.origin and would otherwise
      // hit the UI server (404) or, if the api returns absolute URLs to a
      // separate data-plane host, run into CORS preflight redirects on
      // gateway-fronted devboxes. Proxying through the UI keeps it
      // same-origin and avoids that whole class of failure.
      {
        source: '/v1/sessions/:path*',
        destination: `${process.env.NEXT_PUBLIC_DATA_PLANE_URL || defaultDataPlaneUrl}/v1/sessions/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
