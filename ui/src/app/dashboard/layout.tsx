'use client';

import { useEffect, useRef, useState } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import Link from 'next/link';
import { useAuth } from '@/lib/auth';
import { useTheme, useCuriosityTheme } from '@/lib/theme';
import { consoleAccess } from '@/lib/console-access';
import { App, Layout, Menu, Typography, Button, ConfigProvider, Avatar, Space, Segmented, Modal, Result } from 'antd';
import {
  DashboardOutlined,
  TeamOutlined,
  UserOutlined,
  CloudServerOutlined,
  AppstoreOutlined,
  ClusterOutlined,
  KeyOutlined,
  LogoutOutlined,
  ReloadOutlined,
} from '@ant-design/icons';

const { Sider, Content } = Layout;
const { Text } = Typography;

type DashboardMenuItem = {
  key: string;
  icon: React.ReactNode;
  label: React.ReactNode;
};

// Every page reads the admin API, which answers only organization admins of
// the cluster's managing organization; other users get the notice below.
const menuItems: DashboardMenuItem[] = [
  { key: '/dashboard', icon: <DashboardOutlined />, label: <Link href="/dashboard">Overview</Link> },
  { key: '/dashboard/orgs', icon: <TeamOutlined />, label: <Link href="/dashboard/orgs">Organizations</Link> },
  { key: '/dashboard/users', icon: <UserOutlined />, label: <Link href="/dashboard/users">Users</Link> },
  { key: '/dashboard/api-keys', icon: <KeyOutlined />, label: <Link href="/dashboard/api-keys">API Keys</Link> },
  { key: '/dashboard/sandboxes', icon: <CloudServerOutlined />, label: <Link href="/dashboard/sandboxes">Sandboxes</Link> },
  { key: '/dashboard/credential-rules', icon: <KeyOutlined />, label: <Link href="/dashboard/credential-rules">Credential Rules</Link> },
  { key: '/dashboard/templates', icon: <AppstoreOutlined />, label: <Link href="/dashboard/templates">Templates</Link> },
  { key: '/dashboard/nodes', icon: <ClusterOutlined />, label: <Link href="/dashboard/nodes">Nodes</Link> },
];

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  const { user, org, cluster, loading, logout, reloadCluster, authMode } = useAuth();
  const [reloadingCluster, setReloadingCluster] = useState(false);
  const [clusterRetryError, setClusterRetryError] = useState<string | null>(null);
  const { mode, setMode, isDark } = useTheme();
  const { config, buttonClassName, primaryColor, surfaces } = useCuriosityTheme(org?.theme_primary);
  const router = useRouter();
  const pathname = usePathname();
  const [logoutConfirmationOpen, setLogoutConfirmationOpen] = useState(false);
  const [isLoggingOut, setIsLoggingOut] = useState(false);
  const logoutPending = useRef(false);

  const handleLogout = async () => {
    if (logoutPending.current) return;
    logoutPending.current = true;
    setIsLoggingOut(true);
    try {
      await logout();
    } finally {
      logoutPending.current = false;
      setIsLoggingOut(false);
      setLogoutConfirmationOpen(false);
    }
  };

  useEffect(() => {
    if (!loading && !user) {
      router.push('/login');
    }
  }, [loading, user, router]);

  if (loading || !user || !org) return null;

  const access = consoleAccess(user, cluster);
  const providerSignOutOnly = authMode?.mode === 'external' && !authMode.logoutUrl;
  const visibleItems = access === 'operator' ? menuItems : [];

  const retryCluster = async () => {
    setReloadingCluster(true);
    try {
      await reloadCluster();
      setClusterRetryError(null);
    } catch (err) {
      // The notice stays up with the error; the next Retry tries again.
      setClusterRetryError(err instanceof Error ? err.message : 'request failed');
    } finally {
      setReloadingCluster(false);
    }
  };

  const selectedKey = visibleItems
    .filter((item) => pathname.startsWith(item.key))
    .sort((a, b) => b.key.length - a.key.length)[0]?.key || '/dashboard';

  return (
    <ConfigProvider theme={config} button={{ className: buttonClassName }}>
      <App>
        <Layout style={{ minHeight: '100vh' }}>
          <Sider
            width={200}
            theme={isDark ? 'dark' : 'light'}
            style={{ background: surfaces.sidebar, borderRight: `1px solid ${surfaces.sidebarBorder}` }}
          >
            <div style={{ padding: '16px 24px', borderBottom: `1px solid ${surfaces.sidebarBorder}` }}>
              {org.logo_url ? (
                <img src={org.logo_url} alt={org.name} style={{ maxWidth: 160, marginBottom: 8 }} />
              ) : (
                <Text strong style={{ fontSize: 16 }}>{org.name}</Text>
              )}
            </div>
            <Menu
              mode="inline"
              selectedKeys={[selectedKey]}
              items={visibleItems}
              style={{ borderRight: 0, background: 'transparent' }}
            />
            <div style={{ position: 'absolute', bottom: 0, width: '100%', padding: '16px 24px', borderTop: `1px solid ${surfaces.sidebarBorder}` }}>
              <Space orientation="vertical" size={8} style={{ width: '100%' }}>
                <Segmented
                  size="small"
                  block
                  value={mode}
                  onChange={(v) => setMode(v as 'light' | 'dark' | 'auto')}
                  options={[
                    { label: 'Light', value: 'light' },
                    { label: 'Dark', value: 'dark' },
                    { label: 'Auto', value: 'auto' },
                  ]}
                />
                <Space>
                  <Avatar size="small" icon={<UserOutlined />} style={{ background: primaryColor }} />
                  <div>
                    <Text style={{ fontSize: 13 }}>{user.display_name}</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: 11 }}>{user.role}</Text>
                  </div>
                </Space>
                <Button type="text" icon={<LogoutOutlined />} onClick={() => setLogoutConfirmationOpen(true)} size="small" block>
                  Log out
                </Button>
              </Space>
            </div>
          </Sider>
          <Content style={{ padding: 24, minWidth: 0, overflowX: 'auto', background: surfaces.page }}>
            {access === 'operator' ? children : access === 'cluster-unknown' ? (
              <Result
                status="warning"
                title="Could not load cluster information"
                subTitle={
                  <>
                    The console needs the cluster record to tell whether your organization manages this cluster.
                    {clusterRetryError ? <><br />Retry failed: {clusterRetryError}</> : null}
                  </>
                }
                extra={
                  <Button type="primary" icon={<ReloadOutlined />} loading={reloadingCluster} onClick={retryCluster}>
                    Retry
                  </Button>
                }
              />
            ) : (
              <Result
                status="403"
                title="This console is for cluster operators"
                subTitle={
                  <>
                    You are signed in as {user.email} ({org.name}). The admin console requires an organization admin of the
                    organization that manages this cluster.
                    {providerSignOutOnly ? (
                      <><br />To use another account, sign out of {authMode?.mode === 'external' ? authMode.providerName : 'your sign-in provider'}.</>
                    ) : null}
                  </>
                }
                extra={
                  // External sign-in without a logout URL can only return to
                  // /login, which signs the same account in again.
                  providerSignOutOnly ? null : (
                    <Button type="primary" icon={<LogoutOutlined />} onClick={() => setLogoutConfirmationOpen(true)}>
                      Log out
                    </Button>
                  )
                }
              />
            )}
          </Content>
        </Layout>
        <Modal
          title="Log out?"
          open={logoutConfirmationOpen}
          onOk={handleLogout}
          onCancel={() => {
            if (!logoutPending.current) setLogoutConfirmationOpen(false);
          }}
          okText="Log out"
          cancelText="Cancel"
          confirmLoading={isLoggingOut}
          cancelButtonProps={{ disabled: isLoggingOut }}
          closable={!isLoggingOut}
          keyboard={!isLoggingOut}
          mask={{ closable: !isLoggingOut }}
        >
          Are you sure you want to log out of SPACE?
        </Modal>
      </App>
    </ConfigProvider>
  );
}
