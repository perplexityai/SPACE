'use client';

import { useEffect, useRef, useState } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import Link from 'next/link';
import { useAuth } from '@/lib/auth';
import { useTheme, useAntAlgorithm } from '@/lib/theme';
import { UserRole } from '@/lib/types';
import { isManagingOrgAdmin } from '@/lib/userRoles';
import { Layout, Menu, Typography, Button, ConfigProvider, Avatar, Space, Segmented, Modal } from 'antd';
import {
  DashboardOutlined,
  TeamOutlined,
  UserOutlined,
  CloudServerOutlined,
  AppstoreOutlined,
  ClusterOutlined,
  KeyOutlined,
  LogoutOutlined,
} from '@ant-design/icons';

const { Sider, Content } = Layout;
const { Text } = Typography;

type DashboardMenuItem = {
  key: string;
  icon: React.ReactNode;
  label: React.ReactNode;
  roles?: UserRole[];
  managingOrgAdmin?: boolean;
};

const menuItems: DashboardMenuItem[] = [
  { key: '/dashboard', icon: <DashboardOutlined />, label: <Link href="/dashboard">Overview</Link> },
  { key: '/dashboard/orgs', icon: <TeamOutlined />, label: <Link href="/dashboard/orgs">Organizations</Link>, roles: ['organization_admin'] },
  { key: '/dashboard/users', icon: <UserOutlined />, label: <Link href="/dashboard/users">Users</Link>, roles: ['organization_admin'] },
  { key: '/dashboard/api-keys', icon: <KeyOutlined />, label: <Link href="/dashboard/api-keys">API Keys</Link>, managingOrgAdmin: true },
  { key: '/dashboard/sandboxes', icon: <CloudServerOutlined />, label: <Link href="/dashboard/sandboxes">Sandboxes</Link> },
  { key: '/dashboard/credential-rules', icon: <KeyOutlined />, label: <Link href="/dashboard/credential-rules">Credential Rules</Link>, managingOrgAdmin: true },
  { key: '/dashboard/templates', icon: <AppstoreOutlined />, label: <Link href="/dashboard/templates">Templates</Link>, managingOrgAdmin: true },
  { key: '/dashboard/nodes', icon: <ClusterOutlined />, label: <Link href="/dashboard/nodes">Nodes</Link>, managingOrgAdmin: true },
];

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  const { user, org, cluster, loading, logout } = useAuth();
  const { mode, setMode, isDark } = useTheme();
  const algorithm = useAntAlgorithm();
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

  const managingOrgAdmin = isManagingOrgAdmin(user, cluster);
  const visibleItems = menuItems.filter((item) => {
    if (item.managingOrgAdmin) return managingOrgAdmin;
    return !item.roles || item.roles.includes(user.role);
  });

  const selectedKey = visibleItems
    .filter((item) => pathname.startsWith(item.key))
    .sort((a, b) => b.key.length - a.key.length)[0]?.key || '/dashboard';

  const primary = org.theme_primary || '#1677ff';

  const sidebarBg = isDark
    ? `color-mix(in srgb, ${primary} 8%, #141414)`
    : `color-mix(in srgb, ${primary} 10%, #ffffff)`;
  const sidebarBorder = isDark
    ? `color-mix(in srgb, ${primary} 12%, #303030)`
    : `color-mix(in srgb, ${primary} 15%, #e8e8e8)`;
  const contentBg = isDark ? '#141414' : '#fff';

  return (
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: primary,
        },
        algorithm,
      }}
    >
      <Layout style={{ minHeight: '100vh' }}>
        <Sider
          width={200}
          theme={isDark ? 'dark' : 'light'}
          style={{ background: sidebarBg, borderRight: `1px solid ${sidebarBorder}` }}
        >
          <div style={{ padding: '16px 24px', borderBottom: `1px solid ${sidebarBorder}` }}>
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
          <div style={{ position: 'absolute', bottom: 0, width: '100%', padding: '16px 24px', borderTop: `1px solid ${sidebarBorder}` }}>
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
                <Avatar size="small" icon={<UserOutlined />} style={{ background: primary }} />
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
        <Content style={{ padding: 24, background: contentBg }}>{children}</Content>
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
    </ConfigProvider>
  );
}
