'use client';

import { useEffect, useState } from 'react';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { getConsoleSandboxes } from '@/lib/public-api';
import { Organization } from '@/lib/types';
import { Card, Col, Row, Statistic, Typography } from 'antd';
import { TeamOutlined, CloudServerOutlined } from '@ant-design/icons';

const { Title } = Typography;

export default function DashboardPage() {
  const { user } = useAuth();
  const [orgCount, setOrgCount] = useState(0);
  const [sandboxCount, setSandboxCount] = useState(0);

  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then((orgs) => setOrgCount(orgs.length)).catch(() => {});
    getConsoleSandboxes()
      .then((sbx) => setSandboxCount(sbx.length))
      .catch(() => {});
  }, []);

  return (
    <div>
      <Title level={3}>Dashboard</Title>
      <p>Welcome, {user?.display_name}</p>
      <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic title="Organizations" value={orgCount} prefix={<TeamOutlined />} />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic title="Sandboxes" value={sandboxCount} prefix={<CloudServerOutlined />} />
          </Card>
        </Col>
      </Row>
    </div>
  );
}
