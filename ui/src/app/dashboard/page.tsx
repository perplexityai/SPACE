'use client';

import { useEffect, useState } from 'react';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { getConsoleSandboxes } from '@/lib/public-api';
import { Organization } from '@/lib/types';
import { Alert, Card, Col, Row, Statistic, Typography } from 'antd';
import { TeamOutlined, CloudServerOutlined } from '@ant-design/icons';

const { Title } = Typography;

export default function DashboardPage() {
  const { user } = useAuth();
  const [orgCount, setOrgCount] = useState<number | null>(null);
  const [sandboxCount, setSandboxCount] = useState<number | null>(null);
  const [errors, setErrors] = useState<string[]>([]);

  useEffect(() => {
    // Answers that land after the page is left are dropped.
    let active = true;
    const failed = (what: string) => (err: unknown) => {
      if (active) setErrors((current) => [...current, `${what}: ${err instanceof Error ? err.message : 'request failed'}`]);
    };
    adminApi.get<Organization[]>('/v1/orgs').then((orgs) => { if (active) setOrgCount(orgs.length); }).catch(failed('Organizations'));
    getConsoleSandboxes()
      .then((sbx) => { if (active) setSandboxCount(sbx.length); })
      .catch(failed('Sandboxes'));
    return () => {
      active = false;
    };
  }, []);

  return (
    <div>
      <Title level={3}>Dashboard</Title>
      <p>Welcome, {user?.display_name}</p>
      {errors.map((error) => (
        <Alert key={error} type="error" showIcon title={`Could not load ${error}`} style={{ marginBottom: 8 }} />
      ))}
      <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic title="Organizations" value={orgCount ?? '—'} prefix={<TeamOutlined />} />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic title="Sandboxes" value={sandboxCount ?? '—'} prefix={<CloudServerOutlined />} />
          </Card>
        </Col>
      </Row>
    </div>
  );
}
