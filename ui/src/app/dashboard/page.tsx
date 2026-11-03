'use client';

import { useEffect, useState } from 'react';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { getConsoleSandboxes, getSpacedVersionCounts } from '@/lib/public-api';
import { DEFAULT_LIST_LIMIT } from '@/lib/sandbox-id';
import { loadSpacedCounts, type SpacedCounts } from '@/lib/spaced-counts';
import { Organization } from '@/lib/types';
import { Alert, Card, Col, Row, Statistic, Typography } from 'antd';
import { TeamOutlined, CloudServerOutlined } from '@ant-design/icons';

const { Title, Text } = Typography;

export default function DashboardPage() {
  const { user } = useAuth();
  const [orgCount, setOrgCount] = useState<number | null>(null);
  const [sandboxCounts, setSandboxCounts] = useState<SpacedCounts | null>(null);
  const [errors, setErrors] = useState<string[]>([]);

  useEffect(() => {
    // Answers that land after the page is left are dropped.
    let active = true;
    const failed = (what: string) => (err: unknown) => {
      if (active) setErrors((current) => [...current, `${what}: ${err instanceof Error ? err.message : 'request failed'}`]);
    };
    adminApi.get<Organization[]>('/v1/orgs').then((orgs) => { if (active) setOrgCount(orgs.length); }).catch(failed('Organizations'));
    // The server's count of every sandbox; a server without the count route
    // gets today's tally of the newest list rows.
    loadSpacedCounts(getSpacedVersionCounts, () => getConsoleSandboxes(DEFAULT_LIST_LIMIT), DEFAULT_LIST_LIMIT)
      .then((counts) => { if (active) setSandboxCounts(counts); })
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
            <Statistic
              title="Sandboxes"
              value={sandboxCounts === null ? '—' : `${sandboxCounts.listed}${sandboxCounts.truncated ? '+' : ''}`}
              prefix={<CloudServerOutlined />}
            />
            {sandboxCounts ? (
              <Text type="secondary" style={{ fontSize: 12 }}>
                {sandboxCounts.source === 'server'
                  ? 'Not deleted, in every organization'
                  : `Not deleted, among the newest ${DEFAULT_LIST_LIMIT.toLocaleString('en-US')} listed`}
              </Text>
            ) : null}
          </Card>
        </Col>
      </Row>
    </div>
  );
}
