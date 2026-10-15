'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/lib/auth';
import { useCuriosityTheme } from '@/lib/theme';
import { Button, Card, Form, Input, Alert, Typography, Flex } from 'antd';

const { Title } = Typography;

export default function LoginPage() {
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login } = useAuth();
  const { surfaces } = useCuriosityTheme();
  const router = useRouter();

  const handleSubmit = async (values: { email: string; password: string }) => {
    setError('');
    setLoading(true);
    try {
      await login(values.email, values.password);
      router.push('/dashboard');
    } catch (err: any) {
      setError(err.message || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Flex align="center" justify="center" style={{ minHeight: '100vh', background: surfaces.page }}>
      <Card style={{ width: 400, maxWidth: '100%' }}>
        <Title level={3} style={{ textAlign: 'center', marginBottom: 24 }}>
          SPACE Management
        </Title>
        {error && (
          <Alert message={error} type="error" showIcon closable style={{ marginBottom: 16 }} onClose={() => setError('')} />
        )}
        <Form layout="vertical" onFinish={handleSubmit} autoComplete="off">
          <Form.Item label="Email" name="email" rules={[{ required: true }]}>
            <Input size="large" />
          </Form.Item>
          <Form.Item label="Password" name="password" rules={[{ required: true }]}>
            <Input.Password size="large" />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" size="large" block loading={loading}>
              Log in
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </Flex>
  );
}
