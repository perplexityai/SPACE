'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/lib/auth';
import { ConsoleAccessProblem, SIGN_IN_AGAIN_PATH } from '@/lib/console-auth';
import { useCuriosityTheme } from '@/lib/theme';
import { Button, Card, Form, Input, Alert, Typography, Flex, Spin } from 'antd';

const { Title, Paragraph } = Typography;

const ACCESS_PROBLEM_TEXT: Record<ConsoleAccessProblem, { message: string; description: string }> = {
  unauthenticated: {
    message: 'Your sign-in could not be verified',
    description: 'Sign in again to continue. If this keeps happening, contact your administrator.',
  },
  forbidden: {
    message: 'You are not authorized to use this console',
    description: 'Your account is signed in but has not been granted console access. Contact your administrator.',
  },
  unavailable: {
    message: 'Sign-in is temporarily unavailable',
    description: 'The console could not verify your sign-in right now. Try again in a moment.',
  },
  error: {
    message: 'Something went wrong while signing you in',
    description: 'Try again in a moment.',
  },
};

export default function LoginPage() {
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const { user, loading, authMode, authModeError, accessProblem, login, logout, retry } = useAuth();
  const { surfaces } = useCuriosityTheme();
  const router = useRouter();

  const external = authMode?.mode === 'external';

  useEffect(() => {
    if (!loading && user && external) {
      router.replace('/dashboard');
    }
  }, [loading, user, external, router]);

  const handleSubmit = async (values: { email: string; password: string }) => {
    setError('');
    setSubmitting(true);
    try {
      await login(values.email, values.password);
      router.push('/dashboard');
    } catch (err: any) {
      setError(err.message || 'Login failed');
    } finally {
      setSubmitting(false);
    }
  };

  const title = (
    <Title level={3} style={{ textAlign: 'center', marginBottom: 24 }}>
      SPACE Management
    </Title>
  );

  let body;
  if (authModeError) {
    body = (
      <>
        <Alert
          type="error"
          showIcon
          message="Could not load sign-in settings"
          description="The console could not reach the SPACE API."
          style={{ marginBottom: 16 }}
        />
        <Button size="large" block onClick={retry}>
          Try again
        </Button>
      </>
    );
  } else if (!authMode || (external && (loading || user))) {
    body = (
      <Flex justify="center" style={{ padding: 24 }} data-testid="login-checking">
        <Spin />
      </Flex>
    );
  } else if (external) {
    const problem = accessProblem ? ACCESS_PROBLEM_TEXT[accessProblem] : null;
    body = (
      <>
        <Paragraph style={{ textAlign: 'center' }}>
          This console uses {authMode.providerName}. No SPACE password is needed.
        </Paragraph>
        {problem && (
          <Alert
            type={accessProblem === 'forbidden' ? 'warning' : 'error'}
            showIcon
            message={problem.message}
            description={problem.description}
            style={{ marginBottom: 16 }}
            data-testid={`access-problem-${accessProblem}`}
          />
        )}
        <Flex vertical gap={8}>
          {accessProblem === 'forbidden' ? null : (
            <Button
              type="primary"
              size="large"
              block
              onClick={() => {
                // A full navigation, so the proxy can run its sign-in flow.
                window.location.assign(SIGN_IN_AGAIN_PATH);
              }}
            >
              Continue with {authMode.providerName}
            </Button>
          )}
          <Button size="large" block onClick={() => logout()}>
            Sign out of {authMode.providerName}
          </Button>
        </Flex>
      </>
    );
  } else {
    body = (
      <>
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
            <Button type="primary" htmlType="submit" size="large" block loading={submitting}>
              Log in
            </Button>
          </Form.Item>
        </Form>
      </>
    );
  }

  return (
    <Flex align="center" justify="center" style={{ minHeight: '100vh', background: surfaces.page }}>
      <Card style={{ width: 400, maxWidth: '100%' }}>
        {title}
        {body}
      </Card>
    </Flex>
  );
}
