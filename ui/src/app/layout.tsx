import { AuthProvider } from '@/lib/auth';
import { ThemeProvider } from '@/lib/theme';
import { AntdRegistry } from '@ant-design/nextjs-registry';
import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'SPACE Management',
  description: 'SPACE Sandbox Platform Management UI',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link rel="stylesheet" href="/fonts/inter/inter.css" />
      </head>
      <body style={{ margin: 0 }}>
        <AntdRegistry>
          <ThemeProvider>
            <AuthProvider>{children}</AuthProvider>
          </ThemeProvider>
        </AntdRegistry>
      </body>
    </html>
  );
}
