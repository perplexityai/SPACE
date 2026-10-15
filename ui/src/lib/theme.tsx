'use client';

import { createContext, useContext, useEffect, useMemo, useState, ReactNode } from 'react';
import { ConfigProvider } from 'antd';
import { createCuriosityTheme } from './curiosity-theme';

type ThemeMode = 'light' | 'dark' | 'auto';

interface ThemeContextType {
  mode: ThemeMode;
  setMode: (mode: ThemeMode) => void;
  isDark: boolean;
}

const ThemeContext = createContext<ThemeContextType>({
  mode: 'auto',
  setMode: () => {},
  isDark: false,
});

function getSystemDark() {
  if (typeof window === 'undefined') return false;
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ThemeMode>('auto');
  const [systemDark, setSystemDark] = useState(false);

  useEffect(() => {
    const stored = localStorage.getItem('theme-mode') as ThemeMode | null;
    if (stored && ['light', 'dark', 'auto'].includes(stored)) {
      setModeState(stored);
    }
    setSystemDark(getSystemDark());

    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  const setMode = (m: ThemeMode) => {
    setModeState(m);
    localStorage.setItem('theme-mode', m);
  };

  const isDark = mode === 'dark' || (mode === 'auto' && systemDark);
  const appearance = useMemo(() => createCuriosityTheme({ isDark }), [isDark]);

  useEffect(() => {
    document.documentElement.style.colorScheme = isDark ? 'dark' : 'light';
  }, [isDark]);

  return (
    <ThemeContext.Provider value={{ mode, setMode, isDark }}>
      <ConfigProvider theme={appearance.config}>{children}</ConfigProvider>
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  return useContext(ThemeContext);
}

export function useCuriosityTheme(primaryColor?: string | null) {
  const { isDark } = useTheme();
  return useMemo(() => createCuriosityTheme({ isDark, primaryColor }), [isDark, primaryColor]);
}
