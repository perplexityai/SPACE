'use client';

import { createContext, useCallback, useContext, useEffect, useState, ReactNode } from 'react';
import { ApiError, authApi, fetchConsoleAuthMode, setConsoleAuthMode } from './api';
import { ConsoleAccessProblem, ConsoleAuthMode, accessProblemForStatus } from './console-auth';
import { Cluster, LoginResponse, MeResponse, Organization, Substrate, User } from './types';

interface AuthContextType {
  user: User | null;
  org: Organization | null;
  cluster: Cluster | null;
  substrate: Substrate | null;
  loading: boolean;
  // `null` until discovery succeeds; see `authModeError`.
  authMode: ConsoleAuthMode | null;
  authModeError: boolean;
  // External mode only: why no user is signed in.
  accessProblem: ConsoleAccessProblem | null;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  retry: () => void;
  refreshOrg: (org: Organization) => void;
  refreshCluster: (cluster: Cluster) => void;
  // Refetch `GET /auth/v1/cluster`, for when it failed after sign-in.
  reloadCluster: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType>({
  user: null,
  org: null,
  cluster: null,
  substrate: null,
  loading: true,
  authMode: null,
  authModeError: false,
  accessProblem: null,
  login: async () => {},
  logout: async () => {},
  retry: () => {},
  refreshOrg: () => {},
  refreshCluster: () => {},
  reloadCluster: async () => {},
});

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [org, setOrg] = useState<Organization | null>(null);
  const [cluster, setCluster] = useState<Cluster | null>(null);
  const [substrate, setSubstrate] = useState<Substrate | null>(null);
  const [loading, setLoading] = useState(true);
  const [authMode, setAuthMode] = useState<ConsoleAuthMode | null>(null);
  const [authModeError, setAuthModeError] = useState(false);
  const [accessProblem, setAccessProblem] = useState<ConsoleAccessProblem | null>(null);
  const [attempt, setAttempt] = useState(0);

  const clearSignedIn = () => {
    setUser(null);
    setOrg(null);
    setCluster(null);
    setSubstrate(null);
  };

  useEffect(() => {
    let cancelled = false;

    const signedIn = (me: MeResponse, current: Cluster | null) => {
      if (cancelled) return;
      setUser(me.user);
      setOrg(me.org);
      setCluster(current);
      setSubstrate(me.substrate ?? null);
    };

    const start = async () => {
      setLoading(true);
      setAuthModeError(false);
      setAccessProblem(null);

      let mode: ConsoleAuthMode;
      try {
        mode = await fetchConsoleAuthMode();
      } catch {
        if (!cancelled) {
          setAuthModeError(true);
          setLoading(false);
        }
        return;
      }
      if (cancelled) return;
      setConsoleAuthMode(mode);
      setAuthMode(mode);

      if (mode.mode === 'external') {
        // A password session from before the mode changed is never sent and
        // never reused.
        localStorage.removeItem('token');
        try {
          // A failed cluster read is not a failed sign-in: the console
          // offers a retry for it (see `consoleAccess`).
          const [me, current] = await Promise.all([
            authApi.probe<MeResponse>('/me'),
            authApi.probe<Cluster>('/cluster').catch(() => null),
          ]);
          signedIn(me, current);
        } catch (err) {
          if (!cancelled) {
            clearSignedIn();
            setAccessProblem(err instanceof ApiError ? accessProblemForStatus(err.status) : 'error');
          }
        }
        if (!cancelled) setLoading(false);
        return;
      }

      const token = localStorage.getItem('token');
      if (!token) {
        setLoading(false);
        return;
      }
      try {
        // As at sign-in, a failed cluster read leaves the session in place
        // and the console offers a retry; only a failed `/me` discards it.
        const [me, current] = await Promise.all([
          authApi.get<MeResponse>('/me'),
          authApi.get<Cluster>('/cluster').catch(() => null),
        ]);
        signedIn(me, current);
      } catch {
        localStorage.removeItem('token');
      }
      if (!cancelled) setLoading(false);
    };

    start();
    return () => {
      cancelled = true;
    };
  }, [attempt]);

  const login = async (email: string, password: string) => {
    if (authMode?.mode !== 'password') {
      throw new Error('Password sign-in is disabled for this console');
    }
    const data = await authApi.post<LoginResponse>('/sessions', { email, password });
    localStorage.setItem('token', data.token);
    setUser(data.user);
    setOrg(data.org);
    const [me, current] = await Promise.all([
      authApi.get<MeResponse>('/me').catch(() => null),
      authApi.get<Cluster>('/cluster').catch(() => null),
    ]);
    setCluster(current);
    setSubstrate(me?.substrate ?? null);
  };

  const logout = async () => {
    if (authMode?.mode === 'external') {
      try {
        await authApi.delete('/session');
      } catch {
        // Nothing is revoked server-side in external mode; ending the
        // provider session below is what signs the browser out.
      }
      // Leave the signed-in state alone: clearing it would race a client-side
      // redirect to /login against the provider logout navigation.
      window.location.assign(authMode.logoutUrl ?? '/login');
      return;
    }
    try {
      await authApi.delete('/session');
    } finally {
      localStorage.removeItem('token');
      clearSignedIn();
    }
  };

  const retry = useCallback(() => setAttempt((value) => value + 1), []);

  const refreshOrg = (updated: Organization) => {
    setOrg(updated);
  };

  const refreshCluster = (updated: Cluster) => {
    setCluster(updated);
  };

  const reloadCluster = async () => {
    const current =
      authMode?.mode === 'external' ? await authApi.probe<Cluster>('/cluster') : await authApi.get<Cluster>('/cluster');
    setCluster(current);
  };

  return (
    <AuthContext.Provider
      value={{
        user,
        org,
        cluster,
        substrate,
        loading,
        authMode,
        authModeError,
        accessProblem,
        login,
        logout,
        retry,
        refreshOrg,
        refreshCluster,
        reloadCluster,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
