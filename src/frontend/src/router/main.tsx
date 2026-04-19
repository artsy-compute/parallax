// src/router/index.tsx
import { lazy, Suspense, useEffect } from 'react';
import { useLocation, useRoutes, Navigate, useNavigate } from 'react-router-dom';
import { useCluster } from '../services';
import { useConstCallback, useRefCallback } from '../hooks';

const PATH_SETUP = '/setup';
const PATH_CHAT = '/chat';
const PATH_LIBRARY = '/library';
const PATH_KNOWLEDGE = '/knowledge';
const PATH_NODES = '/nodes';
const PATH_SETTINGS = '/settings';

const PageSetup = lazy(() => import('../pages/setup'));
const PageChat = lazy(() => import('../pages/chat'));
const PageKnowledge = lazy(() => import('../pages/knowledge'));
const PageNodes = lazy(() => import('../pages/nodes'));
const PageSettings = lazy(() => import('../pages/settings'));

const debugLog = (...args: any[]) => {
  console.log('%c router.tsx ', 'color: white; background: purple;', ...args);
};

export const MainRouter = () => {
  const navigate = useNavigate();
  const { pathname } = useLocation();

  const [
    {
      clusterInfo: { status },
    },
  ] = useCluster();

  useEffect(() => {
    const lazyNavigate = (path: string) => {
      const timer = setTimeout(() => {
        debugLog('navigate to', path);
        navigate(path);
      }, 300);
      return () => clearTimeout(timer);
    };

    if (pathname === '/') {
      return lazyNavigate(PATH_SETTINGS);
    }
    debugLog('pathname', pathname, 'cluster status', status);
    if (status === 'idle' && pathname.startsWith(PATH_CHAT)) {
      return lazyNavigate(PATH_SETTINGS);
    }
    if (
      status === 'available'
      && !pathname.startsWith(PATH_CHAT)
      && !pathname.startsWith(PATH_LIBRARY)
      && !pathname.startsWith(PATH_KNOWLEDGE)
      && !pathname.startsWith(PATH_NODES)
      && !pathname.startsWith(PATH_SETTINGS)
    ) {
      return lazyNavigate(PATH_CHAT);
    }
  }, [navigate, pathname, status]);

  const routes = useRoutes([
    {
      path: PATH_SETUP,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageSetup />
        </Suspense>
      ),
    },
    {
      path: PATH_CHAT,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageChat />
        </Suspense>
      ),
    },
    {
      path: PATH_LIBRARY,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageKnowledge />
        </Suspense>
      ),
    },
    {
      path: PATH_KNOWLEDGE,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageKnowledge />
        </Suspense>
      ),
    },
    {
      path: `${PATH_KNOWLEDGE}/:pageId`,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageKnowledge />
        </Suspense>
      ),
    },
    {
      path: PATH_NODES,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageNodes />
        </Suspense>
      ),
    },
    {
      path: PATH_SETTINGS,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageSettings />
        </Suspense>
      ),
    },
    {
      path: `${PATH_SETTINGS}/:section`,
      element: (
        <Suspense fallback={<div>Loading...</div>}>
          <PageSettings />
        </Suspense>
      ),
    },
    {
      path: '*',
      element: <div>404 - Page Not Found</div>,
    },
  ]);
  return routes;
};
