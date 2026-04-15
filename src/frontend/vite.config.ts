import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react-swc';

const __dirname = dirname(fileURLToPath(import.meta.url));
const backendHost = process.env.PARALLAX_BACKEND_HOST || '127.0.0.1';
const backendPort = process.env.PARALLAX_BACKEND_PORT || '3001';
const backendTarget = `http://${backendHost}:${backendPort}`;

// https://vite.dev/config/
export default defineConfig({
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        chat: resolve(__dirname, 'chat.html'),
      },
    },
  },
  plugins: [react()],
  server: {
    proxy: {
      '/proxy-api/v1/chat/completions': {
        target: backendTarget,
        rewrite: (path) => path.replace(/^\/proxy-api/, ''),
      },
      '/proxy-api': {
        target: backendTarget,
        rewrite: (path) => path.replace(/^\/proxy-api/, ''),
      },
    },
  },
});
