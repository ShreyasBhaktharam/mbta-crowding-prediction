import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  return {
    plugins: [react()],
    define: {
      __APP_ENV__: env.APP_ENV,
    },
    server: {
      port: 5173,
      host: '0.0.0.0',
    },
    build: {
      outDir: 'dist',
    },
  };
});
