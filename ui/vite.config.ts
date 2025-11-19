import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import * as path from 'path';

// Avoid Node built-ins in browser bundle; let Vite resolve loaders.gl normally
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      child_process: path.resolve(__dirname, 'src/shims/empty.js'),
    },
  },
});
