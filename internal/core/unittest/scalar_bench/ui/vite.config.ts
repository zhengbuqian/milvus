import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  base: './',
  server: {
    strictPort: true,
    proxy: {
      '/_artifacts': {
        target: 'http://127.0.0.1:4173',
        changeOrigin: true,
      },
    },
  },
});

