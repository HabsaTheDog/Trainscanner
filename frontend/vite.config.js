import path from 'node:path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        home: path.resolve(__dirname, 'index.html'),
        curation: path.resolve(__dirname, 'curation.html')
      }
    }
  }
});
