import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [tailwindcss(), react()],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:3000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      input: {
        home: path.resolve(__dirname, "index.html"),
        curation: path.resolve(__dirname, "curation.html"),
      },
      output: {
        manualChunks(id) {
          if (id.includes("node_modules/maplibre-gl")) {
            return "maplibre";
          }
        },
      },
    },
  },
});
