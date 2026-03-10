import { defineConfig } from "vite";

export default defineConfig({
  server: {
    port: 5173,
    host: "127.0.0.1",
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8765",
        changeOrigin: true,
      },
      "/vendor": {
        target: "http://127.0.0.1:8765",
        changeOrigin: true,
      },
      "/favicon.ico": {
        target: "http://127.0.0.1:8765",
        changeOrigin: true,
      },
    },
  },
});
