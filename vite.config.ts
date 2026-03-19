import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

function readPort(rawValue: string | undefined, fallback: number) {
  const parsed = Number.parseInt(rawValue ?? '', 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

const devServerPort = readPort(process.env.NAVIDOG_WEB_PORT, 5173)
const devApiPort = readPort(process.env.PORT, 3002)
const devApiTarget = `http://127.0.0.1:${devApiPort}`

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: devServerPort,
    strictPort: true,
    proxy: {
      '/api': {
        target: devApiTarget,
        changeOrigin: true,
      },
    },
  },
})
