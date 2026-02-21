import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const defaultAllowedHosts = ['localhost', '127.0.0.1', '.localtest.me']
const envAllowedHosts = (process.env.VITE_ALLOWED_HOSTS || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [react()],
    server: {
        host: true,
        port: 3000,
        allowedHosts: [...new Set([...defaultAllowedHosts, ...envAllowedHosts])],
    }
})
