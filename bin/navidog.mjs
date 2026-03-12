#!/usr/bin/env node

import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { spawn } from 'node:child_process'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const serverPath = path.resolve(__dirname, '..', 'server', 'index.ts')
const port = process.env.PORT || '3001'

console.log(`\n  🐕 NaviDog starting on http://localhost:${port}\n`)

const child = spawn(
  process.execPath,
  ['--import', 'tsx', serverPath],
  {
    stdio: 'inherit',
    env: { ...process.env, PORT: port },
    cwd: path.resolve(__dirname, '..'),
  }
)

child.on('exit', (code) => process.exit(code ?? 0))
process.on('SIGINT', () => child.kill('SIGINT'))
process.on('SIGTERM', () => child.kill('SIGTERM'))
