#!/usr/bin/env node

import { execFile, spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import process from 'node:process'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const rootDir = path.resolve(__dirname, '..')
const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm'
const currentPid = process.pid
const serverPort = readPort(process.env.PORT, 3002, 'PORT')
const webPort = readPort(process.env.NAVIDOG_WEB_PORT, 5173, 'NAVIDOG_WEB_PORT')
const childProcesses = []

let shuttingDown = false
let shutdownPromise

function readPort(rawValue, fallback, envName) {
  if (!rawValue) {
    return fallback
  }

  const parsed = Number.parseInt(String(rawValue), 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${envName} must be a positive integer, received "${rawValue}".`)
  }

  return parsed
}

async function runCommand(file, args, { allowExitCodes = [0], maxBuffer = 1024 * 1024 } = {}) {
  try {
    return await execFileAsync(file, args, {
      cwd: rootDir,
      maxBuffer,
    })
  } catch (error) {
    const exitCode = typeof error?.code === 'number' ? error.code : undefined
    if (exitCode != null && allowExitCodes.includes(exitCode)) {
      return {
        stdout: error.stdout ?? '',
        stderr: error.stderr ?? '',
      }
    }
    throw error
  }
}

async function getProcessTable() {
  const { stdout } = await runCommand('ps', ['-Ao', 'pid=,command='])

  return stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const match = /^(\d+)\s+(.*)$/.exec(line)
      if (!match) {
        return null
      }

      return {
        pid: Number.parseInt(match[1], 10),
        command: match[2],
      }
    })
    .filter((entry) => entry && Number.isFinite(entry.pid))
}

function looksLikeDevCommand(command) {
  return (
    command.includes('server/index.ts')
    || command.includes('tsx watch')
    || command.includes('node_modules/vite/bin/vite.js')
    || command.includes(`${path.sep}scripts${path.sep}dev.mjs`)
  )
}

async function getProcessCwd(pid) {
  const { stdout } = await runCommand(
    'lsof',
    ['-a', '-p', String(pid), '-d', 'cwd', '-Fn'],
    { allowExitCodes: [0, 1] },
  )

  const cwdLine = stdout
    .split(/\r?\n/)
    .find((line) => line.startsWith('n'))

  return cwdLine ? path.resolve(cwdLine.slice(1)) : null
}

function parseListenerPort(name) {
  const match = /:(\d+)\s+\(LISTEN\)$/.exec(name)
  return match ? Number.parseInt(match[1], 10) : null
}

async function listPortListeners(ports) {
  const args = ['-nP']
  for (const port of ports) {
    args.push(`-iTCP:${port}`)
  }
  args.push('-sTCP:LISTEN', '-Fpcn')

  const { stdout } = await runCommand('lsof', args, { allowExitCodes: [0, 1] })
  const listeners = []
  let current = null

  for (const line of stdout.split(/\r?\n/)) {
    if (!line) {
      continue
    }

    const kind = line[0]
    const value = line.slice(1)

    if (kind === 'p') {
      if (current) {
        listeners.push(current)
      }

      current = {
        pid: Number.parseInt(value, 10),
        command: '',
        name: '',
      }
      continue
    }

    if (!current) {
      continue
    }

    if (kind === 'c') {
      current.command = value
      continue
    }

    if (kind === 'n') {
      current.name = value
    }
  }

  if (current) {
    listeners.push(current)
  }

  return listeners.filter((listener) => Number.isFinite(listener.pid))
}

function isProcessRunning(pid) {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    if (error?.code === 'ESRCH') {
      return false
    }
    throw error
  }
}

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

async function waitForPidExit(pid, timeoutMs) {
  const startedAt = Date.now()

  while (Date.now() - startedAt < timeoutMs) {
    if (!isProcessRunning(pid)) {
      return true
    }
    await delay(150)
  }

  return !isProcessRunning(pid)
}

async function terminatePid(pid, reason) {
  if (!isProcessRunning(pid)) {
    return
  }

  process.kill(pid, 'SIGTERM')
  const exitedGracefully = await waitForPidExit(pid, 2_500)
  if (exitedGracefully) {
    console.log(`[dev] Cleaned stale process ${pid} (${reason}).`)
    return
  }

  process.kill(pid, 'SIGKILL')
  const exitedForcefully = await waitForPidExit(pid, 1_000)
  if (exitedForcefully) {
    console.warn(`[dev] Force-killed stale process ${pid} (${reason}).`)
    return
  }

  throw new Error(`Process ${pid} did not exit after cleanup attempts.`)
}

async function cleanupStaleProjectProcesses() {
  const processTable = await getProcessTable()
  const listeners = await listPortListeners([serverPort, webPort])
  const candidates = new Map()

  for (const entry of processTable) {
    if (entry.pid === currentPid || !looksLikeDevCommand(entry.command)) {
      continue
    }
    candidates.set(entry.pid, entry)
  }

  for (const listener of listeners) {
    if (listener.pid === currentPid) {
      continue
    }

    if (!candidates.has(listener.pid)) {
      candidates.set(listener.pid, {
        pid: listener.pid,
        command: listener.command || listener.name,
      })
    }
  }

  const staleProcesses = []
  for (const candidate of candidates.values()) {
    const cwd = await getProcessCwd(candidate.pid)
    if (cwd === rootDir) {
      staleProcesses.push({
        ...candidate,
        cwd,
      })
    }
  }

  if (staleProcesses.length === 0) {
    return
  }

  console.log(`[dev] Cleaning ${staleProcesses.length} stale NaviDog process(es)...`)
  for (const processInfo of staleProcesses) {
    await terminatePid(processInfo.pid, processInfo.command)
  }
}

async function ensurePortsAvailable() {
  const listeners = await listPortListeners([serverPort, webPort])
  const conflicts = []

  for (const listener of listeners) {
    const cwd = await getProcessCwd(listener.pid)
    if (cwd === rootDir) {
      await terminatePid(listener.pid, listener.command || listener.name)
      continue
    }

    conflicts.push({
      ...listener,
      cwd,
      port: parseListenerPort(listener.name),
    })
  }

  if (conflicts.length === 0) {
    return
  }

  const details = conflicts
    .map((conflict) => {
      const portLabel = conflict.port ?? conflict.name
      const cwdLabel = conflict.cwd ? ` cwd=${conflict.cwd}` : ''
      return `- port ${portLabel}: pid ${conflict.pid} (${conflict.command || 'unknown'})${cwdLabel}`
    })
    .join('\n')

  throw new Error(
    [
      'Ports required by NaviDog dev are already occupied by another project/process:',
      details,
      `Use a different PORT / NAVIDOG_WEB_PORT, or stop the conflicting process first.`,
    ].join('\n'),
  )
}

function waitForChildExit(child, timeoutMs) {
  if (child.exitCode != null || child.signalCode != null) {
    return Promise.resolve(true)
  }

  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      cleanup()
      resolve(false)
    }, timeoutMs)

    const handleExit = () => {
      cleanup()
      resolve(true)
    }

    const cleanup = () => {
      clearTimeout(timer)
      child.off('exit', handleExit)
    }

    child.on('exit', handleExit)
  })
}

async function stopChildProcess(record) {
  const { child } = record
  if (!child.pid || child.exitCode != null || child.signalCode != null) {
    return
  }

  try {
    if (process.platform === 'win32') {
      child.kill('SIGTERM')
    } else {
      process.kill(-child.pid, 'SIGTERM')
    }
  } catch (error) {
    if (error?.code !== 'ESRCH') {
      throw error
    }
  }

  const exitedGracefully = await waitForChildExit(child, 2_500)
  if (exitedGracefully) {
    return
  }

  try {
    if (process.platform === 'win32') {
      child.kill('SIGKILL')
    } else {
      process.kill(-child.pid, 'SIGKILL')
    }
  } catch (error) {
    if (error?.code !== 'ESRCH') {
      throw error
    }
  }

  await waitForChildExit(child, 1_000)
}

function startChildProcess(name, scriptName, extraEnv) {
  const child = spawn(npmCommand, ['run', scriptName], {
    cwd: rootDir,
    env: {
      ...process.env,
      ...extraEnv,
    },
    stdio: 'inherit',
    detached: process.platform !== 'win32',
  })

  const record = { name, child }
  childProcesses.push(record)

  child.on('error', (error) => {
    if (shuttingDown) {
      return
    }

    console.error(`[dev] Failed to start ${name}:`, error)
    void shutdown(1)
  })

  child.on('exit', (code, signal) => {
    if (shuttingDown) {
      return
    }

    const exitLabel = signal ? `signal ${signal}` : `code ${code ?? 0}`
    console.error(`[dev] ${name} exited unexpectedly (${exitLabel}).`)
    void shutdown(typeof code === 'number' ? code : 1)
  })
}

async function shutdown(exitCode) {
  if (shutdownPromise) {
    return shutdownPromise
  }

  shuttingDown = true
  shutdownPromise = (async () => {
    await Promise.allSettled(childProcesses.map((record) => stopChildProcess(record)))
    process.exit(exitCode)
  })()

  return shutdownPromise
}

async function main() {
  console.log(`[dev] NaviDog web: http://127.0.0.1:${webPort}`)
  console.log(`[dev] NaviDog api: http://127.0.0.1:${serverPort}`)

  await cleanupStaleProjectProcesses()
  await ensurePortsAvailable()

  startChildProcess('server', 'dev:server', {
    PORT: String(serverPort),
  })

  startChildProcess('web', 'dev:web', {
    PORT: String(serverPort),
    NAVIDOG_WEB_PORT: String(webPort),
  })
}

process.once('SIGINT', () => {
  void shutdown(130)
})

process.once('SIGTERM', () => {
  void shutdown(143)
})

main().catch((error) => {
  console.error(`[dev] ${error instanceof Error ? error.message : String(error)}`)
  process.exit(1)
})
