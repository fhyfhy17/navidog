import crypto from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import type { Client, ClientChannel, ConnectConfig } from 'ssh2'

import type { ConnectionConfig } from './mysql.js'

type RawSshTarget = {
  host?: unknown
  port?: unknown
  user?: unknown
}

export type SshExecRequest = {
  command?: unknown
  timeoutMs?: unknown
  sshTarget?: RawSshTarget
}

export type SshExecResult = {
  host: string
  port: number
  user: string
  command: string
  stdout: string
  stderr: string
  exitCode: number | null
  signal?: string
  durationMs: number
  truncated: boolean
}

export type SshShellSessionStartRequest = {
  sshTarget?: RawSshTarget
  cols?: unknown
  rows?: unknown
  term?: unknown
  idleTimeoutMs?: unknown
}

export type SshShellSessionReadRequest = {
  sessionId?: unknown
  afterSeq?: unknown
  limit?: unknown
}

export type SshShellSessionInputRequest = {
  sessionId?: unknown
  input?: unknown
  appendNewline?: unknown
}

export type SshShellSessionResizeRequest = {
  sessionId?: unknown
  cols?: unknown
  rows?: unknown
}

export type SshShellSessionCloseRequest = {
  sessionId?: unknown
}

type ShellChunkStream = 'stdout' | 'stderr' | 'system'

export type SshShellChunk = {
  seq: number
  stream: ShellChunkStream
  text: string
  timestamp: number
}

export type SshShellSessionStartResult = {
  sessionId: string
  host: string
  port: number
  user: string
  nextSeq: number
  createdAt: number
}

export type SshShellSessionReadResult = {
  sessionId: string
  host: string
  port: number
  user: string
  chunks: SshShellChunk[]
  nextSeq: number
  closed: boolean
  exitCode: number | null
  signal?: string
  lastActiveAt: number
}

type ShellSessionRecord = {
  id: string
  host: string
  port: number
  user: string
  client: Client
  stream: ClientChannel
  createdAt: number
  lastActiveAt: number
  idleTimeoutMs: number
  nextSeq: number
  chunks: SshShellChunk[]
  closed: boolean
  exitCode: number | null
  signal?: string
}

const DEFAULT_TIMEOUT_MS = 15_000
const MAX_TIMEOUT_MS = 60_000
const MAX_OUTPUT_BYTES = 64 * 1024

const DEFAULT_SHELL_IDLE_TIMEOUT_MS = 15 * 60 * 1000
const MAX_SHELL_IDLE_TIMEOUT_MS = 60 * 60 * 1000
const DEFAULT_SHELL_COLS = 160
const DEFAULT_SHELL_ROWS = 36
const MAX_SHELL_SESSIONS = 24
const MAX_SHELL_CHUNKS = 2_400
const MAX_SHELL_CHUNK_BYTES = 8 * 1024

const shellSessions = new Map<string, ShellSessionRecord>()
let shellGcTimer: NodeJS.Timeout | undefined

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value))
}

function parsePositiveInt(value: unknown, fallback: number) {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function parseNonNegativeInt(value: unknown, fallback: number) {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback
}

function readBoolean(value: unknown, fallback: boolean) {
  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (['true', '1', 'yes', 'on'].includes(normalized)) {
      return true
    }
    if (['false', '0', 'no', 'off', ''].includes(normalized)) {
      return false
    }
  }

  return fallback
}

function normalizeTarget(target: RawSshTarget | undefined) {
  return {
    host: target?.host ? String(target.host).trim() : '',
    port: target?.port ? parsePositiveInt(target.port, 22) : undefined,
    user: target?.user ? String(target.user).trim() : '',
  }
}

function resolveHomePath(filePath: string) {
  if (!filePath.startsWith('~')) {
    return filePath
  }

  return path.join(os.homedir(), filePath.slice(1))
}

function buildSshConfig(connection: ConnectionConfig, target: ReturnType<typeof normalizeTarget>) {
  const host = target.host || connection.sshHost || ''
  const port = target.port ?? connection.sshPort ?? 22
  const user = target.user || connection.sshUser || 'root'

  if (!host) {
    throw new Error('缺少 SSH 主机。请先在连接里配置 SSH，或在工作台里填写目标主机。')
  }

  const sshConfig: ConnectConfig = {
    host,
    port,
    username: user,
    keepaliveInterval: 15_000,
    keepaliveCountMax: 3,
    readyTimeout: 15_000,
  }

  if (connection.sshAuthMethod === 'password') {
    sshConfig.password = connection.sshPassword ?? ''
  } else {
    const keyPath = resolveHomePath(connection.sshPrivateKey ?? '~/.ssh/id_rsa')
    try {
      sshConfig.privateKey = fs.readFileSync(keyPath)
    } catch (error) {
      throw new Error(
        `无法读取 SSH 私钥: ${keyPath}${error instanceof Error ? ` (${error.message})` : ''}`,
      )
    }

    if (connection.sshPassphrase) {
      sshConfig.passphrase = connection.sshPassphrase
    }
  }

  return { host, port, user, sshConfig }
}

function appendLimited(current: string, usedBytes: number, incoming: Buffer | string) {
  const chunk = Buffer.isBuffer(incoming) ? incoming : Buffer.from(incoming)
  if (usedBytes >= MAX_OUTPUT_BYTES) {
    return { text: current, usedBytes, truncated: true }
  }

  const remaining = MAX_OUTPUT_BYTES - usedBytes
  if (chunk.length <= remaining) {
    return {
      text: current + chunk.toString('utf8'),
      usedBytes: usedBytes + chunk.length,
      truncated: false,
    }
  }

  return {
    text: current + chunk.subarray(0, remaining).toString('utf8'),
    usedBytes: usedBytes + remaining,
    truncated: true,
  }
}

function normalizeChunkText(raw: Buffer | string) {
  const chunk = Buffer.isBuffer(raw) ? raw : Buffer.from(raw)
  if (chunk.length <= MAX_SHELL_CHUNK_BYTES) {
    return chunk.toString('utf8')
  }

  return `${chunk.subarray(0, MAX_SHELL_CHUNK_BYTES).toString('utf8')}\n...[chunk truncated]`
}

function touchSession(session: ShellSessionRecord) {
  session.lastActiveAt = Date.now()
}

function pushShellChunk(session: ShellSessionRecord, stream: ShellChunkStream, text: string) {
  if (!text) {
    return
  }

  session.nextSeq += 1
  session.chunks.push({
    seq: session.nextSeq,
    stream,
    text,
    timestamp: Date.now(),
  })

  if (session.chunks.length > MAX_SHELL_CHUNKS) {
    session.chunks.splice(0, session.chunks.length - MAX_SHELL_CHUNKS)
  }
}

function ensureShellGcTimer() {
  if (shellGcTimer) {
    return
  }

  shellGcTimer = setInterval(() => {
    const now = Date.now()
    for (const [sessionId, session] of shellSessions.entries()) {
      if (!session.closed && now - session.lastActiveAt > session.idleTimeoutMs) {
        closeShellSessionInternal(session, 'Session idle timeout reached.')
      }

      if (session.closed && now - session.lastActiveAt > 60_000) {
        shellSessions.delete(sessionId)
      }
    }

    if (shellSessions.size === 0 && shellGcTimer) {
      clearInterval(shellGcTimer)
      shellGcTimer = undefined
    }
  }, 30_000)
}

function getSessionId(value: unknown) {
  const sessionId = String(value ?? '').trim()
  if (!sessionId) {
    throw new Error('缺少 sessionId。')
  }

  return sessionId
}

function getShellSession(sessionId: string) {
  const session = shellSessions.get(sessionId)
  if (!session) {
    throw new Error('SSH 会话不存在或已过期。')
  }

  return session
}

function closeShellSessionInternal(session: ShellSessionRecord, reason?: string) {
  if (session.closed) {
    return
  }

  session.closed = true
  touchSession(session)
  if (reason) {
    pushShellChunk(session, 'system', reason)
  }

  try {
    session.stream.end('exit\n')
  } catch {
    // noop
  }

  try {
    session.stream.close()
  } catch {
    // noop
  }

  try {
    session.client.end()
  } catch {
    // noop
  }
}

export async function executeSshCommand(
  connection: ConnectionConfig,
  request: SshExecRequest,
): Promise<SshExecResult> {
  const command = String(request.command ?? '').trim()
  if (!command) {
    throw new Error('SSH 命令不能为空。')
  }

  const timeoutMs = Math.min(parsePositiveInt(request.timeoutMs, DEFAULT_TIMEOUT_MS), MAX_TIMEOUT_MS)
  const target = normalizeTarget(request.sshTarget)
  const { host, port, user, sshConfig } = buildSshConfig(connection, target)
  const { Client } = await import('ssh2')

  return new Promise((resolve, reject) => {
    const client = new Client()
    const startedAt = Date.now()
    let settled = false
    let stdout = ''
    let stderr = ''
    let stdoutBytes = 0
    let stderrBytes = 0
    let truncated = false
    let exitCode: number | null = null
    let signal: string | undefined
    let activeStream:
      | {
          close?: () => void
        }
      | undefined

    const timeout = setTimeout(() => {
      if (settled) {
        return
      }

      settled = true
      activeStream?.close?.()
      client.end()
      reject(new Error(`SSH 命令执行超时（>${timeoutMs}ms）`))
    }, timeoutMs)

    const cleanup = () => {
      clearTimeout(timeout)
      client.removeAllListeners()
      client.end()
    }

    const rejectOnce = (error: Error) => {
      if (settled) {
        return
      }

      settled = true
      cleanup()
      reject(error)
    }

    const resolveOnce = () => {
      if (settled) {
        return
      }

      settled = true
      cleanup()
      resolve({
        host,
        port,
        user,
        command,
        stdout,
        stderr,
        exitCode,
        signal,
        durationMs: Date.now() - startedAt,
        truncated,
      })
    }

    client
      .on('ready', () => {
        client.exec(command, (error, stream) => {
          if (error) {
            rejectOnce(error)
            return
          }

          activeStream = stream

          stream.on('close', (code: number | null, streamSignal: string | undefined) => {
            exitCode = typeof code === 'number' ? code : null
            signal = streamSignal
            resolveOnce()
          })

          stream.on('data', (chunk: Buffer | string) => {
            const next = appendLimited(stdout, stdoutBytes, chunk)
            stdout = next.text
            stdoutBytes = next.usedBytes
            truncated ||= next.truncated
          })

          stream.stderr.on('data', (chunk: Buffer | string) => {
            const next = appendLimited(stderr, stderrBytes, chunk)
            stderr = next.text
            stderrBytes = next.usedBytes
            truncated ||= next.truncated
          })

          stream.on('error', (streamError: Error) => {
            rejectOnce(streamError)
          })
        })
      })
      .on('error', (error: Error) => {
        rejectOnce(error)
      })

    client.connect(sshConfig)
  })
}

export async function startSshShellSession(
  connection: ConnectionConfig,
  request: SshShellSessionStartRequest,
): Promise<SshShellSessionStartResult> {
  if (shellSessions.size >= MAX_SHELL_SESSIONS) {
    throw new Error('当前 SSH 会话过多，请先关闭一些会话。')
  }

  const target = normalizeTarget(request.sshTarget)
  const { host, port, user, sshConfig } = buildSshConfig(connection, target)
  const cols = clamp(parsePositiveInt(request.cols, DEFAULT_SHELL_COLS), 40, 320)
  const rows = clamp(parsePositiveInt(request.rows, DEFAULT_SHELL_ROWS), 12, 120)
  const term = String(request.term ?? 'xterm-256color').trim() || 'xterm-256color'
  const idleTimeoutMs = Math.min(
    parsePositiveInt(request.idleTimeoutMs, DEFAULT_SHELL_IDLE_TIMEOUT_MS),
    MAX_SHELL_IDLE_TIMEOUT_MS,
  )
  const { Client } = await import('ssh2')

  return new Promise((resolve, reject) => {
    const client = new Client()
    let settled = false
    const startedAt = Date.now()

    const timeout = setTimeout(() => {
      if (settled) {
        return
      }

      settled = true
      client.end()
      reject(new Error('SSH 会话建立超时。'))
    }, 20_000)

    const rejectOnce = (error: Error) => {
      if (settled) {
        return
      }

      settled = true
      clearTimeout(timeout)
      client.end()
      reject(error)
    }

    client
      .on('ready', () => {
        client.shell({ term, cols, rows }, (error, stream) => {
          if (error) {
            rejectOnce(error)
            return
          }

          const sessionId = crypto.randomUUID()
          const session: ShellSessionRecord = {
            id: sessionId,
            host,
            port,
            user,
            client,
            stream,
            createdAt: Date.now(),
            lastActiveAt: Date.now(),
            idleTimeoutMs,
            nextSeq: 0,
            chunks: [],
            closed: false,
            exitCode: null,
          }

          pushShellChunk(
            session,
            'system',
            `Connected to ${user}@${host}:${port} in ${Date.now() - startedAt}ms`,
          )

          stream.on('data', (chunk: Buffer | string) => {
            touchSession(session)
            pushShellChunk(session, 'stdout', normalizeChunkText(chunk))
          })

          stream.stderr.on('data', (chunk: Buffer | string) => {
            touchSession(session)
            pushShellChunk(session, 'stderr', normalizeChunkText(chunk))
          })

          stream.on('close', (code: number | undefined, signal: string | undefined) => {
            session.exitCode = typeof code === 'number' ? code : null
            session.signal = signal
            closeShellSessionInternal(
              session,
              `Session closed${session.exitCode != null ? ` (exit=${session.exitCode})` : ''}${signal ? ` signal=${signal}` : ''}.`,
            )
          })

          stream.on('error', (streamError: Error) => {
            closeShellSessionInternal(session, `Stream error: ${streamError.message}`)
          })

          client.on('error', (clientError: Error) => {
            closeShellSessionInternal(session, `SSH error: ${clientError.message}`)
          })

          client.on('close', () => {
            closeShellSessionInternal(session, 'SSH connection closed.')
          })

          shellSessions.set(sessionId, session)
          ensureShellGcTimer()

          settled = true
          clearTimeout(timeout)
          resolve({
            sessionId,
            host,
            port,
            user,
            nextSeq: session.nextSeq,
            createdAt: session.createdAt,
          })
        })
      })
      .on('error', (error: Error) => {
        rejectOnce(error)
      })

    client.connect(sshConfig)
  })
}

export function readSshShellSession(request: SshShellSessionReadRequest): SshShellSessionReadResult {
  const sessionId = getSessionId(request.sessionId)
  const afterSeq = parseNonNegativeInt(request.afterSeq, 0)
  const limit = clamp(parsePositiveInt(request.limit, 200), 1, 500)
  const session = getShellSession(sessionId)
  touchSession(session)

  const chunks = session.chunks.filter((chunk) => chunk.seq > afterSeq).slice(0, limit)

  return {
    sessionId: session.id,
    host: session.host,
    port: session.port,
    user: session.user,
    chunks,
    nextSeq: session.nextSeq,
    closed: session.closed,
    exitCode: session.exitCode,
    signal: session.signal,
    lastActiveAt: session.lastActiveAt,
  }
}

export function writeSshShellInput(request: SshShellSessionInputRequest) {
  const sessionId = getSessionId(request.sessionId)
  const session = getShellSession(sessionId)
  if (session.closed) {
    throw new Error('SSH 会话已关闭。')
  }

  const input = String(request.input ?? '')
  if (!input.trim()) {
    throw new Error('输入不能为空。')
  }

  if (Buffer.byteLength(input, 'utf8') > 8 * 1024) {
    throw new Error('输入过长，单条命令请控制在 8KB 以内。')
  }

  const appendNewline = readBoolean(request.appendNewline, true)
  const payload = appendNewline ? `${input}\n` : input
  touchSession(session)
  session.stream.write(payload)

  return {
    ok: true as const,
    sessionId: session.id,
    nextSeq: session.nextSeq,
  }
}

export function resizeSshShellSession(request: SshShellSessionResizeRequest) {
  const sessionId = getSessionId(request.sessionId)
  const session = getShellSession(sessionId)
  if (session.closed) {
    throw new Error('SSH 会话已关闭。')
  }

  const cols = clamp(parsePositiveInt(request.cols, DEFAULT_SHELL_COLS), 40, 320)
  const rows = clamp(parsePositiveInt(request.rows, DEFAULT_SHELL_ROWS), 12, 120)

  if (typeof session.stream.setWindow === 'function') {
    session.stream.setWindow(rows, cols, 0, 0)
  }

  touchSession(session)

  return {
    ok: true as const,
    sessionId: session.id,
  }
}

export function closeSshShellSession(request: SshShellSessionCloseRequest) {
  const sessionId = getSessionId(request.sessionId)
  const session = shellSessions.get(sessionId)
  if (!session) {
    return {
      ok: true as const,
    }
  }
  closeShellSessionInternal(session, 'Session closed by client.')
  shellSessions.delete(sessionId)

  return {
    ok: true as const,
  }
}

export function closeAllSshShellSessions() {
  for (const session of shellSessions.values()) {
    closeShellSessionInternal(session, 'Server shutdown.')
  }
  shellSessions.clear()

  if (shellGcTimer) {
    clearInterval(shellGcTimer)
    shellGcTimer = undefined
  }
}
