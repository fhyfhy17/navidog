import express from 'express'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  cancelActiveQuery,
  closePools,
  disposeConnection,
  executeSql,
  executeStatements,
  fetchSchemaTree,
  fetchTableColumns,
  fetchTableMetadata,
  normalizeConnectionPayload,
  QueryCancelledError,
  testConnection,
} from './mysql.js'
import { importBatch } from './import.js'
import {
  closeAllSshShellSessions,
  closeSshShellSession,
  executeSshCommand,
  readSshShellSession,
  resizeSshShellSession,
  startSshShellSession,
  writeSshShellInput,
} from './ssh.js'

const app = express()
const host = '127.0.0.1'
const port = Number.parseInt(process.env.PORT ?? '3001', 10)

app.use(express.json({ limit: '50mb' }))

app.get('/api/health', (_request, response) => {
  response.json({ ok: true })
})

app.post('/api/connection/test', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const result = await testConnection(connection)
    response.json(result)
  } catch (error) {
    console.error('[/api/connection/test] ERROR:', error)
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Connection test failed.',
    })
  }
})

app.post('/api/connection/disconnect', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    await disposeConnection(connection)
    response.json({ ok: true })
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Could not disconnect.',
    })
  }
})

app.post('/api/schema/tree', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const schemas = await fetchSchemaTree(connection)
    response.json({ schemas })
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Could not load schemas.',
    })
  }
})

app.post('/api/schema/table', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const schemaName = String(request.body.schemaName ?? '').trim()
    const tableName = String(request.body.tableName ?? '').trim()

    if (!schemaName || !tableName) {
      throw new Error('Schema and table are required.')
    }

    const columns = await fetchTableColumns(connection, schemaName, tableName)
    response.json({ columns })
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Could not inspect table.',
    })
  }
})

app.post('/api/schema/table-meta', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const schemaName = String(request.body.schemaName ?? '').trim()
    const tableName = String(request.body.tableName ?? '').trim()

    if (!schemaName || !tableName) {
      throw new Error('Schema and table are required.')
    }

    const metadata = await fetchTableMetadata(connection, schemaName, tableName)
    response.json(metadata)
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Could not inspect table metadata.',
    })
  }
})

app.post('/api/query', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const sql = String(request.body.sql ?? '')
    const database = request.body.database ? String(request.body.database).trim() : undefined
    const queryId = request.body.queryId ? String(request.body.queryId).trim() : undefined
    const startedAt = Date.now()
    const results = await executeSql(connection, sql, database, queryId)
    response.json({
      durationMs: Date.now() - startedAt,
      results,
    })
  } catch (error) {
    console.error('[/api/query] ERROR')
    response.status(error instanceof QueryCancelledError ? 409 : 400).json({
      error: error instanceof Error ? error.message : 'SQL execution failed.',
    })
  }
})

app.post('/api/query/batch', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const statements = Array.isArray(request.body.statements)
      ? request.body.statements.map((statement: unknown) => String(statement ?? ''))
      : []
    const database = request.body.database ? String(request.body.database).trim() : undefined
    const transaction = request.body.transaction !== false
    const startedAt = Date.now()
    const results = await executeStatements(connection, statements, database, transaction)
    response.json({
      durationMs: Date.now() - startedAt,
      results,
    })
  } catch (error) {
    console.error('[/api/query/batch] ERROR')
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Batch SQL execution failed.',
    })
  }
})

app.post('/api/query/cancel', async (request, response) => {
  try {
    const queryId = String(request.body.queryId ?? '').trim()
    if (!queryId) {
      throw new Error('Query ID is required.')
    }

    const ok = await cancelActiveQuery(queryId)
    response.json({ ok })
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Could not cancel query.',
    })
  }
})

app.post('/api/ssh/exec', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const startedAt = Date.now()
    const result = await executeSshCommand(connection, {
      command: request.body.command,
      timeoutMs: request.body.timeoutMs,
      sshTarget: request.body.sshTarget,
    })
    response.json({
      ...result,
      durationMs: Date.now() - startedAt,
    })
  } catch (error) {
    console.error('[/api/ssh/exec] ERROR:', error)
    response.status(400).json({
      error: error instanceof Error ? error.message : 'SSH command failed.',
    })
  }
})

app.post('/api/ssh/session/start', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const result = await startSshShellSession(connection, {
      sshTarget: request.body.sshTarget,
      cols: request.body.cols,
      rows: request.body.rows,
      term: request.body.term,
      idleTimeoutMs: request.body.idleTimeoutMs,
    })
    response.json(result)
  } catch (error) {
    console.error('[/api/ssh/session/start] ERROR:', error)
    response.status(400).json({
      error: error instanceof Error ? error.message : 'SSH shell start failed.',
    })
  }
})

app.post('/api/ssh/session/read', (request, response) => {
  try {
    const result = readSshShellSession({
      sessionId: request.body.sessionId,
      afterSeq: request.body.afterSeq,
      limit: request.body.limit,
    })
    response.json(result)
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'SSH shell read failed.',
    })
  }
})

app.post('/api/ssh/session/input', (request, response) => {
  try {
    const result = writeSshShellInput({
      sessionId: request.body.sessionId,
      input: request.body.input,
      appendNewline: request.body.appendNewline,
    })
    response.json(result)
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'SSH shell input failed.',
    })
  }
})

app.post('/api/ssh/session/resize', (request, response) => {
  try {
    const result = resizeSshShellSession({
      sessionId: request.body.sessionId,
      cols: request.body.cols,
      rows: request.body.rows,
    })
    response.json(result)
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'SSH shell resize failed.',
    })
  }
})

app.post('/api/ssh/session/close', (request, response) => {
  try {
    const result = closeSshShellSession({
      sessionId: request.body.sessionId,
    })
    response.json(result)
  } catch (error) {
    response.status(400).json({
      error: error instanceof Error ? error.message : 'SSH shell close failed.',
    })
  }
})

/* ── Data Import ────────────────────────────── */
app.post('/api/import/batch', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const { database, table, mode, primaryKeys, columns, rows, createTable } = request.body
    const result = await importBatch(connection, {
      database: String(database ?? ''),
      table: String(table ?? ''),
      mode: mode ?? 'append',
      primaryKeys: primaryKeys ?? [],
      columns: columns ?? [],
      rows: rows ?? [],
      createTable: createTable ?? undefined,
    })
    response.json(result)
  } catch (error) {
    console.error('[/api/import/batch] ERROR:', error)
    response.status(400).json({
      error: error instanceof Error ? error.message : 'Import failed.',
    })
  }
})

/* ── NCX Import ─────────────────────────────── */
app.post('/api/connections/import-ncx', async (request, response) => {
  void request
  response.status(410).json({
    error: 'This endpoint is disabled. Use the browser-based NCX import flow instead.',
  })
})

/* ── NCX Export ─────────────────────────────── */
app.post('/api/connections/export-ncx', async (request, response) => {
  void request
  response.status(410).json({
    error: 'This endpoint is disabled. Use the browser-based NCX export flow instead.',
  })
})

// In production, serve the built frontend
const __dirname = path.dirname(fileURLToPath(import.meta.url))
const distPath = path.resolve(__dirname, '..', 'dist')
if (fs.existsSync(distPath)) {
  app.use(express.static(distPath))
  app.get(/.*/, (_req, res) => {
    res.sendFile(path.join(distPath, 'index.html'))
  })
  console.log(`[NaviDog] Serving frontend from ${distPath}`)
}

const server = app.listen(port, host, () => {
  console.log(`NaviDog listening on http://${host}:${port}`)
})

async function shutdown() {
  closeAllSshShellSessions()
  await closePools()
  server.close(() => {
    process.exit(0)
  })
}

process.on('SIGINT', () => {
  void shutdown()
})

process.on('SIGTERM', () => {
  void shutdown()
})
