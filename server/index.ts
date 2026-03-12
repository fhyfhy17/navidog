import express from 'express'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  closePools,
  disposeConnection,
  executeSql,
  fetchSchemaTree,
  fetchTableColumns,
  normalizeConnectionPayload,
  testConnection,
} from './mysql.js'

const app = express()
const host = '127.0.0.1'
const port = Number.parseInt(process.env.PORT ?? '3001', 10)

app.use(express.json({ limit: '1mb' }))

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

app.post('/api/query', async (request, response) => {
  try {
    const connection = normalizeConnectionPayload(request.body.connection)
    const sql = String(request.body.sql ?? '')
    const database = request.body.database ? String(request.body.database).trim() : undefined
    const startedAt = Date.now()
    const results = await executeSql(connection, sql, database)
    response.json({
      durationMs: Date.now() - startedAt,
      results,
    })
  } catch (error) {
    console.error('[/api/query] ERROR')
    response.status(400).json({
      error: error instanceof Error ? error.message : 'SQL execution failed.',
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
