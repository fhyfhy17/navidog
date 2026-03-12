import type { PoolConnection, RowDataPacket } from 'mysql2/promise'
import type { ConnectionConfig } from './mysql.js'
import { getPool } from './mysql.js'

export type ImportMode = 'append' | 'replace' | 'upsert'

export type ColumnMapping = {
  source: string
  target: string
}

export type ImportBatchRequest = {
  database: string
  table: string
  mode: ImportMode
  primaryKeys: string[]
  columns: ColumnMapping[]
  rows: Record<string, unknown>[]
  createTable?: {
    columns: {
      name: string
      type: string
      nullable: boolean
      key?: string
    }[]
    primaryKeys: string[]
  }
}

export type ImportBatchResult = {
  processed: number
  inserted: number
  updated: number
  errors: string[]
}

function escapeValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return 'NULL'
  if (typeof value === 'number') return String(value)
  if (typeof value === 'boolean') return value ? '1' : '0'
  if (typeof value === 'object') {
    try {
      value = JSON.stringify(value)
    } catch {
      value = String(value)
    }
  }
  // Escape string for MySQL
  const str = String(value)
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t')
    .replace(/\0/g, '\\0')
  return `'${str}'`
}

function q(identifier: string): string {
  return `\`${identifier.replace(/`/g, '``')}\``
}

function normalizeColumnType(type: string): string {
  const normalized = type.trim().toUpperCase()
  if (!/^[A-Z]+(?:\(\d+(?:,\d+)?\))?$/.test(normalized)) {
    throw new Error(`Unsupported generated column type: ${type}`)
  }
  return normalized
}

async function tableExists(conn: PoolConnection, table: string) {
  const [rows] = await conn.query<RowDataPacket[]>('SHOW TABLES LIKE ?', [table])
  return rows.length > 0
}

async function ensureTableForImport(
  conn: PoolConnection,
  table: string,
  createTable: ImportBatchRequest['createTable'],
) {
  if (await tableExists(conn, table)) {
    return
  }

  if (!createTable || createTable.columns.length === 0) {
    throw new Error(`Target table "${table}" does not exist.`)
  }

  const pkSet = new Set(createTable.primaryKeys)
  const columnSql = createTable.columns.map((column) => {
    const columnType = normalizeColumnType(column.type)
    const notNull = pkSet.has(column.name) || column.nullable === false
    return `${q(column.name)} ${columnType} ${notNull ? 'NOT NULL' : 'NULL'}`
  })

  const pkColumns = createTable.columns
    .map((column) => column.name)
    .filter((name) => pkSet.has(name))

  if (pkColumns.length > 0) {
    columnSql.push(`PRIMARY KEY (${pkColumns.map(q).join(', ')})`)
  }

  const sql = [
    `CREATE TABLE ${q(table)} (`,
    `  ${columnSql.join(',\n  ')}`,
    ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci',
  ].join('\n')

  await conn.query(sql)
}

export async function importBatch(
  config: ConnectionConfig,
  request: ImportBatchRequest,
): Promise<ImportBatchResult> {
  const { database, table, mode, primaryKeys, columns, rows, createTable } = request

  if (!database || !table) {
    throw new Error('Database and table are required.')
  }

  if (columns.length === 0) {
    throw new Error('At least one column mapping is required.')
  }

  if (rows.length === 0) {
    return { processed: 0, inserted: 0, updated: 0, errors: [] }
  }

  const pool = await getPool(config)
  const conn = await pool.getConnection()
  const result: ImportBatchResult = { processed: 0, inserted: 0, updated: 0, errors: [] }

  try {
    await conn.query(`USE ${q(database)}`)
    await ensureTableForImport(conn, table, createTable)

    const targetCols = columns.map((c) => q(c.target))
    const BATCH_SIZE = 500

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE)
      const valueRows = batch.map((row) => {
        const vals = columns.map((col) => escapeValue(row[col.source]))
        return `(${vals.join(', ')})`
      })

      let sql: string

      if (mode === 'replace') {
        sql = `REPLACE INTO ${q(table)} (${targetCols.join(', ')}) VALUES\n${valueRows.join(',\n')}`
      } else if (mode === 'upsert' && primaryKeys.length > 0) {
        const updateCols = columns
          .filter((c) => !primaryKeys.includes(c.target))
          .map((c) => `${q(c.target)} = VALUES(${q(c.target)})`)
        sql = `INSERT INTO ${q(table)} (${targetCols.join(', ')}) VALUES\n${valueRows.join(',\n')}`
        if (updateCols.length > 0) {
          sql += `\nON DUPLICATE KEY UPDATE ${updateCols.join(', ')}`
        }
      } else {
        // append (INSERT IGNORE to skip duplicates gracefully)
        sql = `INSERT INTO ${q(table)} (${targetCols.join(', ')}) VALUES\n${valueRows.join(',\n')}`
      }

      try {
        const queryResult = await conn.query(sql)
        const res = queryResult[0] as { affectedRows?: number }
        const affected = typeof res.affectedRows === 'number' ? res.affectedRows : batch.length
        result.processed += batch.length
        result.inserted += affected
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err)
        result.errors.push(`Rows ${i + 1}-${i + batch.length}: ${msg}`)
        result.processed += batch.length
      }
    }
  } finally {
    conn.release()
  }

  return result
}
