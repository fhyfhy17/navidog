import crypto from 'node:crypto'
import mysql, {
  type FieldPacket,
  type Pool,
  type PoolConnection,
  type ResultSetHeader,
  type RowDataPacket,
} from 'mysql2/promise'

type RawConnectionPayload = {
  host?: unknown
  port?: unknown
  user?: unknown
  password?: unknown
  database?: unknown
  ssl?: unknown
  sslRejectUnauthorized?: unknown
  useSSH?: unknown
  sshHost?: unknown
  sshPort?: unknown
  sshUser?: unknown
  sshAuthMethod?: unknown
  sshPrivateKey?: unknown
  sshPassphrase?: unknown
  sshPassword?: unknown
}

export type ConnectionConfig = {
  host: string
  port: number
  user: string
  password: string
  database?: string
  ssl: boolean
  sslRejectUnauthorized: boolean
  // SSH tunnel
  useSSH: boolean
  sshHost?: string
  sshPort?: number
  sshUser?: string
  sshAuthMethod?: 'password' | 'privateKey'
  sshPrivateKey?: string
  sshPassphrase?: string
  sshPassword?: string
}

export type SchemaTreeNode = {
  name: string
  tables: {
    name: string
    type: string
    rows: number | null
    dataLength: number | null
    engine: string | null
  }[]
}

export type TableColumnInfo = {
  name: string
  type: string
  nullable: boolean
  key: string
  extra?: string
  defaultValue?: string
  comment?: string
}

export type TableIndexInfo = {
  name: string
  unique: boolean
  primary: boolean
  type: string
  columns: string[]
  cardinality?: number | null
  comment?: string
}

export type TableForeignKeyInfo = {
  name: string
  columns: string[]
  referencedSchema: string
  referencedTable: string
  referencedColumns: string[]
  onUpdate: string
  onDelete: string
}

export type QueryResultSet =
  | {
      kind: 'rows'
      title: string
      columns: string[]
      rows: Record<string, unknown>[]
    }
  | {
      kind: 'mutation'
      title: string
      affectedRows: number
      insertId?: number
      warningStatus?: number
      info: string
    }
  | {
      kind: 'message'
      title: string
      message: string
    }

type TunnelRecord = {
  localPort: number
  close: () => void
}

type ActiveQueryRecord = {
  cancelled: boolean
  connection: PoolConnection & { destroy?: () => void; connection?: { destroy?: () => void } }
}

const pools = new Map<string, Pool>()
const pendingPools = new Map<string, Promise<Pool>>()
const activeQueries = new Map<string, ActiveQueryRecord>()
const hiddenSchemas = new Set([
  'information_schema',
  'mysql',
  'performance_schema',
  'sys',
])

export class QueryCancelledError extends Error {
  constructor() {
    super('Query was cancelled.')
    this.name = 'QueryCancelledError'
  }
}

function destroyPoolConnection(connection: ActiveQueryRecord['connection']) {
  if (typeof connection.destroy === 'function') {
    connection.destroy()
    return
  }

  connection.connection?.destroy?.()
}

function readBoolean(value: unknown, fallback: boolean) {
  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'number') {
    return value !== 0
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

function q(identifier: string) {
  return `\`${identifier.replaceAll('`', '``')}\``
}

export function normalizeConnectionPayload(payload: unknown): ConnectionConfig {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Connection payload is missing.')
  }

  const rawConnection = payload as RawConnectionPayload
  const host = String(rawConnection.host ?? '').trim()
  const user = String(rawConnection.user ?? '').trim()
  const password = String(rawConnection.password ?? '')
  const database = String(rawConnection.database ?? '').trim()
  const parsedPort = Number.parseInt(String(rawConnection.port ?? '3306'), 10)

  if (!host) {
    throw new Error('Connection host is required.')
  }

  if (!user) {
    throw new Error('Connection user is required.')
  }

  if (Number.isNaN(parsedPort) || parsedPort <= 0) {
    throw new Error('Connection port must be a positive number.')
  }

  return {
    host,
    port: parsedPort,
    user,
    password,
    database: database || undefined,
    ssl: readBoolean(rawConnection.ssl, false),
    sslRejectUnauthorized: readBoolean(rawConnection.sslRejectUnauthorized, false),
    useSSH: readBoolean(rawConnection.useSSH, false),
    sshHost: rawConnection.sshHost ? String(rawConnection.sshHost).trim() : undefined,
    sshPort: rawConnection.sshPort ? Number.parseInt(String(rawConnection.sshPort), 10) : undefined,
    sshUser: rawConnection.sshUser ? String(rawConnection.sshUser).trim() : undefined,
    sshAuthMethod: rawConnection.sshAuthMethod === 'password' ? 'password' : rawConnection.sshAuthMethod === 'privateKey' ? 'privateKey' : undefined,
    sshPrivateKey: rawConnection.sshPrivateKey ? String(rawConnection.sshPrivateKey).trim() : undefined,
    sshPassphrase: rawConnection.sshPassphrase ? String(rawConnection.sshPassphrase) : undefined,
    sshPassword: rawConnection.sshPassword ? String(rawConnection.sshPassword) : undefined,
  }
}

function fingerprintConnection(config: ConnectionConfig) {
  return crypto.createHash('sha1').update(JSON.stringify(config)).digest('hex')
}

// SSH tunnel tracking
const sshTunnels = new Map<string, TunnelRecord>()

function buildSslOptions(config: ConnectionConfig) {
  if (!config.ssl) {
    return undefined
  }

  return {
    rejectUnauthorized: config.sslRejectUnauthorized,
  }
}

async function destroyResourcesByFingerprint(fingerprint: string) {
  pendingPools.delete(fingerprint)

  const pool = pools.get(fingerprint)
  if (pool) {
    pools.delete(fingerprint)
    await pool.end().catch(() => undefined)
  }

  const tunnel = sshTunnels.get(fingerprint)
  if (tunnel) {
    sshTunnels.delete(fingerprint)
    tunnel.close()
  }
}

function isRecoverableConnectionError(error: unknown) {
  if (!error || typeof error !== 'object') {
    return false
  }

  const code = 'code' in error ? String(error.code ?? '') : ''
  const message = 'message' in error ? String(error.message ?? '') : ''

  return [
    'PROTOCOL_CONNECTION_LOST',
    'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR',
    'ECONNRESET',
    'EPIPE',
    'ETIMEDOUT',
  ].includes(code) || message.includes('Connection lost')
}

function isNoSslSupportError(error: unknown) {
  if (!error || typeof error !== 'object') {
    return false
  }

  const code = 'code' in error ? String(error.code ?? '') : ''
  const message = 'message' in error ? String(error.message ?? '') : ''
  return code === 'HANDSHAKE_NO_SSL_SUPPORT' || message.includes('does not support secure connection')
}

function formatConnectionError(config: ConnectionConfig, error: unknown) {
  if (error instanceof Error) {
    const code = 'code' in error ? String((error as { code?: unknown }).code ?? '') : ''
    const message = error.message

    if (isNoSslSupportError(error)) {
      return new Error(`${message} Disable SSL/TLS for this server and try again.`)
    }

    if (
      code === 'PROTOCOL_CONNECTION_LOST' ||
      message.includes('reading initial communication packet') ||
      message.includes('server closed the connection')
    ) {
      const hints = [
        'The server dropped the connection before authentication finished.',
        'Check that this client public IP is in the RDS whitelist or security policy.',
      ]

      if (config.useSSH) {
        hints.push('If you are using SSH tunneling, verify the jump host can reach the target RDS endpoint.')
      }

      return new Error(`${message} ${hints.join(' ')}`)
    }

    return error
  }

  return new Error('Database connection failed.')
}

async function withSslFallback<T>(
  config: ConnectionConfig,
  operation: (effectiveConfig: ConnectionConfig) => Promise<T>,
): Promise<T> {
  try {
    return await operation(config)
  } catch (error) {
    if (!config.ssl || !isNoSslSupportError(error)) {
      throw error
    }

    return operation({
      ...config,
      ssl: false,
      sslRejectUnauthorized: false,
    })
  }
}

async function withReconnect<T>(
  config: ConnectionConfig,
  operation: () => Promise<T>,
): Promise<T> {
  try {
    return await operation()
  } catch (error) {
    if (!isRecoverableConnectionError(error)) {
      throw formatConnectionError(config, error)
    }

    await destroyResourcesByFingerprint(fingerprintConnection(config))

    try {
      return await operation()
    } catch (retryError) {
      throw formatConnectionError(config, retryError)
    }
  }
}

export async function getPool(config: ConnectionConfig, forceRefresh = false): Promise<Pool> {
  const fingerprint = fingerprintConnection(config)
  if (forceRefresh) {
    await destroyResourcesByFingerprint(fingerprint)
  }

  const existingPool = pools.get(fingerprint)
  if (existingPool) {
    return existingPool
  }

  const pendingPool = pendingPools.get(fingerprint)
  if (pendingPool) {
    return pendingPool
  }

  const creatingPool = (async () => {
    let mysqlHost = config.host
    let mysqlPort = config.port

    if (config.useSSH && config.sshHost) {
      const existingTunnel = sshTunnels.get(fingerprint)
      const localPort = existingTunnel?.localPort ?? await setupSSHTunnel(config, fingerprint)
      mysqlHost = '127.0.0.1'
      mysqlPort = localPort
    }

    const pool = mysql.createPool({
      host: mysqlHost,
      port: mysqlPort,
      user: config.user,
      password: config.password,
      database: config.database,
      ssl: buildSslOptions(config),
      waitForConnections: true,
      connectionLimit: 6,
      maxIdle: 6,
      idleTimeout: 60_000,
      queueLimit: 0,
      connectTimeout: 15_000,
      enableKeepAlive: true,
      keepAliveInitialDelay: 0,
      multipleStatements: true,
    })

    pools.set(fingerprint, pool)
    return pool
  })()

  pendingPools.set(fingerprint, creatingPool)

  try {
    return await creatingPool
  } catch (error) {
    await destroyResourcesByFingerprint(fingerprint)
    throw error
  } finally {
    pendingPools.delete(fingerprint)
  }
}

async function setupSSHTunnel(config: ConnectionConfig, tunnelKey: string): Promise<number> {
  const { Client } = await import('ssh2')
  const net = await import('node:net')
  const fs = await import('node:fs')
  const path = await import('node:path')
  const os = await import('node:os')

  return new Promise((resolve, reject) => {
    const sshClient = new Client()
    let tunnelServer: import('node:net').Server | undefined
    let settled = false
    let resolved = false
    let closed = false

    const rejectOnce = (error: Error) => {
      if (settled) {
        return
      }

      settled = true
      reject(error)
    }

    const resolveOnce = (localPort: number) => {
      if (settled) {
        return
      }

      settled = true
      resolve(localPort)
    }

    const closeTunnel = () => {
      if (closed) {
        return
      }

      closed = true
      sshTunnels.delete(tunnelKey)
      tunnelServer?.close()
      sshClient.end()
    }

    // Build auth config
    const sshConfig: Record<string, unknown> = {
      host: config.sshHost,
      port: config.sshPort ?? 22,
      username: config.sshUser ?? 'root',
      keepaliveInterval: 15_000,
      keepaliveCountMax: 3,
    }

    if (config.sshAuthMethod === 'password') {
      sshConfig.password = config.sshPassword ?? ''
    } else {
      // Private key auth
      let keyPath = config.sshPrivateKey ?? '~/.ssh/id_rsa'
      if (keyPath.startsWith('~')) {
        keyPath = path.join(os.homedir(), keyPath.slice(1))
      }
      try {
        sshConfig.privateKey = fs.readFileSync(keyPath)
        if (config.sshPassphrase) {
          sshConfig.passphrase = config.sshPassphrase
        }
      } catch (err) {
        rejectOnce(new Error(`无法读取 SSH 私钥文件: ${keyPath} - ${err instanceof Error ? err.message : String(err)}`))
        return
      }
    }

    sshClient.on('close', () => {
      if (!resolved) {
        rejectOnce(new Error('SSH 连接在隧道建立前被关闭。'))
      }
      closeTunnel()
    })

    sshClient.on('end', () => {
      if (!resolved) {
        rejectOnce(new Error('SSH 连接在隧道建立前已结束。'))
      }
      closeTunnel()
    })

    sshClient.on('ready', () => {
      // Create local TCP server to forward connections
      tunnelServer = net.createServer((localSocket) => {
        sshClient.forwardOut(
          '127.0.0.1', 0,
          config.host, config.port,
          (err, stream) => {
            if (err) {
              localSocket.end()
              return
            }

            stream.on('error', () => {
              localSocket.destroy()
            })

            localSocket.on('error', () => {
              stream.destroy()
            })

            localSocket.pipe(stream).pipe(localSocket)
          },
        )
      })

      tunnelServer.on('error', (error) => {
        if (!resolved) {
          rejectOnce(new Error(`SSH 隧道启动失败: ${error.message}`))
        }
        closeTunnel()
      })

      tunnelServer.listen(0, '127.0.0.1', () => {
        const addr = tunnelServer?.address()
        const localPort = typeof addr === 'object' && addr ? addr.port : 0
        resolved = true
        sshTunnels.set(tunnelKey, {
          localPort,
          close: closeTunnel,
        })
        resolveOnce(localPort)
      })
    })

    sshClient.on('error', (err) => {
      if (!resolved) {
        rejectOnce(new Error(`SSH 连接失败: ${err.message}`))
        return
      }

      closeTunnel()
    })

    sshClient.connect(sshConfig as Parameters<typeof sshClient.connect>[0])
  })
}

export async function disposeConnection(config: ConnectionConfig) {
  await destroyResourcesByFingerprint(fingerprintConnection(config))
}

export async function closePools() {
  for (const [, activeQuery] of activeQueries) {
    activeQuery.cancelled = true
    destroyPoolConnection(activeQuery.connection)
  }
  activeQueries.clear()

  await Promise.all(Array.from(pools.values(), (pool) => pool.end()))
  pools.clear()
  for (const tunnel of sshTunnels.values()) {
    tunnel.close()
  }
  sshTunnels.clear()
  pendingPools.clear()
}

export async function testConnection(config: ConnectionConfig) {
  return withSslFallback(config, (effectiveConfig) =>
    withReconnect(effectiveConfig, async () => {
      const pool = await getPool(effectiveConfig)
      const [rows] = await pool.query<RowDataPacket[]>(
        'SELECT VERSION() AS version, DATABASE() AS databaseName, @@hostname AS hostName',
      )

      const firstRow = rows[0] ?? {}

      return {
        version: String(firstRow.version ?? 'unknown'),
        databaseName: String(firstRow.databaseName ?? effectiveConfig.database ?? ''),
        hostName: String(firstRow.hostName ?? effectiveConfig.host),
      }
    }),
  )
}

export async function fetchSchemaTree(config: ConnectionConfig): Promise<SchemaTreeNode[]> {
  return withSslFallback(config, (effectiveConfig) =>
    withReconnect(effectiveConfig, async () => {
      const pool = await getPool(effectiveConfig)
      const [schemaRows] = await pool.query<RowDataPacket[]>('SHOW DATABASES')

      const schemaNames = schemaRows
        .map((row) => String(row.Database ?? ''))
        .filter((schemaName) => schemaName && !hiddenSchemas.has(schemaName))

      if (schemaNames.length === 0) {
        return []
      }

      const [tableRows] = await pool.query<RowDataPacket[]>(
        `
          SELECT
            TABLE_SCHEMA AS schemaName,
            TABLE_NAME AS tableName,
            TABLE_TYPE AS tableType,
            TABLE_ROWS AS tableRows,
            DATA_LENGTH AS dataLength,
            ENGINE AS engine
          FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA IN (?)
          ORDER BY TABLE_SCHEMA, TABLE_NAME
        `,
        [schemaNames],
      )

      const groupedTables = new Map<string, SchemaTreeNode['tables']>()

      for (const schemaName of schemaNames) {
        groupedTables.set(schemaName, [])
      }

      for (const row of tableRows) {
        const schemaName = String(row.schemaName)
        groupedTables.get(schemaName)?.push({
          name: String(row.tableName),
          type: String(row.tableType),
          rows: row.tableRows != null ? Number(row.tableRows) : null,
          dataLength: row.dataLength != null ? Number(row.dataLength) : null,
          engine: row.engine != null ? String(row.engine) : null,
        })
      }

      return schemaNames.map((schemaName) => ({
        name: schemaName,
        tables: groupedTables.get(schemaName) ?? [],
      }))
    }),
  )
}

export async function fetchTableColumns(
  config: ConnectionConfig,
  schemaName: string,
  tableName: string,
): Promise<TableColumnInfo[]> {
  return withSslFallback(config, (effectiveConfig) =>
    withReconnect(effectiveConfig, async () => {
      const pool = await getPool(effectiveConfig)
      const [columnRows] = await pool.query<RowDataPacket[]>(
        `SHOW FULL COLUMNS FROM ${q(schemaName)}.${q(tableName)}`,
      )

      return columnRows.map((row) => ({
        name: String(row.Field ?? ''),
        type: String(row.Type ?? ''),
        nullable: String(row.Null ?? '').toUpperCase() === 'YES',
        key: String(row.Key ?? ''),
        extra: String(row.Extra ?? ''),
        defaultValue: row.Default != null ? String(row.Default) : undefined,
        comment: row.Comment ? String(row.Comment) : undefined,
      }))
    }),
  )
}

export async function fetchTableMetadata(
  config: ConnectionConfig,
  schemaName: string,
  tableName: string,
): Promise<{ indexes: TableIndexInfo[]; foreignKeys: TableForeignKeyInfo[] }> {
  return withSslFallback(config, (effectiveConfig) =>
    withReconnect(effectiveConfig, async () => {
      const pool = await getPool(effectiveConfig)
      const [indexRows] = await pool.query<RowDataPacket[]>(
        `SHOW INDEX FROM ${q(schemaName)}.${q(tableName)}`,
      )

      const indexesByName = new Map<string, {
        name: string
        unique: boolean
        primary: boolean
        type: string
        cardinality?: number | null
        comment?: string
        columns: { seq: number; name: string }[]
      }>()

      for (const row of indexRows) {
        const indexName = String(row.Key_name ?? '')
        if (!indexName) continue

        const existing = indexesByName.get(indexName) ?? {
          name: indexName,
          unique: Number(row.Non_unique ?? 1) === 0,
          primary: indexName === 'PRIMARY',
          type: String(row.Index_type ?? ''),
          cardinality: row.Cardinality != null ? Number(row.Cardinality) : null,
          comment: row.Index_comment ? String(row.Index_comment) : undefined,
          columns: [],
        }

        existing.columns.push({
          seq: Number(row.Seq_in_index ?? existing.columns.length + 1),
          name: String(row.Column_name ?? ''),
        })

        indexesByName.set(indexName, existing)
      }

      const indexes = Array.from(indexesByName.values())
        .map((index) => ({
          name: index.name,
          unique: index.unique,
          primary: index.primary,
          type: index.type,
          columns: index.columns
            .sort((a, b) => a.seq - b.seq)
            .map((column) => column.name),
          cardinality: index.cardinality ?? null,
          comment: index.comment,
        }))
        .sort((a, b) => {
          if (a.primary && !b.primary) return -1
          if (!a.primary && b.primary) return 1
          return a.name.localeCompare(b.name)
        })

      const [fkRows] = await pool.query<RowDataPacket[]>(
        `
          SELECT
            kcu.CONSTRAINT_NAME AS constraintName,
            kcu.COLUMN_NAME AS columnName,
            kcu.ORDINAL_POSITION AS ordinalPosition,
            kcu.REFERENCED_TABLE_SCHEMA AS referencedSchema,
            kcu.REFERENCED_TABLE_NAME AS referencedTable,
            kcu.REFERENCED_COLUMN_NAME AS referencedColumn,
            rc.UPDATE_RULE AS updateRule,
            rc.DELETE_RULE AS deleteRule
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
           AND rc.TABLE_NAME = kcu.TABLE_NAME
           AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
          WHERE kcu.TABLE_SCHEMA = ?
            AND kcu.TABLE_NAME = ?
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
          ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        `,
        [schemaName, tableName],
      )

      const foreignKeysByName = new Map<string, {
        name: string
        referencedSchema: string
        referencedTable: string
        onUpdate: string
        onDelete: string
        columns: { seq: number; name: string }[]
        referencedColumns: { seq: number; name: string }[]
      }>()

      for (const row of fkRows) {
        const name = String(row.constraintName ?? '')
        if (!name) continue

        const existing = foreignKeysByName.get(name) ?? {
          name,
          referencedSchema: String(row.referencedSchema ?? schemaName),
          referencedTable: String(row.referencedTable ?? ''),
          onUpdate: String(row.updateRule ?? 'RESTRICT'),
          onDelete: String(row.deleteRule ?? 'RESTRICT'),
          columns: [],
          referencedColumns: [],
        }

        const seq = Number(row.ordinalPosition ?? existing.columns.length + 1)
        existing.columns.push({ seq, name: String(row.columnName ?? '') })
        existing.referencedColumns.push({ seq, name: String(row.referencedColumn ?? '') })
        foreignKeysByName.set(name, existing)
      }

      const foreignKeys = Array.from(foreignKeysByName.values()).map((fk) => ({
        name: fk.name,
        columns: fk.columns.sort((a, b) => a.seq - b.seq).map((column) => column.name),
        referencedSchema: fk.referencedSchema,
        referencedTable: fk.referencedTable,
        referencedColumns: fk.referencedColumns
          .sort((a, b) => a.seq - b.seq)
          .map((column) => column.name),
        onUpdate: fk.onUpdate,
        onDelete: fk.onDelete,
      }))

      return { indexes, foreignKeys }
    }),
  )
}

function isResultSetHeader(value: unknown): value is ResultSetHeader {
  return value !== null && typeof value === 'object' && 'affectedRows' in value
}

function normalizeRowPacket(row: RowDataPacket, columns: string[]) {
  return Object.fromEntries(columns.map((column) => {
    let value = row[column]
    // Convert Buffer to string (MySQL returns BLOB/binary columns as Buffer)
    if (Buffer.isBuffer(value)) {
      value = value.toString('utf8')
    }
    // Convert BigInt to string (JSON.stringify cannot serialize BigInt)
    if (typeof value === 'bigint') {
      value = value.toString()
    }
    return [column, value]
  }))
}

function normalizeSingleResult(
  rows: unknown,
  fields: FieldPacket[] | undefined,
  index: number,
): QueryResultSet {
  if (Array.isArray(rows) && (rows.length === 0 || !isResultSetHeader(rows[0]))) {
    const rowPackets = rows as RowDataPacket[]
    const columns = fields?.map((field) => field.name) ?? Object.keys(rowPackets[0] ?? {})

    return {
      kind: 'rows',
      title: `Result ${index + 1}`,
      columns,
      rows: rowPackets.map((row) => normalizeRowPacket(row, columns)),
    }
  }

  if (isResultSetHeader(rows)) {
    return {
      kind: 'mutation',
      title: `Statement ${index + 1}`,
      affectedRows: typeof rows.affectedRows === 'bigint' ? Number(rows.affectedRows) : rows.affectedRows,
      insertId: typeof (rows.insertId as unknown) === 'bigint' ? Number(rows.insertId) : rows.insertId,
      warningStatus: rows.warningStatus,
      info: rows.info,
    }
  }

  return {
    kind: 'message',
    title: `Statement ${index + 1}`,
    message: 'Statement executed.',
  }
}

function normalizeQueryPayload(
  rows: unknown,
  fields: FieldPacket[] | FieldPacket[][] | undefined,
  startIndex = 0,
): QueryResultSet[] {
  if (
    Array.isArray(rows) &&
    rows.some((entry) => Array.isArray(entry) || isResultSetHeader(entry))
  ) {
    return rows.map((entry, index) => {
      const fieldSet =
        Array.isArray(fields) && Array.isArray(fields[index])
          ? (fields[index] as FieldPacket[])
          : undefined

      return normalizeSingleResult(entry, fieldSet, startIndex + index)
    })
  }

  const singleFieldSet =
    Array.isArray(fields) && fields.length > 0 && !Array.isArray(fields[0])
      ? (fields as FieldPacket[])
      : undefined

  return [normalizeSingleResult(rows, singleFieldSet, startIndex)]
}

export async function executeSql(
  config: ConnectionConfig,
  sql: string,
  database?: string,
  queryId?: string,
): Promise<QueryResultSet[]> {
  const trimmedSql = sql.trim()

  if (!trimmedSql) {
    throw new Error('SQL is empty.')
  }

  return withSslFallback(config, (effectiveConfig) =>
    withReconnect(effectiveConfig, async () => {
      const pool = await getPool(effectiveConfig)
      const conn = await pool.getConnection() as PoolConnection & { destroy?: () => void; connection?: { destroy?: () => void } }
      const activeQuery = queryId
        ? { cancelled: false, connection: conn }
        : null

      if (queryId && activeQuery) {
        activeQueries.set(queryId, activeQuery)
      }

      try {
        if (database) {
          await conn.query(`USE \`${database.replaceAll('`', '``')}\``)
        }

        const [rows, fields] = await conn.query(trimmedSql)
        return normalizeQueryPayload(rows, fields)
      } catch (error) {
        if (activeQuery?.cancelled) {
          throw new QueryCancelledError()
        }
        throw error
      } finally {
        if (queryId) {
          activeQueries.delete(queryId)
        }
        try {
          conn.release()
        } catch {
          // Destroyed connections may already be closed.
        }
      }
    }),
  )
}

export async function executeStatements(
  config: ConnectionConfig,
  statements: string[],
  database?: string,
  transaction = false,
): Promise<QueryResultSet[]> {
  const trimmedStatements = statements
    .map((statement) => statement.trim())
    .filter(Boolean)

  if (trimmedStatements.length === 0) {
    throw new Error('No SQL statements provided.')
  }

  return withSslFallback(config, async (effectiveConfig) => {
    const pool = await getPool(effectiveConfig)
    const conn = await pool.getConnection()
    let transactionStarted = false

    try {
      if (database) {
        await conn.query(`USE \`${database.replaceAll('`', '``')}\``)
      }

      if (transaction) {
        await conn.beginTransaction()
        transactionStarted = true
      }

      const results: QueryResultSet[] = []

      for (let index = 0; index < trimmedStatements.length; index += 1) {
        const statement = trimmedStatements[index]

        try {
          const [rows, fields] = await conn.query(statement)
          results.push(...normalizeQueryPayload(rows, fields, results.length))
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error)
          throw new Error(`Statement ${index + 1} failed: ${message}`)
        }
      }

      if (transactionStarted) {
        await conn.commit()
        transactionStarted = false
      }

      return results
    } catch (error) {
      if (transactionStarted) {
        await conn.rollback().catch(() => undefined)
      }
      throw formatConnectionError(effectiveConfig, error)
    } finally {
      try {
        conn.release()
      } catch {
        // Destroyed connections may already be closed.
      }
    }
  })
}

export async function cancelActiveQuery(queryId: string): Promise<boolean> {
  const activeQuery = activeQueries.get(queryId)
  if (!activeQuery) {
    return false
  }

  activeQuery.cancelled = true
  destroyPoolConnection(activeQuery.connection)
  return true
}
