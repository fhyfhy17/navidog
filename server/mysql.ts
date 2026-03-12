import crypto from 'node:crypto'
import mysql, {
  type FieldPacket,
  type Pool,
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

const pools = new Map<string, Pool>()
const pendingPools = new Map<string, Promise<Pool>>()
const hiddenSchemas = new Set([
  'information_schema',
  'mysql',
  'performance_schema',
  'sys',
])

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
      }))
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

export async function executeSql(
  config: ConnectionConfig,
  sql: string,
  database?: string,
): Promise<QueryResultSet[]> {
  const trimmedSql = sql.trim()

  if (!trimmedSql) {
    throw new Error('SQL is empty.')
  }

  return withSslFallback(config, (effectiveConfig) =>
    withReconnect(effectiveConfig, async () => {
      const pool = await getPool(effectiveConfig)
      const conn = await pool.getConnection()

      try {
        if (database) {
          await conn.query(`USE \`${database.replaceAll('`', '``')}\``)
        }

        const [rows, fields] = await conn.query(trimmedSql)

        if (
          Array.isArray(rows) &&
          rows.some((entry) => Array.isArray(entry) || isResultSetHeader(entry))
        ) {
          return rows.map((entry, index) => {
            const fieldSet =
              Array.isArray(fields) && Array.isArray(fields[index])
                ? (fields[index] as FieldPacket[])
                : undefined

            return normalizeSingleResult(entry, fieldSet, index)
          })
        }

        const singleFieldSet =
          Array.isArray(fields) && fields.length > 0 && !Array.isArray(fields[0])
            ? (fields as FieldPacket[])
            : undefined

        return [normalizeSingleResult(rows, singleFieldSet, 0)]
      } finally {
        conn.release()
      }
    }),
  )
}
