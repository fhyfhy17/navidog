/* ── Connection ──────────────────────────────── */

export type ConnectionProfile = {
  id: string
  label: string
  host: string
  port: number
  user: string
  password: string
  database?: string
  ssl?: boolean
  sslRejectUnauthorized?: boolean
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

/* ── Schema tree ────────────────────────────── */

export type SchemaNode = {
  name: string
  tables: {
    name: string
    type: string
    rows: number | null
    dataLength: number | null
    engine: string | null
  }[]
}

export type TableColumn = {
  name: string
  type: string
  nullable: boolean
  key: string
}

export type ImportCreateTablePlan = {
  columns: TableColumn[]
  primaryKeys: string[]
}

/* ── Query results ──────────────────────────── */

export type RowResultSet = {
  kind: 'rows'
  title: string
  columns: string[]
  rows: Record<string, unknown>[]
}

export type MutationResultSet = {
  kind: 'mutation'
  title: string
  affectedRows: number
  insertId?: number
  warningStatus?: number
  info: string
}

export type MessageResultSet = {
  kind: 'message'
  title: string
  message: string
}

export type QueryResultSet =
  | RowResultSet
  | MutationResultSet
  | MessageResultSet

/* ── API responses ──────────────────────────── */

export type TestConnectionResponse = {
  version: string
  hostName: string
  databaseName: string
}

export type SchemaTreeResponse = {
  schemas: SchemaNode[]
}

export type TableColumnsResponse = {
  columns: TableColumn[]
}

export type QueryExecutionResponse = {
  durationMs: number
  results: QueryResultSet[]
}

/* ── Tab system ─────────────────────────────── */

export type ObjectsTab = {
  id: string
  kind: 'objects'
  title: string
  connectionId: string
  schemaName: string
  tableFilter: string
}

export type DataTab = {
  id: string
  kind: 'data'
  title: string
  connectionId: string
  schemaName: string
  tableName: string
  columns: string[]
  rows: Record<string, unknown>[]
  page: number
  totalRows: number
  loading: boolean
}

export type QueryTab = {
  id: string
  kind: 'query'
  title: string
  connectionId: string
  schemaName: string
  sql: string
  results: QueryResultSet[]
  activeResultIndex: number
  durationMs: number
  loading: boolean
}

export type AppTab = ObjectsTab | DataTab | QueryTab

/* ── Tree selection ─────────────────────────── */

export type SelectedNode =
  | { kind: 'connection'; connectionId: string }
  | { kind: 'database'; connectionId: string; schemaName: string }
  | { kind: 'table'; connectionId: string; schemaName: string; tableName: string }
