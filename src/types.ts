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
  color?: string
  group?: string
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
  extra?: string
  defaultValue?: string
  comment?: string
}

export type TableIndex = {
  name: string
  unique: boolean
  primary: boolean
  type: string
  columns: string[]
  cardinality?: number | null
  comment?: string
}

export type TableForeignKey = {
  name: string
  columns: string[]
  referencedSchema: string
  referencedTable: string
  referencedColumns: string[]
  onUpdate: string
  onDelete: string
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

export type TableMetadataResponse = {
  indexes: TableIndex[]
  foreignKeys: TableForeignKey[]
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
  pageCursors: (Record<string, unknown> | null)[]
  pagingMode: 'offset' | 'primaryKey'
  totalRows: number
  hasMore: boolean
  rowCountExact: boolean
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
  activeQueryId?: string
  loading: boolean
}

export type CliTab = {
  id: string
  kind: 'cli'
  title: string
  connectionId: string
  schemaName: string
  history: { input: string; output: string; isError?: boolean }[]
  loading: boolean
}

export type DesignTab = {
  id: string
  kind: 'design'
  title: string
  connectionId: string
  schemaName: string
  tableName: string
  mode: 'create' | 'alter'
}

export type AppTab = ObjectsTab | DataTab | QueryTab | CliTab | DesignTab

/* ── Tree selection ─────────────────────────── */

export type SelectedNode =
  | { kind: 'connection'; connectionId: string }
  | { kind: 'database'; connectionId: string; schemaName: string }
  | { kind: 'table'; connectionId: string; schemaName: string; tableName: string }
