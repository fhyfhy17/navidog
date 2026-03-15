import type {
  ConnectionProfile,
  ImportCreateTablePlan,
  QueryExecutionResponse,
  SchemaTreeResponse,
  TableColumnsResponse,
  TableMetadataResponse,
  TestConnectionResponse,
} from './types'

async function request<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await fetch(`/api${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as
      | { error?: string }
      | null
    throw new Error(payload?.error ?? `Request failed with ${response.status}.`)
  }

  return (await response.json()) as T
}

export const api = {
  testConnection(connection: ConnectionProfile) {
    return request<TestConnectionResponse>('/connection/test', { connection })
  },
  disconnectConnection(connection: ConnectionProfile) {
    return request<{ ok: boolean }>('/connection/disconnect', { connection })
  },
  fetchSchemaTree(connection: ConnectionProfile) {
    return request<SchemaTreeResponse>('/schema/tree', { connection })
  },
  fetchTableColumns(connection: ConnectionProfile, schemaName: string, tableName: string) {
    return request<TableColumnsResponse>('/schema/table', {
      connection,
      schemaName,
      tableName,
    })
  },
  fetchTableMetadata(connection: ConnectionProfile, schemaName: string, tableName: string) {
    return request<TableMetadataResponse>('/schema/table-meta', {
      connection,
      schemaName,
      tableName,
    })
  },
  runQuery(connection: ConnectionProfile, sql: string, database?: string, queryId?: string) {
    return request<QueryExecutionResponse>('/query', { connection, sql, database, queryId })
  },
  runStatements(
    connection: ConnectionProfile,
    statements: string[],
    database?: string,
    transaction = true,
  ) {
    return request<QueryExecutionResponse>('/query/batch', {
      connection,
      statements,
      database,
      transaction,
    })
  },
  cancelQuery(queryId: string) {
    return request<{ ok: boolean }>('/query/cancel', { queryId })
  },
  importBatch(
    connection: ConnectionProfile,
    database: string,
    table: string,
    mode: 'append' | 'replace' | 'upsert',
    primaryKeys: string[],
    columns: { source: string; target: string }[],
    rows: Record<string, unknown>[],
    createTable?: ImportCreateTablePlan | null,
  ) {
    return request<{
      processed: number
      inserted: number
      updated: number
      errors: string[]
    }>('/import/batch', {
      connection,
      database,
      table,
      mode,
      primaryKeys,
      columns,
      rows,
      createTable,
    })
  },
}
