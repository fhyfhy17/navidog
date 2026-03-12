import type {
  ConnectionProfile,
  QueryExecutionResponse,
  SchemaTreeResponse,
  TableColumnsResponse,
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
  runQuery(connection: ConnectionProfile, sql: string, database?: string) {
    return request<QueryExecutionResponse>('/query', { connection, sql, database })
  },
}
