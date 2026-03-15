import type { TableColumn } from './types'

type CursorValueMap = Record<string, unknown>

function q(identifier: string) {
  return `\`${identifier.replaceAll('`', '``')}\``
}

function sqlLiteral(value: unknown) {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'number' || typeof value === 'bigint') return String(value)
  if (typeof value === 'boolean') return value ? '1' : '0'
  return `'${String(value).replace(/\\/g, '\\\\').replace(/'/g, "''")}'`
}

export function getPrimaryKeyColumns(columns: Pick<TableColumn, 'name' | 'key'>[]) {
  return columns
    .filter((column) => column.key === 'PRI')
    .map((column) => column.name)
}

export function readPrimaryKeyCursor(
  row: Record<string, unknown>,
  primaryKeyColumns: string[],
): CursorValueMap | null {
  if (primaryKeyColumns.length === 0) {
    return null
  }

  const cursor: CursorValueMap = {}

  for (const column of primaryKeyColumns) {
    if (!(column in row)) {
      return null
    }
    cursor[column] = row[column]
  }

  return cursor
}

export function buildBatchSelectQuery(options: {
  batchSize: number
  baseWhereClause?: string
  fallbackOffset: number
  primaryKeyColumns: string[]
  schemaName: string
  selectColumns: string[]
  tableName: string
  cursor?: CursorValueMap | null
}) {
  const {
    batchSize,
    baseWhereClause,
    cursor,
    fallbackOffset,
    primaryKeyColumns,
    schemaName,
    selectColumns,
    tableName,
  } = options

  const queryColumns = Array.from(new Set([
    ...selectColumns,
    ...primaryKeyColumns,
  ]))

  if (primaryKeyColumns.length > 0) {
    const orderClause = primaryKeyColumns
      .map((column) => `${q(column)} ASC`)
      .join(', ')
    const filters: string[] = []

    if (baseWhereClause) {
      filters.push(`(${baseWhereClause})`)
    }

    if (cursor) {
      filters.push(`(${primaryKeyColumns.map((column) => q(column)).join(', ')}) > (${primaryKeyColumns.map((column) => sqlLiteral(cursor[column])).join(', ')})`)
    }

    const whereClause = filters.length > 0
      ? ` WHERE ${filters.join(' AND ')}`
      : ''

    return {
      mode: 'primaryKey' as const,
      queryColumns,
      sql: `SELECT ${queryColumns.map((column) => q(column)).join(', ')} FROM ${q(schemaName)}.${q(tableName)}${whereClause} ORDER BY ${orderClause} LIMIT ${batchSize}`,
    }
  }

  return {
    mode: 'offset' as const,
    queryColumns,
    sql: `SELECT ${queryColumns.map((column) => q(column)).join(', ')} FROM ${q(schemaName)}.${q(tableName)}${baseWhereClause ? ` WHERE ${baseWhereClause}` : ''} LIMIT ${batchSize} OFFSET ${fallbackOffset}`,
  }
}
