import { memo, startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { api } from './api'
import SqlEditor from './components/SqlEditor'
import type { SqlEditorHandle } from './components/SqlEditor'
import TableDesigner from './components/TableDesigner'
import type { ColumnDef } from './components/TableDesigner'
import ImportWizard from './components/ImportWizard'
import ExportWizard from './components/ExportWizard'
import type {
  AppTab,
  CliTab,
  ConnectionProfile,
  DataTab,
  DesignTab,
  QueryTab,
  SchemaNode,
  SelectedNode,
  TableColumn,
  TableForeignKey,
  TableIndex,
} from './types'

/* ═══════════════════════════════════════════════
   Constants
   ═══════════════════════════════════════════════ */

const STORAGE_PROFILES = 'navidog.profiles.v1'
const STORAGE_HISTORY = 'navidog.history.v1'
const STORAGE_SEARCH_HISTORY = 'navidog.search-history.v1'
const STORAGE_QUERY_TABS = 'navidog.query-tabs.v1'
const STORAGE_ACTIVE_QUERY_TAB = 'navidog.active-query-tab.v1'
const PAGE_SIZE = 1000

/* ═══════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════ */

function readStorage<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as T) : fallback
  } catch {
    return fallback
  }
}

function writeStorage<T>(key: string, value: T) {
  localStorage.setItem(key, JSON.stringify(value))
}

function q(identifier: string) {
  return `\`${identifier.replaceAll('`', '``')}\``
}

function formatCell(value: unknown): { text: string; isNull: boolean } {
  if (value === null || value === undefined) return { text: 'NULL', isNull: true }
  if (typeof value === 'object') return { text: JSON.stringify(value), isNull: false }
  if (typeof value === 'boolean') return { text: value ? 'true' : 'false', isNull: false }
  return { text: String(value), isNull: false }
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  const val = bytes / Math.pow(1024, i)
  return `${val < 10 ? val.toFixed(1) : Math.round(val)} ${units[i]}`
}

function isQueryCancelledMessage(error: unknown) {
  return error instanceof Error && error.message === 'Query was cancelled.'
}

function escapeCsvValue(value: unknown) {
  if (value === null || value === undefined) return ''
  const text = String(value)
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`
  }
  return text
}

function downloadTextFile(fileName: string, content: string, mimeType = 'text/plain;charset=utf-8') {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = fileName
  link.click()
  URL.revokeObjectURL(url)
}

function buildResultFileName(schemaName: string, resultIndex: number, extension: 'csv' | 'json') {
  const base = (schemaName || 'query')
    .replace(/[^\w.-]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'query'
  return `${base}-result-${resultIndex + 1}.${extension}`
}

function normalizeSearchHistoryValue(value: string) {
  return value.trim().replace(/\s+/g, ' ').toLowerCase()
}

function pushRecentSearch(values: string[], rawValue: string, limit = 5) {
  const value = rawValue.trim().replace(/\s+/g, ' ')
  if (!value) return values
  const normalized = normalizeSearchHistoryValue(value)
  return [
    value,
    ...values.filter((item) => normalizeSearchHistoryValue(item) !== normalized),
  ].slice(0, limit)
}

type SavePickerWindow = Window & {
  showSaveFilePicker?: (options: {
    suggestedName: string
    types: Array<{
      description: string
      accept: Record<string, string[]>
    }>
  }) => Promise<FileSystemFileHandle>
}



type PersistedQueryTab = {
  id: string
  title: string
  connectionId: string
  schemaName: string
  sql: string
}

function readPersistedQueryTabs(): QueryTab[] {
  const stored = readStorage<PersistedQueryTab[]>(STORAGE_QUERY_TABS, [])
  return stored.map((tab) => ({
    ...tab,
    kind: 'query' as const,
    results: [],
    activeResultIndex: 0,
    durationMs: 0,
    loading: false,
  }))
}

function createRafUpdater<T>(apply: (value: T) => void) {
  let frameId = 0
  let pendingValue: T | null = null

  function flush() {
    if (frameId) {
      window.cancelAnimationFrame(frameId)
      frameId = 0
    }
    if (pendingValue === null) return
    const value = pendingValue
    pendingValue = null
    apply(value)
  }

  return {
    schedule(value: T) {
      pendingValue = value
      if (frameId) return
      frameId = window.requestAnimationFrame(() => {
        frameId = 0
        flush()
      })
    },
    flush,
    cancel() {
      if (frameId) {
        window.cancelAnimationFrame(frameId)
        frameId = 0
      }
      pendingValue = null
    },
  }
}

const QUERY_ROW_NUM_WIDTH = 44
const QUERY_MIN_COL_WIDTH = 96
const QUERY_MAX_AUTO_COL_WIDTH = 280

function estimateQueryColumnWidth(column: string) {
  return Math.max(
    QUERY_MIN_COL_WIDTH,
    Math.min(QUERY_MAX_AUTO_COL_WIDTH, 36 + column.length * 10),
  )
}

/** Lightweight SQL syntax highlighter for DDL display */
function highlightSql(sql: string): string {
  const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  // Tokenize: strings, comments, words, numbers, punctuation, whitespace
  const tokenRe = /('(?:[^'\\]|\\.)*'|`[^`]*`)|(--[^\n]*|#[^\n]*|\/\*[\s\S]*?\*\/)|([a-zA-Z_]\w*)|([0-9]+(?:\.[0-9]+)?)|([\r\n]+)|([(),.;=])|( +)|(\S)/g
  const kw = new Set('CREATE,TABLE,ALTER,DROP,INSERT,UPDATE,DELETE,SELECT,FROM,WHERE,AND,OR,NOT,NULL,DEFAULT,PRIMARY,KEY,UNIQUE,INDEX,USING,ENGINE,CHARSET,COLLATE,COMMENT,AUTO_INCREMENT,IF,EXISTS,SET,VALUES,INTO,ON,ADD,COLUMN,MODIFY,CHANGE,RENAME,TO,AS,LIKE,IN,IS,BETWEEN,JOIN,LEFT,RIGHT,INNER,OUTER,CROSS,ORDER,BY,GROUP,HAVING,LIMIT,OFFSET,UNION,ALL,DISTINCT,FOREIGN,REFERENCES,CASCADE,RESTRICT,CHECK,CONSTRAINT,TEMPORARY,REPLACE,IGNORE,LOCK,UNLOCK,TRUNCATE,BEGIN,COMMIT,ROLLBACK,GRANT,REVOKE,SHOW,DESCRIBE,EXPLAIN,USE,DATABASE,SCHEMA,ROW_FORMAT,COMPACT,DYNAMIC,REDUNDANT,COMPRESSED,BTREE,HASH'.split(','))
  const types = new Set('INT,TINYINT,SMALLINT,MEDIUMINT,BIGINT,FLOAT,DOUBLE,DECIMAL,NUMERIC,BIT,BOOLEAN,BOOL,CHAR,VARCHAR,TEXT,TINYTEXT,MEDIUMTEXT,LONGTEXT,BLOB,TINYBLOB,MEDIUMBLOB,LONGBLOB,DATE,DATETIME,TIMESTAMP,TIME,YEAR,ENUM,JSON,BINARY,VARBINARY,UNSIGNED,SIGNED,ZEROFILL,CHARACTER'.split(','))
  let result = ''
  let m: RegExpExecArray | null
  while ((m = tokenRe.exec(sql)) !== null) {
    const [full, str, comment, word, num, nl, punct, space, other] = m
    if (str) {
      result += `<span class="sql-str">${esc(full)}</span>`
    } else if (comment) {
      result += `<span class="sql-comment">${esc(full)}</span>`
    } else if (word) {
      const upper = word.toUpperCase()
      if (kw.has(upper)) result += `<span class="sql-kw">${esc(full)}</span>`
      else if (types.has(upper)) result += `<span class="sql-type">${esc(full)}</span>`
      else result += esc(full)
    } else if (num) {
      result += `<span class="sql-num">${esc(full)}</span>`
    } else if (nl) {
      result += full
    } else if (punct) {
      result += `<span class="sql-punct">${esc(full)}</span>`
    } else if (space) {
      result += full
    } else if (other) {
      result += esc(full)
    }
  }
  return result
}

/** Search input with recent history dropdown */
function SearchWithHistory({ wrapperClassName, className, placeholder, value, onChange, history, onCommit }: {
  wrapperClassName?: string
  className: string
  placeholder: string
  value: string
  onChange: (v: string) => void
  history: string[]
  onCommit: (v: string) => void
}) {
  const [open, setOpen] = useState(false)
  const wrapRef = useRef<HTMLDivElement>(null)
  const normalizedValue = normalizeSearchHistoryValue(value)
  const filtered = history.filter((item) => {
    const normalizedItem = normalizeSearchHistoryValue(item)
    if (normalizedItem === normalizedValue) return false
    if (!normalizedValue) return true
    return normalizedItem.includes(normalizedValue)
  })

  function commitValue(rawValue: string) {
    const nextValue = rawValue.trim().replace(/\s+/g, ' ')
    if (!nextValue) return
    onCommit(nextValue)
  }

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  return (
    <div
      ref={wrapRef}
      className={['search-with-history-wrap', wrapperClassName].filter(Boolean).join(' ')}
    >
      <input
        className={className}
        placeholder={placeholder}
        value={value}
        onChange={e => {
          onChange(e.target.value)
          if (!open && history.length > 0) setOpen(true)
        }}
        onFocus={() => filtered.length > 0 && setOpen(true)}
        onBlur={() => commitValue(value)}
        onKeyDown={e => {
          if (e.key === 'Enter' && value.trim()) {
            commitValue(value)
            setOpen(false)
          }
          if (e.key === 'Escape') setOpen(false)
        }}
        style={{ width: '100%' }}
      />
      {open && filtered.length > 0 && (
        <div className="search-history-dropdown">
          {filtered.map(h => (
            <div
              key={h}
              className="search-history-item"
              onMouseDown={e => {
                e.preventDefault()
                onChange(h)
                commitValue(h)
                setOpen(false)
              }}
            >
              <span style={{ opacity: 0.4, marginRight: 6 }}>🕘</span>{h}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
type ConnectionDraft = {
  id?: string
  label: string
  host: string
  port: string
  user: string
  password: string
  database: string
  ssl: boolean
  sslRejectUnauthorized: boolean
  // SSH
  useSSH: boolean
  sshHost: string
  sshPort: string
  sshUser: string
  sshAuthMethod: 'password' | 'privateKey'
  sshPrivateKey: string
  sshPassphrase: string
  sshPassword: string
}

function blankDraft(): ConnectionDraft {
  return { label: '', host: '127.0.0.1', port: '3306', user: 'root', password: '', database: '',
    ssl: false, sslRejectUnauthorized: false,
    useSSH: false, sshHost: '', sshPort: '22', sshUser: 'root', sshAuthMethod: 'privateKey',
    sshPrivateKey: '~/.ssh/id_rsa', sshPassphrase: '', sshPassword: '' }
}

function profileToDraft(p: ConnectionProfile): ConnectionDraft {
  return { id: p.id, label: p.label, host: p.host, port: String(p.port), user: p.user, password: p.password, database: p.database ?? '',
    ssl: p.ssl ?? false, sslRejectUnauthorized: p.sslRejectUnauthorized ?? false,
    useSSH: p.useSSH ?? false, sshHost: p.sshHost ?? '', sshPort: String(p.sshPort ?? 22), sshUser: p.sshUser ?? 'root',
    sshAuthMethod: p.sshAuthMethod ?? 'privateKey', sshPrivateKey: p.sshPrivateKey ?? '~/.ssh/id_rsa',
    sshPassphrase: p.sshPassphrase ?? '', sshPassword: p.sshPassword ?? '' }
}

function draftToProfile(d: ConnectionDraft): ConnectionProfile {
  const host = d.host.trim()
  const user = d.user.trim()
  const port = parseInt(d.port, 10)
  if (!host) throw new Error('主机不能为空')
  if (!user) throw new Error('用户名不能为空')
  if (isNaN(port) || port <= 0) throw new Error('端口必须为正整数')
  return {
    id: d.id ?? crypto.randomUUID(),
    label: d.label.trim() || `${user}@${host}`,
    host, port, user,
    password: d.password,
    database: d.database.trim() || undefined,
    ssl: d.ssl,
    sslRejectUnauthorized: d.ssl ? d.sslRejectUnauthorized : undefined,
    useSSH: d.useSSH,
    sshHost: d.sshHost.trim() || undefined,
    sshPort: parseInt(d.sshPort, 10) || 22,
    sshUser: d.sshUser.trim() || undefined,
    sshAuthMethod: d.sshAuthMethod,
    sshPrivateKey: d.sshPrivateKey.trim() || undefined,
    sshPassphrase: d.sshPassphrase || undefined,
    sshPassword: d.sshPassword || undefined,
  }
}

/* ═══════════════════════════════════════════════
   Virtual-scroll query grid (standalone component)
   ═══════════════════════════════════════════════ */

const ROW_HEIGHT = 28
const OVERSCAN = 10

const VirtualQueryGrid = memo(function VirtualQueryGrid({
  columns,
  rows,
}: {
  columns: string[]
  rows: Record<string, unknown>[]
}) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [scrollTop, setScrollTop] = useState(0)
  const [viewHeight, setViewHeight] = useState(400)
  const [manualColumnWidths, setManualColumnWidths] = useState<Record<string, number>>({})
  const scrollUpdaterRef = useRef(createRafUpdater<number>((value) => {
    setScrollTop((prev) => (prev === value ? prev : value))
  }))
  const columnWidths = useMemo(
    () => Object.fromEntries(
      columns.map((column) => [column, manualColumnWidths[column] ?? estimateQueryColumnWidth(column)]),
    ),
    [columns, manualColumnWidths],
  )

  // Observe container size
  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    setViewHeight((prev) => (prev === el.clientHeight ? prev : el.clientHeight))
    const ro = new ResizeObserver(() => {
      const nextHeight = el.clientHeight
      setViewHeight((prev) => (prev === nextHeight ? prev : nextHeight))
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  useEffect(() => () => {
    scrollUpdaterRef.current.cancel()
  }, [])

  // Reset scroll when data changes
  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = 0
  }, [rows])

  const startIdx = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN)
  const visibleCount = Math.ceil(viewHeight / ROW_HEIGHT) + OVERSCAN * 2
  const endIdx = Math.min(rows.length, startIdx + visibleCount)
  const topPad = startIdx * ROW_HEIGHT
  const bottomPad = Math.max(0, (rows.length - endIdx) * ROW_HEIGHT)
  const visibleRows = useMemo(() => rows.slice(startIdx, endIdx), [rows, startIdx, endIdx])
  const tableWidth = useMemo(
    () => QUERY_ROW_NUM_WIDTH + columns.reduce((total, column) => total + (columnWidths[column] ?? estimateQueryColumnWidth(column)), 0),
    [columnWidths, columns],
  )

  const handleScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    scrollUpdaterRef.current.schedule((e.target as HTMLElement).scrollTop)
  }, [])

  const handleColumnResizeStart = useCallback((column: string, e: React.MouseEvent<HTMLSpanElement>) => {
    e.preventDefault()
    e.stopPropagation()
    const startX = e.clientX
    const startW = columnWidths[column] ?? estimateQueryColumnWidth(column)
    const widthUpdater = createRafUpdater<number>((width) => {
      setManualColumnWidths((prev) => (prev[column] === width ? prev : { ...prev, [column]: width }))
    })

    function onMove(ev: MouseEvent) {
      const width = Math.max(QUERY_MIN_COL_WIDTH, startW + ev.clientX - startX)
      widthUpdater.schedule(width)
    }

    function onUp() {
      widthUpdater.flush()
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      document.body.style.userSelect = ''
      document.body.style.cursor = ''
    }

    document.body.style.userSelect = 'none'
    document.body.style.cursor = 'col-resize'
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }, [columnWidths])

  return (
    <div
      ref={scrollRef}
      className="virtual-grid-scroll"
      style={{ flex: 1, overflow: 'auto', minHeight: 0 }}
      onScroll={handleScroll}
    >
      <table className="data-grid query-result-grid" style={{ width: tableWidth, minWidth: tableWidth }}>
        <colgroup>
          <col style={{ width: QUERY_ROW_NUM_WIDTH, minWidth: QUERY_ROW_NUM_WIDTH }} />
          {columns.map((column) => {
            const width = columnWidths[column] ?? estimateQueryColumnWidth(column)
            return <col key={column} style={{ width, minWidth: width }} />
          })}
        </colgroup>
        <thead>
          <tr>
            <th className="row-num-header">#</th>
            {columns.map((col) => {
              const width = columnWidths[col] ?? estimateQueryColumnWidth(col)
              return (
                <th key={col} style={{ width, minWidth: width }}>
                  {col}
                  <span className="col-resize-handle" onMouseDown={(e) => handleColumnResizeStart(col, e)} />
                </th>
              )
            })}
          </tr>
        </thead>
        <tbody>
          {topPad > 0 && <tr style={{ height: topPad }} />}
          {visibleRows.map((row, vi) => {
            const i = startIdx + vi
            return (
              <tr key={i} style={{ height: ROW_HEIGHT }}>
                <td className="row-num">{i + 1}</td>
                {columns.map((col) => {
                  const { text, isNull } = formatCell(row[col])
                  const width = columnWidths[col] ?? estimateQueryColumnWidth(col)
                  return (
                    <td key={col} className={isNull ? 'cell-null' : ''} style={{ width, minWidth: width, maxWidth: width }}>
                      {text}
                    </td>
                  )
                })}
              </tr>
            )
          })}
          {bottomPad > 0 && <tr style={{ height: bottomPad }} />}
        </tbody>
      </table>
    </div>
  )
})

type DataGridRow = {
  data: Record<string, unknown>
  type: 'existing' | 'new'
  originalIdx: number
}

type VirtualDataGridProps = {
  columns: string[]
  rows: DataGridRow[]
  rowOffset: number
  resetScrollKey: unknown
  selectedRows: Set<number>
  deletedRows: Set<number>
  editingCell: { row: number; col: string } | null
  orderBy: { col: string; dir: 'ASC' | 'DESC' } | null
  colSortMenu: string | null
  onSetSortMenu: (column: string | null) => void
  onApplySort: (column: string, dir: 'ASC' | 'DESC' | null) => void
  onRowClick: (event: React.MouseEvent, rowIndex: number) => void
  onCellContextMenu: (event: React.MouseEvent, cellText: string, colName: string, rowIndex: number) => void
  onStartEdit: (rowIndex: number, colName: string) => void
  onCommitCell: (row: DataGridRow, colName: string, value: string) => void
  onCancelEdit: () => void
  getCellValue: (row: DataGridRow, colName: string) => unknown
  isCellModified: (row: DataGridRow, colName: string) => boolean
}

function VirtualDataGrid({
  columns,
  rows,
  rowOffset,
  resetScrollKey,
  selectedRows,
  deletedRows,
  editingCell,
  orderBy,
  colSortMenu,
  onSetSortMenu,
  onApplySort,
  onRowClick,
  onCellContextMenu,
  onStartEdit,
  onCommitCell,
  onCancelEdit,
  getCellValue,
  isCellModified,
}: VirtualDataGridProps) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [scrollTop, setScrollTop] = useState(0)
  const [viewHeight, setViewHeight] = useState(400)
  const [manualColumnWidths, setManualColumnWidths] = useState<Record<string, number>>({})
  const scrollUpdaterRef = useRef(createRafUpdater<number>((value) => {
    setScrollTop((prev) => (prev === value ? prev : value))
  }))

  const columnWidths = useMemo(
    () => Object.fromEntries(
      columns.map((column) => [column, manualColumnWidths[column] ?? estimateQueryColumnWidth(column)]),
    ),
    [columns, manualColumnWidths],
  )

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    setViewHeight((prev) => (prev === el.clientHeight ? prev : el.clientHeight))
    const ro = new ResizeObserver(() => {
      const nextHeight = el.clientHeight
      setViewHeight((prev) => (prev === nextHeight ? prev : nextHeight))
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  useEffect(() => () => {
    scrollUpdaterRef.current.cancel()
  }, [])

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = 0
  }, [resetScrollKey])

  const startIdx = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN)
  const visibleCount = Math.ceil(viewHeight / ROW_HEIGHT) + OVERSCAN * 2
  const endIdx = Math.min(rows.length, startIdx + visibleCount)
  const topPad = startIdx * ROW_HEIGHT
  const bottomPad = Math.max(0, (rows.length - endIdx) * ROW_HEIGHT)
  const visibleRows = useMemo(() => rows.slice(startIdx, endIdx), [rows, startIdx, endIdx])
  const tableWidth = useMemo(
    () => QUERY_ROW_NUM_WIDTH + columns.reduce((total, column) => total + (columnWidths[column] ?? estimateQueryColumnWidth(column)), 0),
    [columnWidths, columns],
  )

  const handleScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    scrollUpdaterRef.current.schedule((e.target as HTMLElement).scrollTop)
  }, [])

  const handleColumnResizeStart = useCallback((column: string, e: React.MouseEvent<HTMLSpanElement>) => {
    e.preventDefault()
    e.stopPropagation()
    const startX = e.clientX
    const startW = columnWidths[column] ?? estimateQueryColumnWidth(column)
    const widthUpdater = createRafUpdater<number>((width) => {
      setManualColumnWidths((prev) => (prev[column] === width ? prev : { ...prev, [column]: width }))
    })

    function onMove(ev: MouseEvent) {
      const width = Math.max(QUERY_MIN_COL_WIDTH, startW + ev.clientX - startX)
      widthUpdater.schedule(width)
    }

    function onUp() {
      widthUpdater.flush()
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      document.body.style.userSelect = ''
      document.body.style.cursor = ''
    }

    document.body.style.userSelect = 'none'
    document.body.style.cursor = 'col-resize'
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }, [columnWidths])

  return (
    <div
      ref={scrollRef}
      className="data-grid-wrap"
      onClick={() => onSetSortMenu(null)}
      onScroll={handleScroll}
    >
      <table className="data-grid" style={{ width: tableWidth, minWidth: tableWidth, tableLayout: 'fixed' }}>
        <colgroup>
          <col style={{ width: QUERY_ROW_NUM_WIDTH, minWidth: QUERY_ROW_NUM_WIDTH }} />
          {columns.map((column) => {
            const width = columnWidths[column] ?? estimateQueryColumnWidth(column)
            return <col key={column} style={{ width, minWidth: width }} />
          })}
        </colgroup>
        <thead>
          <tr>
            <th className="row-num-header">#</th>
            {columns.map((col) => {
              const width = columnWidths[col] ?? estimateQueryColumnWidth(col)
              return (
                <th key={col} className="col-header" style={{ width, minWidth: width }}>
                  <span className="col-header-label">
                    {col}
                    {orderBy?.col === col && <span className="sort-indicator">{orderBy.dir === 'ASC' ? ' ↑' : ' ↓'}</span>}
                  </span>
                  <span className="col-sort-trigger" onClick={(e) => { e.stopPropagation(); onSetSortMenu(colSortMenu === col ? null : col) }}>▾</span>
                  {colSortMenu === col && (
                    <div className="col-sort-menu" onClick={(e) => e.stopPropagation()}>
                      <div className="col-sort-item" onClick={() => onApplySort(col, 'ASC')}>↓<span style={{ color: '#4285f4' }}>A</span><span style={{ color: '#4285f4' }}>Z</span>&nbsp; 升序排序</div>
                      <div className="col-sort-item" onClick={() => onApplySort(col, 'DESC')}>↓<span style={{ color: '#ea4335' }}>Z</span><span style={{ color: '#ea4335' }}>A</span>&nbsp; 降序排序</div>
                      <div className="col-sort-item" onClick={() => onApplySort(col, null)}>↓<span style={{ color: '#ea4335' }}>⊘</span>&nbsp; 移除排序</div>
                    </div>
                  )}
                  <span className="col-resize-handle" onMouseDown={(e) => handleColumnResizeStart(col, e)} />
                </th>
              )
            })}
          </tr>
        </thead>
        <tbody>
          {topPad > 0 && (
            <tr aria-hidden="true">
              <td colSpan={columns.length + 1} style={{ height: topPad, padding: 0, border: 0 }} />
            </tr>
          )}
          {visibleRows.map((item, visibleIdx) => {
            const rowIdx = startIdx + visibleIdx
            const isDeleted = item.type === 'existing' && deletedRows.has(item.originalIdx)
            const isNew = item.type === 'new'
            let rowClass = ''
            if (selectedRows.has(rowIdx)) rowClass += ' row-selected'
            if (isDeleted) rowClass += ' row-deleted'
            if (isNew) rowClass += ' row-new'

            return (
              <tr
                key={`${item.type}:${item.originalIdx}`}
                className={rowClass.trim()}
                style={{ height: ROW_HEIGHT }}
                onClick={(e) => onRowClick(e, rowIdx)}
              >
                <td className="row-num">{rowOffset + rowIdx + 1}{isNew ? ' +' : ''}{isDeleted ? ' −' : ''}</td>
                {columns.map((col) => {
                  const width = columnWidths[col] ?? estimateQueryColumnWidth(col)
                  const isEditing = editingCell?.row === rowIdx && editingCell?.col === col
                  const rawVal = getCellValue(item, col)
                  const { text, isNull } = formatCell(rawVal)
                  const modified = !isNew && isCellModified(item, col)
                  let cellClass = ''
                  if (isNull) cellClass += ' cell-null'
                  if (modified) cellClass += ' cell-modified'

                  if (isEditing) {
                    return (
                      <td key={col} className={cellClass.trim()} style={{ width, minWidth: width, maxWidth: width }}>
                        <input
                          className="cell-edit-input"
                          autoFocus
                          defaultValue={text === 'NULL' ? '' : text}
                          onBlur={(e) => {
                            onCommitCell(item, col, e.target.value)
                            onCancelEdit()
                          }}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') (e.target as HTMLInputElement).blur()
                            if (e.key === 'Escape') onCancelEdit()
                            if (e.key === 'Tab') {
                              e.preventDefault()
                              const ci = columns.indexOf(col)
                              const nextCol = columns[ci + 1]
                              if (nextCol) {
                                ;(e.target as HTMLInputElement).blur()
                                setTimeout(() => onStartEdit(rowIdx, nextCol), 0)
                              } else {
                                ;(e.target as HTMLInputElement).blur()
                              }
                            }
                          }}
                        />
                      </td>
                    )
                  }

                  return (
                    <td
                      key={col}
                      className={cellClass.trim()}
                      style={{ width, minWidth: width, maxWidth: width }}
                      onDoubleClick={() => !isDeleted && onStartEdit(rowIdx, col)}
                      onContextMenu={(e) => onCellContextMenu(e, text, col, rowIdx)}
                    >{text}</td>
                  )
                })}
              </tr>
            )
          })}
          {bottomPad > 0 && (
            <tr aria-hidden="true">
              <td colSpan={columns.length + 1} style={{ height: bottomPad, padding: 0, border: 0 }} />
            </tr>
          )}
          {rows.length === 0 && (
            <tr><td colSpan={columns.length + 1} style={{ color: '#aaa', textAlign: 'center', padding: 20 }}>无数据</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

type QueryResultsPaneProps = {
  results: QueryTab['results']
  activeResultIndex: number
  durationMs: number
  onSetActiveResult: (index: number) => void
  onExportActiveResult: (format: 'csv' | 'json') => void
}

const QueryResultsPane = memo(function QueryResultsPane({
  results,
  activeResultIndex,
  durationMs,
  onSetActiveResult,
  onExportActiveResult,
}: QueryResultsPaneProps) {
  const activeResult = results[activeResultIndex] ?? null
  const activeRowsResult = activeResult?.kind === 'rows' ? activeResult : null

  return (
    <div className="query-results">
      {results.length > 0 && (
        <div className="result-view-tabs">
          {results.some(r => r.kind === 'rows') && results.map((r, i) => r.kind === 'rows' ? (
            <button
              key={i}
              className={`result-view-tab${activeResultIndex === i ? ' active' : ''}`}
              onClick={() => onSetActiveResult(i)}
            >
              {r.title || `结果 ${i + 1}`}
            </button>
          ) : null)}
          <button
            className={`result-view-tab${activeResultIndex === -1 ? ' active' : ''}`}
            onClick={() => onSetActiveResult(-1)}
          >
            消息
          </button>
          <button
            className={`result-view-tab${activeResultIndex === -2 ? ' active' : ''}`}
            onClick={() => onSetActiveResult(-2)}
          >
            摘要
          </button>
          <div style={{ flex: 1 }} />
          {activeRowsResult && (
            <div className="result-view-actions">
              <button
                type="button"
                className="result-action-btn"
                onClick={() => onExportActiveResult('csv')}
                title="导出当前结果为 CSV"
              >
                CSV
              </button>
              <button
                type="button"
                className="result-action-btn"
                onClick={() => onExportActiveResult('json')}
                title="导出当前结果为 JSON"
              >
                JSON
              </button>
            </div>
          )}
          {durationMs > 0 && (
            <span style={{ fontSize: 11, color: '#888', padding: '0 8px', alignSelf: 'center' }}>运行时间: {(durationMs / 1000).toFixed(3)}s</span>
          )}
        </div>
      )}
      <div className="result-grid-wrap">
        {results.length === 0 ? (
          <div className="empty-state" style={{ padding: 40 }}>
            <span style={{ fontSize: 13, color: '#aaa' }}>运行查询以查看结果</span>
          </div>
        ) : activeResultIndex === -1 ? (
          <div className="query-messages">
            {results.map((r, i) => (
              <div key={i} className="query-message-item">
                {r.kind === 'rows' ? (
                  <>
                    <div className="qm-sql">{r.title}</div>
                    <div className="qm-info">&gt; {r.rows.length} 条记录</div>
                    <div className="qm-time">&gt; Time: {(durationMs / 1000 / results.length).toFixed(3)}s</div>
                  </>
                ) : r.kind === 'mutation' ? (
                  <>
                    <div className="qm-sql">{r.title}</div>
                    <div className="qm-info">&gt; Affected rows: {r.affectedRows}</div>
                    <div className="qm-time">&gt; Time: {(durationMs / 1000 / results.length).toFixed(3)}s</div>
                  </>
                ) : (
                  <>
                    <div className="qm-sql">{r.title}</div>
                    <div className="qm-info">&gt; {r.message}</div>
                  </>
                )}
              </div>
            ))}
          </div>
        ) : activeResultIndex === -2 ? (
          <table className="data-grid">
            <thead>
              <tr>
                <th>Query</th>
                <th>Message</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => (
                <tr key={i}>
                  <td style={{ maxWidth: 400 }}>{r.title}</td>
                  <td>
                    {r.kind === 'rows' ? `${r.rows.length} 条记录` : r.kind === 'mutation' ? `Affected rows: ${r.affectedRows}` : r.message}
                  </td>
                  <td>{(durationMs / 1000 / results.length).toFixed(6)}s</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : activeResult?.kind === 'rows' ? (
          <VirtualQueryGrid
            columns={activeResult.columns}
            rows={activeResult.rows}
          />
        ) : activeResult?.kind === 'mutation' ? (
          <div className="query-messages">
            <div className="query-message-item">
              <div className="qm-sql">{activeResult.title}</div>
              <div className="qm-info">&gt; Affected rows: {activeResult.affectedRows}</div>
              <div className="qm-time">&gt; Time: {(durationMs / 1000).toFixed(3)}s</div>
            </div>
          </div>
        ) : (
          <div className="empty-state" style={{ padding: 40 }}>
            <span style={{ fontSize: 13, color: '#aaa' }}>无结果</span>
          </div>
        )}
      </div>
    </div>
  )
}, (prev, next) => (
  prev.results === next.results &&
  prev.activeResultIndex === next.activeResultIndex &&
  prev.durationMs === next.durationMs
))

type QueryTabPaneProps = {
  tab: QueryTab
  schemas: SchemaNode[]
  history: string[]
  flash: (tone: 'success' | 'error', message: string) => void
  onPersistSql: (tabId: string, sql: string) => void
  onChangeSchema: (tabId: string, schemaName: string) => void
  onRunQuery: (tabId: string, sql?: string) => void
  onCancelRunningQuery: (tabId: string, activeQueryId?: string) => void
  onClearHistory: () => void
  onSetActiveResult: (tabId: string, index: number) => void
}

const QueryTabPane = memo(function QueryTabPane({
  tab,
  schemas,
  history,
  flash,
  onPersistSql,
  onChangeSchema,
  onRunQuery,
  onCancelRunningQuery,
  onClearHistory,
  onSetActiveResult,
}: QueryTabPaneProps) {
  const sqlEditorRef = useRef<SqlEditorHandle>(null)
  const [showHistoryPanel, setShowHistoryPanel] = useState(false)
  const draftSqlRef = useRef(tab.sql)
  const persistedSqlRef = useRef(tab.sql)
  const persistSqlRef = useRef(onPersistSql)
  const activeResult = tab.results[tab.activeResultIndex] ?? null
  const activeRowsResult = activeResult?.kind === 'rows' ? activeResult : null

  useEffect(() => {
    persistedSqlRef.current = tab.sql
  }, [tab.sql])

  useEffect(() => {
    persistSqlRef.current = onPersistSql
  }, [onPersistSql])

  const commitDraftSql = useCallback((sql = draftSqlRef.current) => {
    if (sql === persistedSqlRef.current) {
      return
    }
    persistedSqlRef.current = sql
    persistSqlRef.current(tab.id, sql)
  }, [tab.id])

  function handleEditorChange(sql: string) {
    draftSqlRef.current = sql
  }

  useEffect(() => () => {
    commitDraftSql()
  }, [commitDraftSql])

  function applyEditorSql(sql: string) {
    draftSqlRef.current = sql
    sqlEditorRef.current?.setValue(sql)
    commitDraftSql(sql)
  }

  function runActiveSql() {
    commitDraftSql()
    const sql = sqlEditorRef.current?.getRunnableSql().trim() ?? ''
    onRunQuery(tab.id, sql || draftSqlRef.current)
  }

  function runAllSql() {
    commitDraftSql()
    onRunQuery(tab.id, draftSqlRef.current)
  }

  function exportActiveResult(format: 'csv' | 'json') {
    if (!activeRowsResult) return

    if (format === 'json') {
      const payload = activeRowsResult.rows.map((row) => Object.fromEntries(
        activeRowsResult.columns.map((column) => [column, row[column] ?? null]),
      ))
      downloadTextFile(
        buildResultFileName(tab.schemaName, tab.activeResultIndex, 'json'),
        `${JSON.stringify(payload, null, 2)}\n`,
        'application/json;charset=utf-8',
      )
      flash('success', `已导出 ${activeRowsResult.rows.length} 行 JSON`)
      return
    }

    const csvLines = [
      activeRowsResult.columns.map((column) => escapeCsvValue(column)).join(','),
      ...activeRowsResult.rows.map((row) =>
        activeRowsResult.columns.map((column) => escapeCsvValue(row[column])).join(','),
      ),
    ]
    downloadTextFile(
      buildResultFileName(tab.schemaName, tab.activeResultIndex, 'csv'),
      `${csvLines.join('\n')}\n`,
      'text/csv;charset=utf-8',
    )
    flash('success', `已导出 ${activeRowsResult.rows.length} 行 CSV`)
  }

  return (
    <div className="query-pane">
      <div className="query-toolbar">
        {tab.loading ? (
          <button
            className="run-btn stop"
            onClick={() => onCancelRunningQuery(tab.id, tab.activeQueryId)}
            disabled={!tab.activeQueryId}
            title="停止执行"
          >
            ■ 停止
          </button>
        ) : (
          <button
            className="run-btn"
            onClick={runActiveSql}
            title="运行当前语句，或已选中的 SQL"
          >
            ▶ 运行
          </button>
        )}
        <div className="query-db-selector">
          <span className="db-selector-icon">📦</span>
          <select
            value={tab.schemaName}
            onChange={e => onChangeSchema(tab.id, e.target.value)}
          >
            <option value="">-- 选择数据库 --</option>
            {schemas.map(s => (
              <option key={s.name} value={s.name}>{s.name}</option>
            ))}
          </select>
        </div>

        <div style={{ position: 'relative' }}>
          <button className="action-btn" onClick={() => setShowHistoryPanel(prev => !prev)} title="执行历史">🕘</button>
          {showHistoryPanel && (
            <div className="history-panel">
              <div className="history-panel-header">
                <span>执行历史 ({history.length})</span>
                <button onClick={() => { onClearHistory(); setShowHistoryPanel(false) }} title="清空历史">🗑️</button>
              </div>
              {history.length === 0 ? (
                <div className="history-empty">暂无历史记录</div>
              ) : (
                <div className="history-list">
                  {history.map((sql, i) => (
                    <div
                      key={i}
                      className="history-item"
                      onClick={() => {
                        applyEditorSql(sql)
                        setShowHistoryPanel(false)
                      }}
                      title={sql}
                    >
                      <code>{sql.length > 120 ? `${sql.slice(0, 120)}...` : sql}</code>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
      <div
        className="query-split-container"
        ref={el => {
          if (!el) return
          const container = el
          if (el.dataset.splitInit) return
          el.dataset.splitInit = 'true'
          const editorArea = el.querySelector('.query-editor-area') as HTMLElement
          const handle = el.querySelector('.query-split-handle') as HTMLElement
          if (!editorArea || !handle) return

          let startY = 0
          let startH = 0
          const heightUpdater = createRafUpdater<number>((height) => {
            editorArea.style.height = `${height}px`
            editorArea.style.flex = 'none'
          })

          function onMouseMove(e: MouseEvent) {
            const delta = e.clientY - startY
            const newH = Math.max(60, Math.min(startH + delta, container.clientHeight - 80))
            heightUpdater.schedule(newH)
          }

          function onMouseUp() {
            heightUpdater.flush()
            document.removeEventListener('mousemove', onMouseMove)
            document.removeEventListener('mouseup', onMouseUp)
            document.body.style.userSelect = ''
            document.body.style.cursor = ''
          }

          handle.addEventListener('mousedown', (e: MouseEvent) => {
            e.preventDefault()
            startY = e.clientY
            startH = editorArea.offsetHeight
            document.body.style.userSelect = 'none'
            document.body.style.cursor = 'row-resize'
            document.addEventListener('mousemove', onMouseMove)
            document.addEventListener('mouseup', onMouseUp)
          })
        }}
      >
        <div className="query-editor-area">
          <SqlEditor
            ref={sqlEditorRef}
            value={tab.sql}
            onChange={handleEditorChange}
            onRun={runAllSql}
            onRunSelection={selectedSql => onRunQuery(tab.id, selectedSql)}
            schemas={schemas}
            currentSchema={tab.schemaName}
            suppressExternalValueSync
          />
        </div>
        <div className="query-split-handle" title="拖拽调整大小" />
        <QueryResultsPane
          results={tab.results}
          activeResultIndex={tab.activeResultIndex}
          durationMs={tab.durationMs}
          onSetActiveResult={(index) => onSetActiveResult(tab.id, index)}
          onExportActiveResult={exportActiveResult}
        />
      </div>
    </div>
  )
}, (prev, next) => (
  prev.tab === next.tab &&
  prev.schemas === next.schemas &&
  prev.history === next.history
))

type DesignTabWrapperProps = {
  tab: DesignTab
  profile: ConnectionProfile
  onSuccess: (message: string) => void
  onError: (message: string) => void
  onCancel: () => void
}

function mapColumnToDesignerColumn(column: TableColumn): ColumnDef {
  const typeMatch = /^([A-Z]+)(?:\(([^)]+)\))?/i.exec(column.type)
  return {
    name: column.name,
    type: (typeMatch?.[1] ?? column.type).toUpperCase(),
    length: typeMatch?.[2] ?? '',
    notNull: !column.nullable,
    isPK: column.key === 'PRI',
    autoIncrement: (column.extra ?? '').toLowerCase().includes('auto_increment'),
    defaultValue: column.defaultValue ?? '',
    comment: column.comment ?? '',
  }
}

function splitSqlStatements(sql: string) {
  return sql
    .split(/;\s*(?:\n|$)/)
    .map((statement) => statement.trim())
    .filter(Boolean)
}

function DesignTabWrapper({
  tab,
  profile,
  onSuccess,
  onError,
  onCancel,
}: DesignTabWrapperProps) {
  const [initialColumns, setInitialColumns] = useState<ColumnDef[] | null>(tab.mode === 'create' ? [] : null)
  const onErrorRef = useRef(onError)

  useEffect(() => {
    onErrorRef.current = onError
  }, [onError])

  useEffect(() => {
    if (tab.mode === 'create') {
      return
    }

    let cancelled = false

    void (async () => {
      try {
        const response = await api.fetchTableColumns(profile, tab.schemaName, tab.tableName)
        if (cancelled) return
        setInitialColumns(response.columns.map(mapColumnToDesignerColumn))
      } catch (err) {
        if (cancelled) return
        onErrorRef.current(`读取表结构失败: ${err instanceof Error ? err.message : String(err)}`)
      }
    })()

    return () => {
      cancelled = true
    }
  }, [profile, tab.id, tab.mode, tab.schemaName, tab.tableName])

  if (tab.mode === 'alter' && initialColumns === null) {
    return (
      <div className="empty-state">
        <span className="empty-title">正在加载表结构...</span>
      </div>
    )
  }

  return (
    <TableDesigner
      initialColumns={initialColumns ?? []}
      tableName={tab.tableName}
      schemaName={tab.schemaName}
      mode={tab.mode}
      onExecute={(sql) => {
        const statements = splitSqlStatements(sql)
        if (statements.length === 0) {
          onError('没有可执行的 SQL')
          return
        }

        void (async () => {
          try {
            await api.runStatements(profile, statements, tab.schemaName, true)
            onSuccess(tab.mode === 'create' ? `已创建表 ${tab.tableName}` : `已更新表 ${tab.tableName}`)
          } catch (err) {
            onError(`执行失败: ${err instanceof Error ? err.message : String(err)}`)
          }
        })()
      }}
      onCancel={onCancel}
    />
  )
}

/* ═══════════════════════════════════════════════
   App
   ═══════════════════════════════════════════════ */

export default function App() {
  /* ── Persisted state ────────────────────────── */
  const [profiles, setProfiles] = useState<ConnectionProfile[]>(() => readStorage(STORAGE_PROFILES, []))
  const [history, setHistory] = useState<string[]>(() => readStorage(STORAGE_HISTORY, []))
  const [searchHistory, setSearchHistory] = useState<{ tree: string[]; table: string[] }>(() => readStorage(STORAGE_SEARCH_HISTORY, { tree: [], table: [] }))


  useEffect(() => {
    const clearDragState = () => {
      document.body.style.userSelect = ''
      document.body.style.cursor = ''
    }

    window.addEventListener('mouseup', clearDragState)
    window.addEventListener('pointerup', clearDragState)
    window.addEventListener('dragend', clearDragState)
    window.addEventListener('blur', clearDragState)

    return () => {
      window.removeEventListener('mouseup', clearDragState)
      window.removeEventListener('pointerup', clearDragState)
      window.removeEventListener('dragend', clearDragState)
      window.removeEventListener('blur', clearDragState)
    }
  }, [])

  useEffect(() => { writeStorage(STORAGE_PROFILES, profiles) }, [profiles])
  useEffect(() => { writeStorage(STORAGE_HISTORY, history) }, [history])
  useEffect(() => { writeStorage(STORAGE_SEARCH_HISTORY, searchHistory) }, [searchHistory])


  /* ── Connection state ───────────────────────── */
  const [liveConnections, setLiveConnections] = useState<Map<string, { profile: ConnectionProfile; schemas: SchemaNode[]; version: string }>>(new Map())
  const [connectingProfiles, setConnectingProfiles] = useState<Set<string>>(new Set())
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set())
  const [selectedNode, setSelectedNode] = useState<SelectedNode | null>(null)
  const [treeFilter, setTreeFilter] = useState('')

  /* ── Tab state ──────────────────────────────── */
  const [tabs, setTabs] = useState<AppTab[]>(() => readPersistedQueryTabs())
  const [activeTabId, setActiveTabId] = useState<string | null>(() => {
    const restoredTabs = readPersistedQueryTabs()
    const savedActive = localStorage.getItem(STORAGE_ACTIVE_QUERY_TAB)
    return restoredTabs.find(tab => tab.id === savedActive)?.id ?? restoredTabs[0]?.id ?? null
  })

  /* ── Column cache ───────────────────────────── */
  const [columnCache, setColumnCache] = useState<Record<string, TableColumn[]>>({})

  /* ── DDL cache ──────────────────────────────── */
  const [ddlCache, setDdlCache] = useState<Record<string, string>>({})

  /* ── Table stats cache ─────────────────────── */
  const [tableStatsCache, setTableStatsCache] = useState<Record<string, Record<string, unknown>>>({})

  /* ── Table metadata cache ──────────────────── */
  const [tableMetaCache, setTableMetaCache] = useState<Record<string, { indexes: TableIndex[]; foreignKeys: TableForeignKey[] }>>({})

  /* ── Info panel tab ────────────────────────── */
  const [infoPanelTab, setInfoPanelTab] = useState<'info' | 'ddl' | 'columns' | 'indexes' | 'foreignKeys'>('info')

  /* ── Panel visibility ────────────────────── */
  const [showSidebar, setShowSidebar] = useState(true)
  const [showInfoPanel, setShowInfoPanel] = useState(true)

  /* ── Grid row selection ────────────────────── */
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set())
  const lastClickedRow = useRef<number>(-1)

  /* ── Cell editing state ───────────────────── */
  const [editingCell, setEditingCell] = useState<{ row: number; col: string } | null>(null)
  const [pendingChanges, setPendingChanges] = useState<Record<string, Record<string, unknown>>>({})
  // key = `row_${index}`, value = { colName: newValue }
  const [newRows, setNewRows] = useState<Record<string, unknown>[]>([])
  const [deletedRows, setDeletedRows] = useState<Set<number>>(new Set())

  /* ── Sort & filter state ─────────────────── */
  const [orderBy, setOrderBy] = useState<{ col: string; dir: 'ASC' | 'DESC' } | null>(null)
  const [filterWhere, setFilterWhere] = useState<string>('')
  const [colSortMenu, setColSortMenu] = useState<string | null>(null)
  const [filterOpen, setFilterOpen] = useState(false)
  const [filterRules, setFilterRules] = useState<{
    id: string; enabled: boolean; col: string; op: string;
    value: string | null; connector: 'and' | 'or';
    isGroup?: boolean; children?: { id: string; enabled: boolean; col: string; op: string; value: string | null; connector: 'and' | 'or' }[]
  }[]>([])
  const [filterValuePicker, setFilterValuePicker] = useState<{
    ruleId: string; groupId?: string; values: string[]; selected: Set<string>; search: string; loading: boolean
  } | null>(null)
  const pickerInputRef = useRef<HTMLInputElement>(null)

  /* ── CLI state ─────────────────────────────── */
  const cliInputRef = useRef<HTMLInputElement>(null)
  const cliScrollRef = useRef<HTMLDivElement>(null)
  const [cliInput, setCliInput] = useState('')
  const [cliHistoryIdx, setCliHistoryIdx] = useState(-1)

  /* ── Modal state ────────────────────────────── */
  const [showModal, setShowModal] = useState(false)
  const [draft, setDraft] = useState<ConnectionDraft>(blankDraft)
  const [modalBusy, setModalBusy] = useState(false)
  const [modalTab, setModalTab] = useState<'general' | 'ssh'>('general')

  /* ── Notice ─────────────────────────────────── */
  const [notice, setNotice] = useState<{ tone: 'success' | 'error'; message: string } | null>(null)
  const noticeTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)

  const tabsRef = useRef(tabs)
  const liveConnectionsRef = useRef(liveConnections)
  const connectingProfilesRef = useRef(connectingProfiles)

  useEffect(() => {
    tabsRef.current = tabs
  }, [tabs])

  useEffect(() => {
    liveConnectionsRef.current = liveConnections
  }, [liveConnections])

  useEffect(() => {
    connectingProfilesRef.current = connectingProfiles
  }, [connectingProfiles])

  const flash = useCallback((tone: 'success' | 'error', message: string) => {
    setNotice({ tone, message })
    clearTimeout(noticeTimer.current)
    noticeTimer.current = setTimeout(() => setNotice(null), 4000)
  }, [])

  /* ── NCX Import/Export modal ─────────────────── */
  const ncxFileInputRef = useRef<HTMLInputElement>(null)

  /* ── NCX Import/Export handlers ─────────────── */

  /** Decrypt Navicat 12+ encrypted password (AES-128-CBC, public key/IV) */
  async function decryptNavicatPassword(hexStr: string): Promise<string> {
    if (!hexStr) return ''
    try {
      const keyBytes = new TextEncoder().encode('libcckeylibcckey')
      const ivBytes = new TextEncoder().encode('libcciv libcciv ')
      const encrypted = new Uint8Array(hexStr.match(/.{2}/g)!.map(b => parseInt(b, 16)))
      const cryptoKey = await crypto.subtle.importKey('raw', keyBytes, { name: 'AES-CBC' }, false, ['decrypt'])
      const decrypted = await crypto.subtle.decrypt({ name: 'AES-CBC', iv: ivBytes }, cryptoKey, encrypted)
      return new TextDecoder().decode(decrypted)
    } catch {
      return '' // Decryption failed (possibly Navicat 11 or corrupted)
    }
  }

  async function parseNcxXml(xml: string): Promise<ConnectionProfile[]> {
    const results: ConnectionProfile[] = []
    const regex = /<Connection\s+([^>]+)\/>/g
    let match: RegExpExecArray | null
    while ((match = regex.exec(xml)) !== null) {
      const attrs: Record<string, string> = {}
      const attrRegex = /(\w+)="([^"]*)"/g
      let attrMatch: RegExpExecArray | null
      while ((attrMatch = attrRegex.exec(match[1])) !== null) {
        attrs[attrMatch[1]] = attrMatch[2]
      }
      if (attrs.ConnType !== 'MYSQL') continue

      // Decrypt passwords
      const password = await decryptNavicatPassword(attrs.Password || '')
      const sshPassword = await decryptNavicatPassword(attrs.SSH_Password || '')
      const sshPassphrase = await decryptNavicatPassword(attrs.SSH_Passphrase || '')

      results.push({
        id: crypto.randomUUID(),
        label: attrs.ConnectionName || 'Imported',
        host: attrs.Host || 'localhost',
        port: parseInt(attrs.Port || '3306', 10),
        user: attrs.UserName || 'root',
        password,
        useSSH: attrs.SSH === 'true',
        sshHost: attrs.SSH_Host || undefined,
        sshPort: attrs.SSH_Port ? parseInt(attrs.SSH_Port, 10) : undefined,
        sshUser: attrs.SSH_UserName || undefined,
        sshAuthMethod: attrs.SSH_AuthenMethod === 'PASSWORD' ? 'password' : attrs.SSH_AuthenMethod === 'PUBLICKEY' ? 'privateKey' : undefined,
        sshPrivateKey: attrs.SSH_PrivateKey || undefined,
        sshPassphrase,
        sshPassword,
      })
    }
    return results
  }

  function handleImportNcxFile(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = () => {
      const xml = reader.result as string
      void (async () => {
        try {
          const imported = await parseNcxXml(xml)
          if (imported.length === 0) {
            flash('error', '未找到 MySQL 连接')
            return
          }
          setProfiles(prev => {
            const existing = new Set(prev.map(p => `${p.label}|${p.host}|${p.port}`))
            const newProfiles = imported.filter(p => !existing.has(`${p.label}|${p.host}|${p.port}`))
            return [...prev, ...newProfiles]
          })
          flash('success', `成功导入 ${imported.length} 个连接（含密码解密）`)
        } catch (err) {
          flash('error', `导入失败: ${err instanceof Error ? err.message : String(err)}`)
        }
      })()
    }
    reader.onerror = () => flash('error', '文件读取失败')
    reader.readAsText(file)
    // Reset input so the same file can be re-selected
    e.target.value = ''
  }

  /** Encrypt password to Navicat 12+ format (AES-128-CBC, public key/IV) */
  async function encryptNavicatPassword(plaintext: string): Promise<string> {
    if (!plaintext) return ''
    try {
      const keyBytes = new TextEncoder().encode('libcckeylibcckey')
      const ivBytes = new TextEncoder().encode('libcciv libcciv ')
      const data = new TextEncoder().encode(plaintext)
      const cryptoKey = await crypto.subtle.importKey('raw', keyBytes, { name: 'AES-CBC' }, false, ['encrypt'])
      const encrypted = await crypto.subtle.encrypt({ name: 'AES-CBC', iv: ivBytes }, cryptoKey, data)
      return Array.from(new Uint8Array(encrypted)).map(b => b.toString(16).padStart(2, '0').toUpperCase()).join('')
    } catch {
      return ''
    }
  }

  function handleExportNcx() {
    if (profiles.length === 0) { flash('error', '没有连接可导出'); return }

    void (async () => {
      try {
        const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        const lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<Connections Ver="1.5">']
        for (const p of profiles) {
          const encPassword = await encryptNavicatPassword(p.password)
          const encSshPassword = await encryptNavicatPassword(p.sshPassword ?? '')
          const encSshPassphrase = await encryptNavicatPassword(p.sshPassphrase ?? '')

          const attrs: string[] = []
          attrs.push(`ConnectionName="${esc(p.label)}"`)
          attrs.push('ConnType="MYSQL"')
          attrs.push(`Host="${esc(p.host)}"`)
          attrs.push(`Port="${p.port}"`)
          attrs.push(`UserName="${esc(p.user)}"`)
          attrs.push(`Password="${encPassword}"`)
          attrs.push('SavePassword="true"')
          attrs.push('SSL="false"')
          attrs.push(`SSH="${p.useSSH}"`)
          if (p.useSSH) {
            attrs.push(`SSH_Host="${esc(p.sshHost ?? '')}"`)
            attrs.push(`SSH_Port="${p.sshPort ?? 22}"`)
            attrs.push(`SSH_UserName="${esc(p.sshUser ?? '')}"`)
            const method = p.sshAuthMethod === 'password' ? 'PASSWORD' : 'PUBLICKEY'
            attrs.push(`SSH_AuthenMethod="${method}"`)
            attrs.push(`SSH_Password="${encSshPassword}"`)
            attrs.push('SSH_SavePassword="true"')
            attrs.push(`SSH_PrivateKey="${esc(p.sshPrivateKey ?? '')}"`)
            attrs.push(`SSH_Passphrase="${encSshPassphrase}"`)
            attrs.push('SSH_SavePassphrase="true"')
          }
          attrs.push('HTTP="false"')
          lines.push(`\t<Connection ${attrs.join(' ')}/>`)
        }
        lines.push('</Connections>')
        lines.push('')

        const blob = new Blob([lines.join('\n')], { type: 'application/xml' })

        // Try native save dialog first, fallback to download link
        const pickerWindow = window as SavePickerWindow
        if (pickerWindow.showSaveFilePicker) {
          const handle = await pickerWindow.showSaveFilePicker({
            suggestedName: 'connections.ncx',
            types: [{ description: 'Navicat Connection Export', accept: { 'application/xml': ['.ncx'] } }],
          })
          const writable = await handle.createWritable()
          await writable.write(blob)
          await writable.close()
          flash('success', `成功导出 ${profiles.length} 个连接`)
        } else {
          // Fallback: trigger download
          const url = URL.createObjectURL(blob)
          const a = document.createElement('a')
          a.href = url
          a.download = 'connections.ncx'
          a.click()
          URL.revokeObjectURL(url)
          flash('success', `成功导出 ${profiles.length} 个连接`)
        }
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') return
        flash('error', err instanceof Error ? err.message : '导出失败')
      }
    })()
  }

  /* ── Context menu ───────────────────────────── */
  type ContextMenuItem = { icon: string; label: string; shortcut?: string; action: () => void; danger?: boolean; disabled?: boolean; children?: ContextMenuItem[] } | 'separator'
  const [ctxMenu, setCtxMenu] = useState<{ x: number; y: number; items: ContextMenuItem[] } | null>(null)

  const ctxMenuRef = useCallback((node: HTMLDivElement | null) => {
    if (!node || !ctxMenu) return
    const rect = node.getBoundingClientRect()
    const pad = 8
    let { x, y } = ctxMenu
    if (rect.right > window.innerWidth - pad) x = window.innerWidth - rect.width - pad
    if (rect.bottom > window.innerHeight - pad) y = window.innerHeight - rect.height - pad
    if (x < pad) x = pad
    if (y < pad) y = pad
    if (x !== ctxMenu.x || y !== ctxMenu.y) {
      setCtxMenu({ ...ctxMenu, x, y })
    }
  }, [ctxMenu])

  function showContextMenu(e: React.MouseEvent, items: ContextMenuItem[]) {
    e.preventDefault()
    e.stopPropagation()
    setCtxMenu({ x: e.clientX, y: e.clientY, items })
  }

  function closeContextMenu() { setCtxMenu(null); setColSortMenu(null) }

  function connContextMenu(e: React.MouseEvent, profile: ConnectionProfile) {
    const isLive = liveConnections.has(profile.id)
    const isConnecting = connectingProfiles.has(profile.id)
    showContextMenu(e, [
      { icon: '🔗', label: isConnecting ? '正在连接...' : '打开连接', action: () => { closeContextMenu(); if (isLive) { setExpandedNodes(prev => { const s = new Set(prev); s.add(`conn:${profile.id}`); return s }) } else { void handleConnect(profile) } }, disabled: isConnecting },
      { icon: '⛔', label: '断开连接', action: () => { closeContextMenu(); void handleDisconnect(profile.id) }, disabled: !isLive },
      'separator',
      { icon: '✏️', label: '编辑连接...', action: () => { closeContextMenu(); openEditConnectionModal(profile) } },
      { icon: '➕', label: '新建连接', action: () => { closeContextMenu(); openNewConnectionModal() } },
      { icon: '🗑️', label: '删除连接', action: () => { closeContextMenu(); handleDeleteProfile(profile.id) }, danger: true },
      'separator',
      { icon: '📝', label: '新建查询', shortcut: '⌘Y', action: () => {
        closeContextMenu()
        if (isLive) {
          const conn = liveConnections.get(profile.id)!
          const schemaName = conn.schemas[0]?.name ?? ''
          if (schemaName) openQueryTab(profile.id, schemaName)
        }
      }, disabled: !isLive },
      { icon: '💻', label: '命令列界面', action: () => {
        closeContextMenu()
        if (isLive) {
          const conn = liveConnections.get(profile.id)!
          const schemaName = conn.schemas[0]?.name ?? ''
          if (schemaName) openCliTab(profile.id, schemaName)
        }
      }, disabled: !isLive },
      'separator',
      { icon: '🔄', label: isConnecting ? '连接中...' : '刷新', shortcut: '⌘R', action: () => { closeContextMenu(); if (isLive) void handleConnect(profile) }, disabled: isConnecting || !isLive },
    ])
  }

  /* ── Import Wizard state ──────────────────── */
  const [importWizard, setImportWizard] = useState<{
    connectionId: string;
    schemaName?: string;
    tableName?: string;
  } | null>(null)

  function openImportWizard(connectionId: string, schemaName?: string, tableName?: string) {
    setImportWizard({ connectionId, schemaName, tableName })
  }

  /* ── Export Wizard state ──────────────────── */
  const [exportWizard, setExportWizard] = useState<{
    connectionId: string;
    schemaName?: string;
    tableName?: string;
  } | null>(null)

  function openExportWizard(connectionId: string, schemaName?: string, tableName?: string) {
    setExportWizard({ connectionId, schemaName, tableName })
  }

  function dbContextMenu(e: React.MouseEvent, connectionId: string, schemaName: string) {
    showContextMenu(e, [
      { icon: '📂', label: '打开数据库', action: () => { closeContextMenu(); openObjectsTab(connectionId, schemaName) } },
      'separator',
      { icon: '📝', label: '新建查询', shortcut: '⌘Y', action: () => { closeContextMenu(); openQueryTab(connectionId, schemaName) } },
      { icon: '💻', label: '命令列界面', action: () => { closeContextMenu(); openCliTab(connectionId, schemaName) } },
      { icon: '📊', label: '新建表...', action: () => {
        closeContextMenu()
        const tableName = prompt('输入新表名:')
        if (!tableName?.trim()) return
        const tab: DesignTab = {
          id: crypto.randomUUID(),
          kind: 'design',
          title: `设计: ${tableName.trim()}`,
          connectionId,
          schemaName,
          tableName: tableName.trim(),
          mode: 'create',
        }
        setTabs(prev => [...prev, tab])
        setActiveTabId(tab.id)
      } },
      { icon: '📄', label: '运行 SQL 文件...', action: () => {
        closeContextMenu()
        const conn = liveConnections.get(connectionId)
        if (!conn) return
        const input = document.createElement('input')
        input.type = 'file'
        input.accept = '.sql'
        input.onchange = async () => {
          const file = input.files?.[0]
          if (!file) return
          const sql = await file.text()
          try {
            await api.runQuery(conn.profile, sql, schemaName)
            flash('success', `已执行 SQL 文件: ${file.name}`)
          } catch (err) {
            flash('error', `执行失败: ${err instanceof Error ? err.message : String(err)}`)
          }
        }
        input.click()
      } },
      'separator',
      { icon: '📥', label: '导入向导...', action: () => {
        closeContextMenu()
        openImportWizard(connectionId, schemaName)
      } },
      { icon: '📤', label: '导出向导...', action: () => {
        closeContextMenu()
        openExportWizard(connectionId, schemaName)
      } },
      'separator',
      { icon: '📥', label: '转储结构...', action: () => {
        closeContextMenu()
        void dumpSchema(connectionId, schemaName, 'structure')
      } },
      { icon: '📦', label: '转储结构和数据...', action: () => {
        closeContextMenu()
        void dumpSchema(connectionId, schemaName, 'both')
      } },
      'separator',
      { icon: '🔄', label: '刷新', action: () => {
        closeContextMenu()
        const conn = liveConnections.get(connectionId)
        if (conn) void handleConnect(conn.profile)
      } },
    ])
  }

  async function dumpSchema(connectionId: string, schemaName: string, mode: 'structure' | 'both') {
    const conn = liveConnections.get(connectionId)
    if (!conn) return

    try {
      flash('success', `正在导出 ${schemaName}...`)
      const schema = conn.schemas.find(s => s.name === schemaName)
      if (!schema || schema.tables.length === 0) {
        flash('error', '数据库中没有表')
        return
      }

      // Build SQL first
      let sql = `-- Dump of database: ${schemaName}\n-- Date: ${new Date().toISOString()}\n-- Mode: ${mode}\n\n`
      for (const table of schema.tables) {
        console.log(`[dump] Processing table: ${table.name}`)
        try {
          const ddlResult = await api.runQuery(conn.profile, `SHOW CREATE TABLE ${q(schemaName)}.${q(table.name)}`, schemaName)
          const ddlRow = ddlResult.results?.[0]
          if (ddlRow?.kind === 'rows' && ddlRow.rows.length > 0) {
            const createSql = String(Object.values(ddlRow.rows[0])[1] ?? '')
            sql += `-- Table structure for ${table.name}\nDROP TABLE IF EXISTS ${q(table.name)};\n${createSql};\n\n`
          }
        } catch (ddlErr) {
          console.error(`[dump] DDL error for ${table.name}:`, ddlErr)
          sql += `-- ERROR getting DDL for ${table.name}: ${ddlErr instanceof Error ? ddlErr.message : String(ddlErr)}\n\n`
        }

        if (mode === 'both') {
          const BATCH = 5000
          const MAX_ROWS = 500000
          let offset = 0
          let hasMore = true
          let rowCount = 0
          while (hasMore) {
            try {
              const batchSql = `SELECT * FROM ${q(schemaName)}.${q(table.name)} LIMIT ${BATCH} OFFSET ${offset}`
              const dataResult = await api.runQuery(conn.profile, batchSql, schemaName)
              const dataRows = dataResult.results?.[0]
              if (dataRows?.kind === 'rows' && dataRows.rows.length > 0) {
                const cols = dataRows.columns
                for (const row of dataRows.rows) {
                  const vals = cols.map(c => {
                    const v = row[c]
                    if (v === null || v === undefined) return 'NULL'
                    return `'${String(v).replace(/'/g, "''")}'`
                  })
                  sql += `INSERT INTO ${q(table.name)} (${cols.map(c => q(c)).join(', ')}) VALUES (${vals.join(', ')});\n`
                  rowCount++
                }
                offset += dataRows.rows.length
                if (dataRows.rows.length < BATCH) hasMore = false
                // Progress feedback
                if (rowCount % 10000 === 0) {
                  flash('success', `正在导出 ${table.name}... ${rowCount} 行`)
                }
                // Safety limit
                if (rowCount >= MAX_ROWS) {
                  sql += `-- WARNING: reached ${MAX_ROWS} row limit for ${table.name}\n`
                  hasMore = false
                }
              } else {
                hasMore = false
              }
            } catch (dataErr) {
              console.error(`[dump] Data error for ${table.name}:`, dataErr)
              sql += `-- ERROR fetching data for ${table.name}: ${dataErr instanceof Error ? dataErr.message : String(dataErr)}\n`
              hasMore = false
            }
          }
          console.log(`[dump] ${table.name}: ${rowCount} rows exported`)
          sql += '\n'
        }
      }

      console.log(`[dump] SQL length: ${sql.length} chars`)

      // Now ask user for save location
      const defaultName = `${schemaName}.sql`
      let fileHandle: FileSystemFileHandle | null = null
      const pickerWindow = window as SavePickerWindow
      try {
        if (pickerWindow.showSaveFilePicker) {
          fileHandle = await pickerWindow.showSaveFilePicker({
            suggestedName: defaultName,
            types: [{
              description: 'SQL Files',
              accept: { 'text/sql': ['.sql'] },
            }],
          })
        }
      } catch (e: unknown) {
        if (e instanceof DOMException && e.name === 'AbortError') return
        // fallback to auto download
      }

      // Save file
      if (fileHandle) {
        const writable = await fileHandle.createWritable()
        await writable.write(sql)
        await writable.close()
        flash('success', `已导出 ${schemaName} (${sql.length} 字符)`)
      } else {
        const blob = new Blob([sql], { type: 'text/sql' })
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = defaultName
        a.click()
        URL.revokeObjectURL(url)
        flash('success', `已导出 ${schemaName} (${sql.length} 字符)`)
      }
    } catch (err) {
      console.error('[dump] Fatal error:', err)
      flash('error', `导出失败: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  async function dumpTable(connectionId: string, schemaName: string, tableName: string, mode: 'structure' | 'both') {
    const conn = liveConnections.get(connectionId)
    if (!conn) return

    try {
      flash('success', `正在导出 ${tableName}...`)
      let sql = `-- Dump of table: ${tableName}\n-- Database: ${schemaName}\n-- Date: ${new Date().toISOString()}\n-- Mode: ${mode}\n\n`

      // DDL
      try {
        const ddlResult = await api.runQuery(conn.profile, `SHOW CREATE TABLE ${q(schemaName)}.${q(tableName)}`, schemaName)
        const ddlRow = ddlResult.results?.[0]
        if (ddlRow?.kind === 'rows' && ddlRow.rows.length > 0) {
          const createSql = String(Object.values(ddlRow.rows[0])[1] ?? '')
          sql += `DROP TABLE IF EXISTS ${q(tableName)};\n${createSql};\n\n`
        }
      } catch (ddlErr) {
        sql += `-- ERROR getting DDL for ${tableName}: ${ddlErr instanceof Error ? ddlErr.message : String(ddlErr)}\n\n`
      }

      // Data
      if (mode === 'both') {
        const BATCH = 5000
        const MAX_ROWS = 500000
        let offset = 0
        let hasMore = true
        let rowCount = 0
        while (hasMore) {
          try {
            const batchSql = `SELECT * FROM ${q(schemaName)}.${q(tableName)} LIMIT ${BATCH} OFFSET ${offset}`
            const dataResult = await api.runQuery(conn.profile, batchSql, schemaName)
            const dataRows = dataResult.results?.[0]
            if (dataRows?.kind === 'rows' && dataRows.rows.length > 0) {
              const cols = dataRows.columns
              for (const row of dataRows.rows) {
                const vals = cols.map(c => {
                  const v = row[c]
                  if (v === null || v === undefined) return 'NULL'
                  return `'${String(v).replace(/'/g, "''")}'`
                })
                sql += `INSERT INTO ${q(tableName)} (${cols.map(c => q(c)).join(', ')}) VALUES (${vals.join(', ')});\n`
                rowCount++
              }
              offset += dataRows.rows.length
              if (dataRows.rows.length < BATCH) hasMore = false
              if (rowCount % 10000 === 0) flash('success', `正在导出 ${tableName}... ${rowCount} 行`)
              if (rowCount >= MAX_ROWS) {
                sql += `-- WARNING: reached ${MAX_ROWS} row limit for ${tableName}\n`
                hasMore = false
              }
            } else { hasMore = false }
          } catch (dataErr) {
            sql += `-- ERROR fetching data for ${tableName}: ${dataErr instanceof Error ? dataErr.message : String(dataErr)}\n`
            hasMore = false
          }
        }
        sql += '\n'
      }

      // Save
      const defaultName = `${tableName}.sql`
      let fileHandle: FileSystemFileHandle | null = null
      const pickerWindow = window as SavePickerWindow
      try {
        if (pickerWindow.showSaveFilePicker) {
          fileHandle = await pickerWindow.showSaveFilePicker({
            suggestedName: defaultName,
            types: [{ description: 'SQL Files', accept: { 'text/sql': ['.sql'] } }],
          })
        }
      } catch (e: unknown) {
        if (e instanceof DOMException && e.name === 'AbortError') return
      }

      if (fileHandle) {
        const writable = await fileHandle.createWritable()
        await writable.write(sql)
        await writable.close()
        flash('success', `已导出 ${tableName} (${sql.length} 字符)`)
      } else {
        const blob = new Blob([sql], { type: 'text/sql' })
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = defaultName
        a.click()
        URL.revokeObjectURL(url)
        flash('success', `已导出 ${tableName} (${sql.length} 字符)`)
      }
    } catch (err) {
      flash('error', `导出失败: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  function tableContextMenu(e: React.MouseEvent, connectionId: string, schemaName: string, tableName: string) {
    showContextMenu(e, [
      { icon: '📊', label: '打开表', action: () => { closeContextMenu(); void openDataTab(connectionId, schemaName, tableName) } },
      { icon: '📋', label: '查看对象列表', action: () => { closeContextMenu(); openObjectsTab(connectionId, schemaName) } },
      'separator',
      { icon: '📝', label: '新建查询', shortcut: '⌘Y', action: () => { closeContextMenu(); openQueryTab(connectionId, schemaName) } },
      'separator',
      { icon: '📄', label: '查看 DDL', action: () => {
        closeContextMenu()
        const conn = liveConnections.get(connectionId)
        if (conn) {
          const sql = `SHOW CREATE TABLE ${q(schemaName)}.${q(tableName)};`
          const tab: QueryTab = {
            id: crypto.randomUUID(), kind: 'query', title: `DDL: ${tableName}`,
            connectionId, schemaName, sql, results: [], activeResultIndex: 0, durationMs: 0, loading: false,
          }
          setTabs(prev => [...prev, tab])
          setActiveTabId(tab.id)
          setTimeout(() => void runQuery(tab.id), 100)
        }
      } },
      { icon: '🔧', label: '修改表结构...', action: () => {
        closeContextMenu()
        const tab: DesignTab = {
          id: crypto.randomUUID(),
          kind: 'design',
          title: `设计: ${tableName}`,
          connectionId,
          schemaName,
          tableName,
          mode: 'alter',
        }
        setTabs(prev => [...prev, tab])
        setActiveTabId(tab.id)
      } },
      'separator',
      { icon: '🗑️', label: '删除表', danger: true, action: () => {
        closeContextMenu()
        if (!confirm(`确定要删除表 ${tableName} 吗？此操作不可恢复！`)) return
        const conn = liveConnections.get(connectionId)
        if (!conn) return
        void (async () => {
          try {
            await api.runQuery(conn.profile, `DROP TABLE ${q(schemaName)}.${q(tableName)}`, schemaName)
            flash('success', `已删除表 ${tableName}`)
            void handleConnect(conn.profile) // refresh tree
          } catch (err) {
            flash('error', `删除失败: ${err instanceof Error ? err.message : String(err)}`)
          }
        })()
      } },
      { icon: '🧹', label: '清空表', danger: true, action: () => {
        closeContextMenu()
        if (!confirm(`确定要清空表 ${tableName} 的所有数据吗？`)) return
        const conn = liveConnections.get(connectionId)
        if (!conn) return
        void (async () => {
          try {
            await api.runQuery(conn.profile, `TRUNCATE TABLE ${q(schemaName)}.${q(tableName)}`, schemaName)
            flash('success', `已清空表 ${tableName}`)
          } catch (err) {
            flash('error', `清空失败: ${err instanceof Error ? err.message : String(err)}`)
          }
        })()
      } },
      'separator',
      { icon: '📋', label: '复制表', action: () => {}, children: [
        { icon: '📦', label: '结构和数据', action: () => { closeContextMenu(); void copyTable(connectionId, schemaName, tableName, 'both') } },
        { icon: '📐', label: '仅结构', action: () => { closeContextMenu(); void copyTable(connectionId, schemaName, tableName, 'structure') } },
      ] },
      { icon: '💾', label: '转储 SQL 文件', action: () => {}, children: [
        { icon: '📦', label: '结构和数据...', action: () => { closeContextMenu(); void dumpTable(connectionId, schemaName, tableName, 'both') } },
        { icon: '📐', label: '仅结构...', action: () => { closeContextMenu(); void dumpTable(connectionId, schemaName, tableName, 'structure') } },
      ] },
      'separator',
      { icon: '📥', label: '导入数据...', action: () => {
        closeContextMenu()
        openImportWizard(connectionId, schemaName, tableName)
      } },
      { icon: '📤', label: '导出数据...', action: () => {
        closeContextMenu()
        openExportWizard(connectionId, schemaName, tableName)
      } },
    ])
  }

  async function copyTable(connectionId: string, schemaName: string, tableName: string, mode: 'structure' | 'both') {
    const conn = liveConnections.get(connectionId)
    if (!conn) return

    try {
      // Find next available copy name
      const schema = conn.schemas.find(s => s.name === schemaName)
      const existingNames = new Set(schema?.tables.map(t => t.name) ?? [])
      let copyNum = 1
      while (existingNames.has(`${tableName}_copy${copyNum}`)) copyNum++
      const newName = `${tableName}_copy${copyNum}`

      flash('success', `正在复制表 ${tableName} → ${newName}...`)

      // Create structure
      await api.runQuery(conn.profile, `CREATE TABLE ${q(schemaName)}.${q(newName)} LIKE ${q(schemaName)}.${q(tableName)}`, schemaName)

      // Copy data if needed
      if (mode === 'both') {
        await api.runQuery(conn.profile, `INSERT INTO ${q(schemaName)}.${q(newName)} SELECT * FROM ${q(schemaName)}.${q(tableName)}`, schemaName)
      }

      flash('success', `已复制表 ${tableName} → ${newName}`)
      void handleConnect(conn.profile) // refresh tree
    } catch (err) {
      flash('error', `复制失败: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  /* ═══════════════════════════════════════════════
     Connection actions
     ═══════════════════════════════════════════════ */

  async function handleConnect(profile: ConnectionProfile) {
    if (connectingProfilesRef.current.has(profile.id)) return

    setConnectingProfiles(prev => {
      const next = new Set(prev)
      next.add(profile.id)
      return next
    })

    try {
      const health = await api.testConnection(profile)
      const tree = await api.fetchSchemaTree(profile)
      startTransition(() => {
        setLiveConnections(prev => {
          const next = new Map(prev)
          next.set(profile.id, { profile, schemas: tree.schemas, version: health.version })
          return next
        })
        setExpandedNodes(prev => {
          const next = new Set(prev)
          next.add(`conn:${profile.id}`)
          if (tree.schemas.length > 0) next.add(`db:${profile.id}:${tree.schemas[0].name}`)
          return next
        })
      })
      flash('success', `已连接 ${profile.label} — MySQL ${health.version}`)
    } catch (err) {
      flash('error', err instanceof Error ? err.message : '连接失败')
    } finally {
      setConnectingProfiles(prev => {
        if (!prev.has(profile.id)) return prev
        const next = new Set(prev)
        next.delete(profile.id)
        return next
      })
    }
  }

  async function handleDisconnect(profileId: string) {
    const profile =
      liveConnections.get(profileId)?.profile ??
      profiles.find((candidate) => candidate.id === profileId)

    if (profile) {
      try {
        await api.disconnectConnection(profile)
      } catch {
        // Frontend state should still clear even if cleanup fails.
      }
    }

    setLiveConnections(prev => {
      const next = new Map(prev)
      next.delete(profileId)
      return next
    })
    // Close tabs belonging to this connection
    setTabs(prev => prev.filter(t => t.connectionId !== profileId))
    flash('success', '已断开连接')
  }

  /* ═══════════════════════════════════════════════
     Modal actions
     ═══════════════════════════════════════════════ */

  function openNewConnectionModal() {
    setDraft(blankDraft())
    setShowModal(true)
  }

  function openEditConnectionModal(profile: ConnectionProfile) {
    setDraft(profileToDraft(profile))
    setShowModal(true)
  }

  function handleSaveProfile() {
    try {
      const profile = draftToProfile(draft)
      setProfiles(prev => {
        const idx = prev.findIndex(p => p.id === profile.id)
        if (idx === -1) return [profile, ...prev]
        return prev.map(p => p.id === profile.id ? profile : p)
      })
      setDraft(profileToDraft(profile))
      flash('success', `已保存 "${profile.label}"`)
    } catch (err) {
      flash('error', err instanceof Error ? err.message : '保存失败')
    }
  }

  async function handleTestConnection() {
    try {
      setModalBusy(true)
      const profile = draftToProfile(draft)
      const result = await api.testConnection(profile)
      flash('success', `连接成功！MySQL ${result.version}`)
    } catch (err) {
      flash('error', err instanceof Error ? err.message : '测试失败')
    } finally {
      setModalBusy(false)
    }
  }

  async function handleSaveAndConnect() {
    try {
      const profile = draftToProfile(draft)
      setProfiles(prev => {
        const idx = prev.findIndex(p => p.id === profile.id)
        if (idx === -1) return [profile, ...prev]
        return prev.map(p => p.id === profile.id ? profile : p)
      })
      setDraft(profileToDraft(profile))
      setShowModal(false)
      await handleConnect(profile)
    } catch (err) {
      flash('error', err instanceof Error ? err.message : '连接失败')
    }
  }

  function handleDeleteProfile(id: string) {
    setProfiles(prev => prev.filter(p => p.id !== id))
    void handleDisconnect(id)
    if (draft.id === id) setDraft(blankDraft())
  }

  /* ═══════════════════════════════════════════════
     Tree actions
     ═══════════════════════════════════════════════ */

  function toggleNode(key: string) {
    setExpandedNodes(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  /* ═══════════════════════════════════════════════
     Tab actions
     ═══════════════════════════════════════════════ */

  function openObjectsTab(connectionId: string, schemaName: string) {
    const existingTab = tabs.find(t => t.kind === 'objects' && t.connectionId === connectionId && t.schemaName === schemaName)
    if (existingTab) {
      setActiveTabId(existingTab.id)
      return
    }
    const tab: AppTab = {
      id: crypto.randomUUID(),
      kind: 'objects',
      title: `对象`,
      connectionId,
      schemaName,
      tableFilter: '',
    }
    setTabs(prev => [...prev, tab])
    setActiveTabId(tab.id)
  }

  async function openDataTab(connectionId: string, schemaName: string, tableName: string) {
    const existingTab = tabs.find(t => t.kind === 'data' && t.connectionId === connectionId && t.schemaName === schemaName && (t as DataTab).tableName === tableName)
    if (existingTab) {
      setActiveTabId(existingTab.id)
      return
    }
    const conn = liveConnections.get(connectionId)
    if (!conn) return

    const tabId = crypto.randomUUID()
    const tab: DataTab = {
      id: tabId,
      kind: 'data',
      title: `${tableName}@${schemaName}`,
      connectionId,
      schemaName,
      tableName,
      columns: [],
      rows: [],
      page: 1,
      pageCursors: [null],
      pagingMode: 'offset',
      totalRows: 0,
      hasMore: false,
      rowCountExact: false,
      loading: true,
    }
    // Reset filter state for the new tab
    setFilterWhere('')
    setFilterRules([])
    setFilterOpen(false)
    setFilterValuePicker(null)

    setTabs(prev => [...prev, tab])
    setActiveTabId(tabId)

    // Update info panel to show this table's details (3 tabs)
    setSelectedNode({ kind: 'table', connectionId, schemaName, tableName })
    void ensureColumns(connectionId, schemaName, tableName)
    void fetchTableStats(conn.profile, schemaName, tableName)
    void fetchTableMetadata(conn.profile, schemaName, tableName)

    await loadDataPage(tabId, conn.profile, schemaName, tableName, 1)

    // Also fetch & cache the DDL
    void fetchDdl(conn.profile, schemaName, tableName)
  }

  async function loadDataPage(tabId: string, profile: ConnectionProfile, schemaName: string, tableName: string, page: number, whereOverride?: string, orderOverride?: { col: string; dir: 'ASC' | 'DESC' } | null) {
    updateTab(tabId, { loading: true })
    try {
      // Build WHERE clause — use override if provided (avoids stale closure state)
      const effectiveWhere = whereOverride !== undefined ? whereOverride : filterWhere
      const whereClause = effectiveWhere ? ` WHERE ${effectiveWhere}` : ''

      // Fetch page data — use override if provided (avoids stale closure)
      const effectiveOrder = orderOverride !== undefined ? orderOverride : orderBy
      const offset = (page - 1) * PAGE_SIZE
      const orderClause = effectiveOrder ? ` ORDER BY \`${effectiveOrder.col}\` ${effectiveOrder.dir}` : ''
      // Fetch one extra row to know if there's a next page
      const dataSql = `SELECT * FROM ${q(schemaName)}.${q(tableName)}${whereClause}${orderClause} LIMIT ${PAGE_SIZE + 1} OFFSET ${offset};`
      const dataResp = await api.runQuery(profile, dataSql, schemaName)
      const dataResult = dataResp.results[0]

      if (dataResult?.kind === 'rows') {
        const hasMore = dataResult.rows.length > PAGE_SIZE
        const rows = hasMore ? dataResult.rows.slice(0, PAGE_SIZE) : dataResult.rows
        updateTab<DataTab>(tabId, {
          columns: dataResult.columns,
          rows,
          page,
          hasMore,
          totalRows: offset + rows.length + (hasMore ? 1 : 0),
          rowCountExact: false,
          loading: false,
        })
      } else {
        updateTab<DataTab>(tabId, { page, hasMore: false, loading: false })
      }
    } catch (err) {
      flash('error', err instanceof Error ? err.message : '加载数据失败')
      updateTab(tabId, { loading: false })
    }
  }

  function openQueryTab(connectionId: string, schemaName: string) {
    const tab: QueryTab = {
      id: crypto.randomUUID(),
      kind: 'query',
      title: `无标题 @${schemaName}`,
      connectionId,
      schemaName,
      sql: '',
      results: [],
      activeResultIndex: 0,
      durationMs: 0,
      loading: false,
    }
    setTabs(prev => [...prev, tab])
    setActiveTabId(tab.id)
  }

  function openCliTab(connectionId: string, schemaName: string) {
    const conn = liveConnections.get(connectionId)
    const label = conn?.profile.label ?? ''
    const tab: CliTab = {
      id: crypto.randomUUID(),
      kind: 'cli',
      title: `命令列界面 - ${label}`,
      connectionId,
      schemaName,
      history: [],
      loading: false,
    }
    setTabs(prev => [...prev, tab])
    setActiveTabId(tab.id)
  }

  function closeTab(tabId: string) {
    setTabs(prev => {
      const next = prev.filter(t => t.id !== tabId)
      if (activeTabId === tabId) {
        const idx = prev.findIndex(t => t.id === tabId)
        const neighbor = prev[idx - 1] ?? prev[idx + 1]
        setActiveTabId(neighbor?.id ?? null)
      }
      return next
    })
  }

  const updateTab = useCallback(<T extends AppTab>(tabId: string, patch: Partial<T>) => {
    setTabs(prev => prev.map(t => t.id === tabId ? { ...t, ...patch } : t))
  }, [])

  const persistQuerySql = useCallback((tabId: string, sql: string) => {
    setTabs(prev => prev.map(tab =>
      tab.id === tabId && tab.kind === 'query'
        ? { ...tab, sql }
        : tab,
    ))
  }, [])

  const changeQuerySchema = useCallback((tabId: string, schemaName: string) => {
    setTabs(prev => prev.map(tab =>
      tab.id === tabId && tab.kind === 'query'
        ? { ...tab, schemaName, title: `无标题 @${schemaName}` }
        : tab,
    ))
  }, [])

  const setQueryActiveResult = useCallback((tabId: string, index: number) => {
    startTransition(() => {
      setTabs(prev => prev.map(tab =>
        tab.id === tabId && tab.kind === 'query'
          ? { ...tab, activeResultIndex: index }
          : tab,
      ))
    })
  }, [])

  const clearQueryHistory = useCallback(() => {
    setHistory([])
  }, [])



  const cancelRunningQuery = useCallback(async (tabId: string, activeQueryId?: string) => {
    if (!activeQueryId) return
    try {
      await api.cancelQuery(activeQueryId)
      updateTab<QueryTab>(tabId, { loading: false, activeQueryId: undefined })
      flash('success', '已发送停止请求')
    } catch (err) {
      flash('error', err instanceof Error ? err.message : '停止 SQL 失败')
    }
  }, [flash, updateTab])

  /* ═══════════════════════════════════════════════
     Query execution
     ═══════════════════════════════════════════════ */

  const runQuery = useCallback(async (tabId: string, overrideSql?: string) => {
    const tab = tabsRef.current.find(t => t.id === tabId) as QueryTab | undefined
    if (!tab || tab.kind !== 'query') return
    const conn = liveConnectionsRef.current.get(tab.connectionId)
    if (!conn) { flash('error', '未连接'); return }
    const sql = (overrideSql ?? tab.sql).trim()
    if (!sql) { flash('error', 'SQL 为空'); return }
    const queryId = crypto.randomUUID()

    updateTab<QueryTab>(tabId, { loading: true, activeQueryId: queryId })

    try {
      const response = await api.runQuery(conn.profile, sql, tab.schemaName || undefined, queryId)
      startTransition(() => {
        updateTab<QueryTab>(tabId, {
          results: response.results,
          activeResultIndex: 0,
          durationMs: response.durationMs,
          activeQueryId: undefined,
          loading: false,
        })
      })
      setHistory(prev => [sql, ...prev.filter(h => h !== sql)].slice(0, 50))
      flash('success', `执行完成 — ${response.results.length} 个结果集，耗时 ${response.durationMs}ms`)
    } catch (err) {
      if (isQueryCancelledMessage(err)) {
        flash('success', 'SQL 已停止')
      } else {
        flash('error', err instanceof Error ? err.message : 'SQL 执行失败')
      }
      updateTab<QueryTab>(tabId, { loading: false, activeQueryId: undefined })
    }
  }, [flash, updateTab])

  /* ═══════════════════════════════════════════════
     DDL fetch
     ═══════════════════════════════════════════════ */

  async function fetchDdl(profile: ConnectionProfile, schemaName: string, tableName: string) {
    const key = `${profile.id}:${schemaName}.${tableName}`
    if (ddlCache[key]) return
    try {
      const sql = `SHOW CREATE TABLE ${q(schemaName)}.${q(tableName)};`
      const response = await api.runQuery(profile, sql)
      const result = response.results[0]
      if (result?.kind === 'rows' && result.rows.length > 0) {
        const ddl = String(result.rows[0]['Create Table'] ?? result.rows[0]['Create View'] ?? '')
        setDdlCache(prev => ({ ...prev, [key]: ddl }))
      }
    } catch {
      // ignore
    }
  }

  /* ═══════════════════════════════════════════════
     Table stats fetch (INFORMATION_SCHEMA)
     ═══════════════════════════════════════════════ */

  async function fetchTableStats(profile: ConnectionProfile, schemaName: string, tableName: string) {
    const key = `${profile.id}:${schemaName}.${tableName}`
    if (tableStatsCache[key]) return
    try {
      const sql = `SELECT TABLE_TYPE, ENGINE, ROW_FORMAT, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, AUTO_INCREMENT, CREATE_TIME, UPDATE_TIME, CHECK_TIME, TABLE_COLLATION, TABLE_COMMENT, CREATE_OPTIONS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_NAME = '${tableName}';`
      const response = await api.runQuery(profile, sql)
      const result = response.results[0]
      if (result?.kind === 'rows' && result.rows.length > 0) {
        setTableStatsCache(prev => ({ ...prev, [key]: result.rows[0] }))
      }
    } catch {
      // ignore
    }
  }

  async function fetchTableMetadata(profile: ConnectionProfile, schemaName: string, tableName: string) {
    const key = `${profile.id}:${schemaName}.${tableName}`
    if (tableMetaCache[key]) return
    try {
      const metadata = await api.fetchTableMetadata(profile, schemaName, tableName)
      setTableMetaCache(prev => ({ ...prev, [key]: metadata }))
    } catch {
      // ignore
    }
  }

  /* ═══════════════════════════════════════════════
     Column cache
     ═══════════════════════════════════════════════ */

  async function ensureColumns(connectionId: string, schemaName: string, tableName: string) {
    const key = `${connectionId}:${schemaName}.${tableName}`
    if (columnCache[key]) return
    const conn = liveConnections.get(connectionId)
    if (!conn) return
    try {
      const response = await api.fetchTableColumns(conn.profile, schemaName, tableName)
      setColumnCache(prev => ({ ...prev, [key]: response.columns }))
    } catch {
      // ignore
    }
  }

  /* ═══════════════════════════════════════════════
     Derived state
     ═══════════════════════════════════════════════ */

  const activeTab = tabs.find(t => t.id === activeTabId) ?? null
  const queryTabsPersistTimer = useRef<number | undefined>(undefined)

  useEffect(() => {
    window.clearTimeout(queryTabsPersistTimer.current)
    queryTabsPersistTimer.current = window.setTimeout(() => {
      const persistedTabs = tabs
        .filter((tab): tab is QueryTab => tab.kind === 'query')
        .map((tab) => ({
          id: tab.id,
          title: tab.title,
          connectionId: tab.connectionId,
          schemaName: tab.schemaName,
          sql: tab.sql,
        }))
      writeStorage(STORAGE_QUERY_TABS, persistedTabs)
    }, 320)

    return () => {
      window.clearTimeout(queryTabsPersistTimer.current)
    }
  }, [tabs])

  useEffect(() => {
    const queryTab = activeTab?.kind === 'query' ? activeTab.id : ''
    localStorage.setItem(STORAGE_ACTIVE_QUERY_TAB, queryTab)
  }, [activeTab])

  // Info panel data
  const infoPanelContent = useMemo(() => {
    if (!selectedNode) return null

    if (selectedNode.kind === 'connection') {
      const conn = liveConnections.get(selectedNode.connectionId)
      const profile = conn?.profile ?? profiles.find(p => p.id === selectedNode.connectionId)
      if (!profile) return null
      return {
        kind: 'connection' as const,
        profile,
        version: conn?.version ?? '',
        online: !!conn,
        connecting: connectingProfiles.has(selectedNode.connectionId),
      }
    }

    if (selectedNode.kind === 'database') {
      const conn = liveConnections.get(selectedNode.connectionId)
      if (!conn) return null
      const schema = conn.schemas.find(s => s.name === selectedNode.schemaName)
      return {
        kind: 'database' as const,
        profile: conn.profile,
        schemaName: selectedNode.schemaName,
        tableCount: schema?.tables.length ?? 0,
      }
    }

    if (selectedNode.kind === 'table') {
      const conn = liveConnections.get(selectedNode.connectionId)
      if (!conn) return null
      const ddlKey = `${selectedNode.connectionId}:${selectedNode.schemaName}.${selectedNode.tableName}`
      return {
        kind: 'table' as const,
        profile: conn.profile,
        schemaName: selectedNode.schemaName,
        tableName: selectedNode.tableName,
        ddl: ddlCache[ddlKey] ?? null,
        columns: columnCache[ddlKey] ?? null,
        metadata: tableMetaCache[ddlKey] ?? null,
      }
    }

    return null
  }, [selectedNode, liveConnections, profiles, ddlCache, columnCache, tableMetaCache, connectingProfiles])

  /* ═══════════════════════════════════════════════
     Renderers
     ═══════════════════════════════════════════════ */

  /* ── Tree ───────────────────────────────────── */
  function renderTree() {
    const filterLower = treeFilter.trim().toLowerCase()

    return (
      <>
        {profiles.map(profile => {
          const connKey = `conn:${profile.id}`
          const isExpanded = expandedNodes.has(connKey)
          const conn = liveConnections.get(profile.id)
          const isLive = !!conn
          const isConnecting = connectingProfiles.has(profile.id)
          const isNodeSelected = selectedNode?.kind === 'connection' && selectedNode.connectionId === profile.id

          return (
            <div key={profile.id}>
              <div
                className={`tree-node${isNodeSelected ? ' selected' : ''}${isConnecting ? ' connecting' : ''}`}
                style={{ paddingLeft: 4 }}
                aria-busy={isConnecting}
                onClick={() => {
                  setSelectedNode({ kind: 'connection', connectionId: profile.id })
                  toggleNode(connKey)
                }}
                onDoubleClick={() => {
                  if (isLive) {
                    setExpandedNodes(prev => { const s = new Set(prev); s.add(connKey); return s })
                  } else {
                    void handleConnect(profile)
                  }
                }}
                onContextMenu={e => connContextMenu(e, profile)}
              >
                <span className={`tree-arrow${isExpanded ? ' expanded' : ''}`}>▶</span>
                <span className="tree-icon">
                  <span className={`tree-connection-dot ${isConnecting ? 'connecting' : isLive ? 'online' : 'offline'}`} />
                </span>
                <span className="tree-label">{profile.label}</span>
                {isConnecting && <span className="tree-activity-badge">连接中...</span>}
              </div>

              {isExpanded && conn && conn.schemas
                .filter(schema => !filterLower || schema.name.toLowerCase().includes(filterLower) ||
                  schema.tables.some(t => t.name.toLowerCase().includes(filterLower)))
                .map(schema => {
                  const dbKey = `db:${profile.id}:${schema.name}`
                  const isDbExpanded = expandedNodes.has(dbKey)
                  const isDbSelected = selectedNode?.kind === 'database' && selectedNode.connectionId === profile.id && selectedNode.schemaName === schema.name

                  const filteredTables = filterLower
                    ? schema.tables.filter(t => t.name.toLowerCase().includes(filterLower) || schema.name.toLowerCase().includes(filterLower))
                    : schema.tables

                  return (
                    <div key={dbKey}>
                      <div
                        className={`tree-node${isDbSelected ? ' selected' : ''}`}
                        style={{ paddingLeft: 24 }}
                        onClick={() => {
                          setSelectedNode({ kind: 'database', connectionId: profile.id, schemaName: schema.name })
                          toggleNode(dbKey)
                        }}
                        onDoubleClick={() => {
                          openObjectsTab(profile.id, schema.name)
                        }}
                        onContextMenu={e => dbContextMenu(e, profile.id, schema.name)}
                      >
                        <span className={`tree-arrow${isDbExpanded ? ' expanded' : ''}`}>▶</span>
                        <span className="tree-icon">📦</span>
                        <span className="tree-label">{schema.name}</span>
                      </div>

                      {isDbExpanded && (
                        <div>
                          {/* Tables group */}
                          <div
                            className="tree-node"
                            style={{ paddingLeft: 44, color: '#999', fontSize: 11 }}
                          >
                            <span className="tree-icon" style={{ fontSize: 12 }}>📋</span>
                            <span className="tree-label">表 ({filteredTables.length})</span>
                          </div>
                          {filteredTables.map(table => {
                            const tblKey = `tbl:${profile.id}:${schema.name}:${table.name}`
                            const isTblSelected = selectedNode?.kind === 'table' &&
                              selectedNode.connectionId === profile.id &&
                              selectedNode.schemaName === schema.name &&
                              selectedNode.tableName === table.name

                            return (
                              <div
                                key={tblKey}
                                className={`tree-node${isTblSelected ? ' selected' : ''}`}
                                style={{ paddingLeft: 60 }}
                                onClick={() => {
                                  setSelectedNode({ kind: 'table', connectionId: profile.id, schemaName: schema.name, tableName: table.name })
                                  void ensureColumns(profile.id, schema.name, table.name)
                                  const conn = liveConnections.get(profile.id)
                                  if (conn) {
                                    void fetchDdl(conn.profile, schema.name, table.name)
                                    void fetchTableStats(conn.profile, schema.name, table.name)
                                    void fetchTableMetadata(conn.profile, schema.name, table.name)
                                  }
                                }}
                                onDoubleClick={() => {
                                  void openDataTab(profile.id, schema.name, table.name)
                                }}
                                onContextMenu={e => tableContextMenu(e, profile.id, schema.name, table.name)}
                              >
                                <span className="tree-icon" style={{ fontSize: 12 }}>📄</span>
                                <span className="tree-label">{table.name}</span>
                              </div>
                            )
                          })}
                        </div>
                      )}
                    </div>
                  )
                })}
            </div>
          )
        })}
      </>
    )
  }

  /* ── Objects tab content ────────────────────── */
  function renderObjectsTab(tab: AppTab & { kind: 'objects' }) {
    const conn = liveConnections.get(tab.connectionId)
    if (!conn) return <div className="empty-state"><span className="empty-title">未连接</span></div>
    const schema = conn.schemas.find(s => s.name === tab.schemaName)
    if (!schema) return <div className="empty-state"><span className="empty-title">数据库不存在</span></div>

    const filter = (tab.tableFilter ?? '').toLowerCase()
    const filtered = filter
      ? schema.tables.filter(t => t.name.toLowerCase().includes(filter))
      : schema.tables

    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        <div className="objects-toolbar">
          <button className="toolbar-btn" style={{ padding: '2px 6px', minWidth: 'auto' }} onClick={() => {
            openQueryTab(tab.connectionId, tab.schemaName)
          }}>
            <span className="tb-icon" style={{ fontSize: 16, width: 20, height: 20 }}>📝</span>
          </button>
          <div className="objects-search-wrap" style={{ marginLeft: 'auto' }}>
          <SearchWithHistory
            wrapperClassName="objects-search-wrap"
            className="objects-search"
            placeholder="🔍 搜索"
            value={tab.tableFilter}
            onChange={v => updateTab(tab.id, { tableFilter: v })}
            history={searchHistory.table}
            onCommit={v => setSearchHistory(prev => ({ ...prev, table: pushRecentSearch(prev.table, v) }))}
          />
          </div>
        </div>
        <div className="objects-grid">
          <table className="objects-table">
            <thead>
              <tr>
                <th>名</th>
                <th>行</th>
                <th>数据长度</th>
                <th>引擎</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(table => (
                <tr
                  key={table.name}
                  onDoubleClick={() => void openDataTab(tab.connectionId, tab.schemaName, table.name)}
                  onContextMenu={e => tableContextMenu(e, tab.connectionId, tab.schemaName, table.name)}
                  style={{ cursor: 'pointer' }}
                >
                  <td>
                    <span className="obj-icon">📄</span>
                    {table.name}
                  </td>
                  <td>{table.rows != null ? table.rows.toLocaleString() : '—'}</td>
                  <td>{table.dataLength != null ? formatBytes(table.dataLength) : '—'}</td>
                  <td>{table.engine ?? '—'}</td>
                </tr>
              ))}
              {filtered.length === 0 && (
                <tr><td colSpan={4} style={{ color: '#aaa', textAlign: 'center', padding: 20 }}>无匹配表</td></tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="data-footer">
          <span>找到 {filtered.length} 个项目</span>
        </div>
      </div>
    )
  }

  /* ── Data tab content ───────────────────────── */

  function renderDataTab(tab: DataTab) {
    if (tab.loading && tab.columns.length === 0) {
      return <div className="empty-state"><span className="empty-title">加载中...</span></div>
    }

    const rowOffset = (tab.page - 1) * PAGE_SIZE

    function goToPage(page: number, whereOverride?: string, orderOverride?: { col: string; dir: 'ASC' | 'DESC' } | null) {
      const conn = liveConnections.get(tab.connectionId)
      if (!conn) return
      setSelectedRows(new Set())
      void loadDataPage(tab.id, conn.profile, tab.schemaName, tab.tableName, page, whereOverride, orderOverride)
    }

    /** Escape SQL value */
    function sqlVal(v: unknown): string {
      if (v === null || v === undefined) return 'NULL'
      if (typeof v === 'number') return String(v)
      return `'${String(v).replace(/'/g, "''")}' `
    }

    /** Get selected row indices (sorted), or fall back to clicked row */
    function getEffectiveRows(clickedIdx: number): number[] {
      if (selectedRows.size > 0 && selectedRows.has(clickedIdx)) {
        return Array.from(selectedRows).sort((a, b) => a - b)
      }
      return [clickedIdx]
    }

    /** Generate INSERT SQL for selected rows */
    function copyAsInsert(rows: number[]) {
      const cols = tab.columns
      const lines = rows.map(i => {
        const r = tab.rows[i]
        const vals = cols.map(c => sqlVal(r[c]))
        return `INSERT INTO \`${tab.tableName}\` (${cols.map(c => `\`${c}\``).join(', ')}) VALUES (${vals.join(', ')});`
      })
      void navigator.clipboard.writeText(lines.join('\n'))
      flash('success', `已复制 ${rows.length} 条 INSERT 语句`)
    }

    /** Generate UPDATE SQL for selected rows */
    function copyAsUpdate(rows: number[]) {
      const cols = tab.columns
      const lines = rows.map(i => {
        const r = tab.rows[i]
        const sets = cols.map(c => `\`${c}\` = ${sqlVal(r[c])}`)
        // Use first column as WHERE condition (typically primary key)
        const where = `\`${cols[0]}\` = ${sqlVal(r[cols[0]])}`
        return `UPDATE \`${tab.tableName}\` SET ${sets.join(', ')} WHERE ${where};`
      })
      void navigator.clipboard.writeText(lines.join('\n'))
      flash('success', `已复制 ${rows.length} 条 UPDATE 语句`)
    }

    /** Copy as tab-separated: data only */
    function copyAsTsvData(rows: number[]) {
      const lines = rows.map(i => {
        const r = tab.rows[i]
        return tab.columns.map(c => String(r[c] ?? 'NULL')).join('\t')
      })
      void navigator.clipboard.writeText(lines.join('\n'))
      flash('success', `已复制 ${rows.length} 行数据`)
    }

    /** Copy as tab-separated: field names only */
    function copyAsTsvFields() {
      void navigator.clipboard.writeText(tab.columns.join('\t'))
      flash('success', '已复制字段名')
    }

    /** Copy as tab-separated: field names + data */
    function copyAsTsvAll(rows: number[]) {
      const header = tab.columns.join('\t')
      const lines = rows.map(i => {
        const r = tab.rows[i]
        return tab.columns.map(c => String(r[c] ?? 'NULL')).join('\t')
      })
      void navigator.clipboard.writeText([header, ...lines].join('\n'))
      flash('success', `已复制字段名 + ${rows.length} 行数据`)
    }

    /** Row click handler for selection */
    function handleRowClick(e: React.MouseEvent, rowIdx: number) {
      if (e.metaKey || e.ctrlKey) {
        // Toggle single row
        setSelectedRows(prev => {
          const next = new Set(prev)
          if (next.has(rowIdx)) next.delete(rowIdx)
          else next.add(rowIdx)
          return next
        })
        lastClickedRow.current = rowIdx
      } else if (e.shiftKey && lastClickedRow.current >= 0) {
        // Range select
        const from = Math.min(lastClickedRow.current, rowIdx)
        const to = Math.max(lastClickedRow.current, rowIdx)
        setSelectedRows(() => {
          const next = new Set<number>()
          for (let j = from; j <= to; j++) next.add(j)
          return next
        })
      } else {
        // Single select
        setSelectedRows(new Set([rowIdx]))
        lastClickedRow.current = rowIdx
      }
    }

    function cellContextMenu(e: React.MouseEvent, cellText: string, _colName: string, rowIndex: number) {
      // Auto-select the row if not already selected
      if (!selectedRows.has(rowIndex)) {
        setSelectedRows(new Set([rowIndex]))
        lastClickedRow.current = rowIndex
      }
      const rows = getEffectiveRows(rowIndex)
      showContextMenu(e, [
        { icon: '📋', label: '复制', shortcut: '⌘C', action: () => { closeContextMenu(); void navigator.clipboard.writeText(cellText) } },
        { icon: '📑', label: '复制整行', action: () => {
          closeContextMenu()
          copyAsTsvData(rows)
        } },
        'separator',
        { icon: '🔤', label: '设置为空白字符串', action: () => { closeContextMenu(); flash('error', '只读模式') }, disabled: true },
        { icon: '⊘', label: '设置为 NULL', action: () => { closeContextMenu(); flash('error', '只读模式') }, disabled: true },
        'separator',
        { icon: '📋', label: '复制为', shortcut: '▸', action: () => {}, children: [
          { icon: '📋', label: 'INSERT 语句', action: () => { closeContextMenu(); copyAsInsert(rows) } },
          { icon: '📋', label: 'UPDATE 语句', action: () => { closeContextMenu(); copyAsUpdate(rows) } },
          'separator',
          { icon: '📋', label: '制表符分隔值 (数据)', action: () => { closeContextMenu(); copyAsTsvData(rows) } },
          { icon: '📋', label: '制表符分隔值 (字段名)', action: () => { closeContextMenu(); copyAsTsvFields() } },
          { icon: '📋', label: '制表符分隔值 (字段名和数据)', action: () => { closeContextMenu(); copyAsTsvAll(rows) } },
        ] },
        'separator',
        { icon: '☑️', label: '全选', shortcut: '⌘A', action: () => {
          closeContextMenu()
          const all = new Set<number>()
          for (let j = 0; j < tab.rows.length; j++) all.add(j)
          setSelectedRows(all)
        } },
        { icon: '⬜', label: '取消全选', action: () => { closeContextMenu(); setSelectedRows(new Set()) } },
        'separator',
        { icon: '🔄', label: '刷新', shortcut: '⌘R', action: () => { closeContextMenu(); goToPage(tab.page) } },
      ])
    }

    const hasPendingEdits = Object.keys(pendingChanges).length > 0 || newRows.length > 0 || deletedRows.size > 0

    /** Get effective cell value (pending change or original) */
    function getCellValue(rowIdx: number, col: string): unknown {
      const key = `row_${rowIdx}`
      if (pendingChanges[key]?.[col] !== undefined) return pendingChanges[key][col]
      return tab.rows[rowIdx]?.[col]
    }

    /** Set a cell value in pending changes */
    function setCellValue(rowIdx: number, col: string, value: string) {
      const key = `row_${rowIdx}`
      const originalVal = tab.rows[rowIdx]?.[col]
      // Compare: if value matches original, remove from pending
      const originalStr = originalVal === null || originalVal === undefined ? '' : String(originalVal)
      if (value === originalStr) {
        // Same as original - remove this field from pending
        setPendingChanges(prev => {
          const updated = { ...prev }
          if (updated[key]) {
            const fields = { ...updated[key] }
            delete fields[col]
            if (Object.keys(fields).length === 0) {
              delete updated[key]
            } else {
              updated[key] = fields
            }
          }
          return updated
        })
      } else {
        setPendingChanges(prev => ({
          ...prev,
          [key]: { ...(prev[key] || {}), [col]: value === '' ? null : value }
        }))
      }
    }

    /** Check if a cell has been modified */
    function isCellModified(rowIdx: number, col: string): boolean {
      const key = `row_${rowIdx}`
      return pendingChanges[key]?.[col] !== undefined
    }

    /** Add a new empty row */
    function addNewRow() {
      const emptyRow: Record<string, unknown> = {}
      tab.columns.forEach(c => { emptyRow[c] = null })
      setNewRows(prev => [...prev, emptyRow])
      // Scroll to bottom after render
      setTimeout(() => {
        const wrap = document.querySelector('.data-grid-wrap')
        if (wrap) wrap.scrollTop = wrap.scrollHeight
      }, 50)
      // Select the new row
      const newIdx = tab.rows.length + newRows.length
      setSelectedRows(new Set([newIdx]))
      lastClickedRow.current = newIdx
    }

    /** Delete selected rows */
    function deleteSelectedRows() {
      if (selectedRows.size === 0) { flash('error', '请先选中要删除的行'); return }
      setDeletedRows(prev => {
        const next = new Set(prev)
        selectedRows.forEach(r => { if (r < tab.rows.length) next.add(r) })
        return next
      })
    }

    /** Discard all pending changes */
    function discardChanges() {
      setPendingChanges({})
      setNewRows([])
      setDeletedRows(new Set())
      setEditingCell(null)
    }

    /** Apply all pending changes via SQL */
    async function applyChanges() {
      const conn = liveConnections.get(tab.connectionId)
      if (!conn) return
      const sqls: string[] = []
      const cols = tab.columns
      const firstCol = cols[0] // assume PK

      // UPDATEs
      for (const [key, changes] of Object.entries(pendingChanges)) {
        const idx = parseInt(key.replace('row_', ''), 10)
        if (deletedRows.has(idx)) continue // skip deleted
        const original = tab.rows[idx]
        if (!original) continue
        const sets = Object.entries(changes).map(([c, v]) => `\`${c}\` = ${sqlVal(v)}`)
        const where = `\`${firstCol}\` = ${sqlVal(original[firstCol])}`
        sqls.push(`UPDATE \`${tab.schemaName}\`.\`${tab.tableName}\` SET ${sets.join(', ')} WHERE ${where};`)
      }

      // INSERTs (new rows)
      for (const nr of newRows) {
        const vals = cols.map(c => sqlVal(nr[c]))
        sqls.push(`INSERT INTO \`${tab.schemaName}\`.\`${tab.tableName}\` (${cols.map(c => `\`${c}\``).join(', ')}) VALUES (${vals.join(', ')});`)
      }

      // DELETEs
      for (const idx of deletedRows) {
        const original = tab.rows[idx]
        if (!original) continue
        const where = `\`${firstCol}\` = ${sqlVal(original[firstCol])}`
        sqls.push(`DELETE FROM \`${tab.schemaName}\`.\`${tab.tableName}\` WHERE ${where};`)
      }

      if (sqls.length === 0) { flash('error', '没有待提交的修改'); return }

      try {
        await api.runStatements(conn.profile, sqls, tab.schemaName, true)
        flash('success', `成功执行 ${sqls.length} 条 SQL`)
        discardChanges()
        goToPage(tab.page) // refresh
      } catch (err) {
        flash('error', `执行失败: ${err instanceof Error ? err.message : String(err)}`)
      }
    }

    /** Apply sort on column */
    function applySort(col: string, dir: 'ASC' | 'DESC' | null) {
      const newOrder = dir ? { col, dir } : null
      setOrderBy(newOrder)
      setColSortMenu(null)
      // Pass new sort directly to avoid stale closure
      goToPage(1, undefined, newOrder)
    }

    /** Toggle filter panel */
    function toggleFilter() {
      if (!filterOpen) {
        setFilterOpen(true)
        if (filterRules.length === 0) {
          setFilterRules([{ id: crypto.randomUUID(), enabled: true, col: tab.columns[0] || '', op: '=', value: null, connector: 'and' }])
        }
      } else {
        setFilterOpen(false)
        setFilterValuePicker(null)
      }
    }

    /** Add a filter rule */
    function addFilterRule() {
      setFilterRules(prev => [...prev, { id: crypto.randomUUID(), enabled: true, col: tab.columns[0] || '', op: '=', value: null, connector: 'and' }])
    }

    /** Add a filter group */
    function addFilterGroup() {
      setFilterRules(prev => [...prev, {
        id: crypto.randomUUID(), enabled: true, col: '', op: '', value: null, connector: 'and',
        isGroup: true, children: [{ id: crypto.randomUUID(), enabled: true, col: tab.columns[0] || '', op: '=', value: null, connector: 'and' }]
      }])
    }

    /** Remove a filter rule */
    function removeFilterRule(ruleId: string, groupId?: string) {
      if (groupId) {
        setFilterRules(prev => prev.map(r => r.id === groupId && r.children
          ? { ...r, children: r.children.filter(c => c.id !== ruleId) }
          : r
        ))
      } else {
        setFilterRules(prev => prev.filter(r => r.id !== ruleId))
      }
    }

    /** Open value picker for a rule */
    async function openValuePicker(ruleId: string, col: string, groupId?: string) {
      setFilterValuePicker({ ruleId, groupId, values: [], selected: new Set(), search: '', loading: true })
      const conn = liveConnections.get(tab.connectionId)
      if (!conn) return
      try {
        const sql = `SELECT DISTINCT \`${col}\` AS val FROM ${q(tab.schemaName)}.${q(tab.tableName)} ORDER BY val LIMIT 200`
        const resp = await api.runQuery(conn.profile, sql, tab.schemaName)
        const result = resp.results[0]
        if (result?.kind === 'rows') {
          const vals = result.rows.map(r => r.val === null || r.val === undefined ? '__NULL__' : String(r.val))
          setFilterValuePicker(prev => prev ? { ...prev, values: vals, loading: false } : null)
        }
      } catch {
        setFilterValuePicker(prev => prev ? { ...prev, loading: false } : null)
      }
    }

    /** Apply value picker selection to the rule */
    function applyValuePicker() {
      if (!filterValuePicker) return
      const { ruleId, groupId, selected } = filterValuePicker
      // Also grab text from input if user typed directly without pressing Enter
      const inputText = pickerInputRef.current?.value?.trim() || ''
      const allSelected = new Set(selected)
      if (inputText) allSelected.add(inputText)
      const valueStr = allSelected.size > 0 ? Array.from(allSelected).join(', ') : null
      if (groupId) {
        setFilterRules(prev => prev.map(r => r.id === groupId && r.children
          ? { ...r, children: r.children.map(c => c.id === ruleId ? { ...c, value: valueStr } : c) }
          : r
        ))
      } else {
        setFilterRules(prev => prev.map(r => r.id === ruleId ? { ...r, value: valueStr } : r))
      }
      setFilterValuePicker(null)
    }

    /** Build WHERE from filter rules */
    const FILTER_OPS: { value: string; label: string; noValue?: boolean }[] = [
      { value: '=', label: '=' },
      { value: '!=', label: '!=' },
      { value: '<', label: '<' },
      { value: '<=', label: '<=' },
      { value: '>', label: '>' },
      { value: '>=', label: '>=' },
      { value: 'LIKE', label: '包含' },
      { value: 'NOT LIKE', label: '不包含' },
      { value: 'STARTS', label: '开始以' },
      { value: 'NOT STARTS', label: '不是开始于' },
      { value: 'ENDS', label: '结束以' },
      { value: 'NOT ENDS', label: '不是结束于' },
      { value: 'IS NULL', label: '是 null', noValue: true },
      { value: 'IS NOT NULL', label: '不是 null', noValue: true },
      { value: 'EMPTY', label: '是空的', noValue: true },
      { value: 'NOT EMPTY', label: '不是空的', noValue: true },
    ]
    const noValueOps = new Set(FILTER_OPS.filter(o => o.noValue).map(o => o.value))

    function buildWhereFromRules(rules: typeof filterRules): string {
      const parts: string[] = []
      for (const r of rules) {
        if (!r.enabled) continue
        if (r.isGroup && r.children && r.children.length > 0) {
          const inner = buildWhereFromRules(r.children)
          if (inner) parts.push(`(${inner})`)
        } else if (!r.isGroup) {
          const col = `\`${r.col}\``
          // No-value operators
          if (r.op === 'IS NULL') { parts.push(`${col} IS NULL`); continue }
          if (r.op === 'IS NOT NULL') { parts.push(`${col} IS NOT NULL`); continue }
          if (r.op === 'EMPTY') { parts.push(`(${col} IS NULL OR ${col} = '')`); continue }
          if (r.op === 'NOT EMPTY') { parts.push(`(${col} IS NOT NULL AND ${col} != '')`); continue }
          // Value-based operators
          if (r.value === null) continue
          const vals = r.value.split(',').map(v => v.trim()).filter(Boolean)
          if (vals.length === 0) continue
          const escaped = vals.map(v => v === '__NULL__' ? 'NULL' : `'${v.replace(/'/g, "''")}'`)
          const hasNull = vals.includes('__NULL__')
          const nonNull = escaped.filter((_, i) => vals[i] !== '__NULL__')
          const firstVal = escaped[0]
          switch (r.op) {
            case '=':
              if (vals.length === 1 && !hasNull) {
                parts.push(`${col} = ${firstVal}`)
              } else if (hasNull && nonNull.length > 0) {
                parts.push(`(${col} IN (${nonNull.join(', ')}) OR ${col} IS NULL)`)
              } else if (hasNull) {
                parts.push(`${col} IS NULL`)
              } else {
                parts.push(`${col} IN (${escaped.join(', ')})`)
              }
              break
            case '!=':
              if (vals.length === 1 && !hasNull) {
                parts.push(`${col} != ${firstVal}`)
              } else if (hasNull && nonNull.length > 0) {
                parts.push(`(${col} NOT IN (${nonNull.join(', ')}) AND ${col} IS NOT NULL)`)
              } else if (hasNull) {
                parts.push(`${col} IS NOT NULL`)
              } else {
                parts.push(`${col} NOT IN (${escaped.join(', ')})`)
              }
              break
            case 'LIKE':
              parts.push(`${col} LIKE '%${vals[0].replace(/'/g, "''")}%'`)
              break
            case 'NOT LIKE':
              parts.push(`${col} NOT LIKE '%${vals[0].replace(/'/g, "''")}%'`)
              break
            case 'STARTS':
              parts.push(`${col} LIKE '${vals[0].replace(/'/g, "''")}%'`)
              break
            case 'NOT STARTS':
              parts.push(`${col} NOT LIKE '${vals[0].replace(/'/g, "''")}%'`)
              break
            case 'ENDS':
              parts.push(`${col} LIKE '%${vals[0].replace(/'/g, "''")}'`)
              break
            case 'NOT ENDS':
              parts.push(`${col} NOT LIKE '%${vals[0].replace(/'/g, "''")}'`)
              break
            default:
              parts.push(`${col} ${r.op} ${firstVal}`)
          }
        }
      }
      return parts.join(' AND ')
    }

    /** Apply all filter rules */
    function applyFilterRules() {
      // If a value picker is open, auto-apply it first
      let rules = filterRules
      if (filterValuePicker) {
        const { ruleId, groupId, selected } = filterValuePicker
        const inputText = pickerInputRef.current?.value?.trim() || ''
        const allSelected = new Set(selected)
        if (inputText) allSelected.add(inputText)
        const valueStr = allSelected.size > 0 ? Array.from(allSelected).join(', ') : null
        if (groupId) {
          rules = rules.map(r => r.id === groupId && r.children
            ? { ...r, children: r.children.map(c => c.id === ruleId ? { ...c, value: valueStr } : c) }
            : r
          )
        } else {
          rules = rules.map(r => r.id === ruleId ? { ...r, value: valueStr } : r)
        }
        setFilterRules(rules)
        setFilterValuePicker(null)
      }
      const where = buildWhereFromRules(rules)
      setFilterWhere(where)
      // Pass the WHERE clause directly to avoid stale closure issue
      setTimeout(() => goToPage(1, where), 0)
    }

    // Combine original rows + new rows for rendering
    const allRows: DataGridRow[] = []
    tab.rows.forEach((row, i) => {
      allRows.push({ data: row, type: 'existing', originalIdx: i })
    })
    newRows.forEach((nr, i) => {
      allRows.push({ data: nr, type: 'new', originalIdx: tab.rows.length + i })
    })

    function getGridCellValue(item: DataGridRow, col: string) {
      return item.type === 'new' ? item.data[col] : getCellValue(item.originalIdx, col)
    }

    function isGridCellModified(item: DataGridRow, col: string) {
      return item.type === 'existing' && isCellModified(item.originalIdx, col)
    }

    function commitGridCell(item: DataGridRow, col: string, value: string) {
      if (item.type === 'new') {
        setNewRows(prev => {
          const copy = [...prev]
          const nrIdx = item.originalIdx - tab.rows.length
          copy[nrIdx] = { ...copy[nrIdx], [col]: value === '' ? null : value }
          return copy
        })
        return
      }
      setCellValue(item.originalIdx, col, value)
    }

    /** Render a single filter rule row */
    function renderFilterRule(rule: typeof filterRules[0], isLast: boolean, groupId?: string) {
      return (
        <div key={rule.id} className="navi-filter-row">
          <input type="checkbox" checked={rule.enabled} className="navi-filter-check"
            onChange={() => {
              if (groupId) {
                setFilterRules(prev => prev.map(r => r.id === groupId && r.children
                  ? { ...r, children: r.children.map(c => c.id === rule.id ? { ...c, enabled: !c.enabled } : c) }
                  : r))
              } else {
                setFilterRules(prev => prev.map(r => r.id === rule.id ? { ...r, enabled: !r.enabled } : r))
              }
            }}
          />
          <select className="navi-filter-col-select" value={rule.col}
            onChange={e => {
              const newCol = e.target.value
              if (groupId) {
                setFilterRules(prev => prev.map(r => r.id === groupId && r.children
                  ? { ...r, children: r.children.map(c => c.id === rule.id ? { ...c, col: newCol, value: null } : c) }
                  : r))
              } else {
                setFilterRules(prev => prev.map(r => r.id === rule.id ? { ...r, col: newCol, value: null } : r))
              }
            }}
          >
            {tab.columns.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
          <select className="navi-filter-op" value={rule.op}
            onChange={e => {
              const newOp = e.target.value
              if (groupId) {
                setFilterRules(prev => prev.map(r => r.id === groupId && r.children
                  ? { ...r, children: r.children.map(c => c.id === rule.id ? { ...c, op: newOp, value: noValueOps.has(newOp) ? null : c.value } : c) }
                  : r))
              } else {
                setFilterRules(prev => prev.map(r => r.id === rule.id ? { ...r, op: newOp, value: noValueOps.has(newOp) ? null : r.value } : r))
              }
            }}
          >
            {FILTER_OPS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
          {!noValueOps.has(rule.op) && (
            <span className="navi-filter-val" onClick={() => openValuePicker(rule.id, rule.col, groupId)}>
              {rule.value !== null ? rule.value : '?'}
            </span>
          )}
          {!isLast && <span className="navi-filter-connector">and</span>}
          {isLast && !groupId && (
            <>
              <span className="navi-filter-btn-icon" title="添加条件" onClick={addFilterRule}>+</span>
              <span className="navi-filter-btn-icon navi-filter-btn-group" title="添加分组条件" onClick={addFilterGroup}>()+</span>
            </>
          )}
          <button className="navi-filter-remove" onClick={() => removeFilterRule(rule.id, groupId)} title="删除条件">✕</button>
        </div>
      )
    }

    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', position: 'relative' }}>
        {/* Status bar with filter toggle */}
        <div className="data-toolbar">
          <button className={`toolbar-icon-btn${filterOpen ? ' active' : ''}`} onClick={toggleFilter} title="筛选">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M10 18h4v-2h-4v2zM3 6v2h18V6H3zm3 7h12v-2H6v2z"/></svg>
          </button>
          <div style={{ flex: 1 }} />
          {tab.loading && <span style={{ fontSize: 11, color: '#999' }}>⏳ 加载中...</span>}
          {filterWhere && <span style={{ fontSize: 11, color: '#4285f4' }}>⏏ 已筛选</span>}
          {selectedRows.size > 0 && (
            <span style={{ fontSize: 11, color: '#666' }}>已选中 {selectedRows.size} 行</span>
          )}
          {hasPendingEdits && (
            <span style={{ fontSize: 11, color: '#e67e22', fontWeight: 600 }}>⚠️ 有未保存的修改</span>
          )}
        </div>

        {/* Navicat filter panel — between toolbar and column headers */}
        {filterOpen && (
          <div className="navi-filter-panel">
            {/* Filter conditions */}
            <div className="navi-filter-conditions">
              {filterRules.map((rule, idx) => {
                if (rule.isGroup && rule.children) {
                  return (
                    <div key={rule.id} className="navi-filter-group">
                      <div className="navi-filter-row">
                        <input type="checkbox" checked={rule.enabled} className="navi-filter-check"
                          onChange={() => setFilterRules(prev => prev.map(r => r.id === rule.id ? { ...r, enabled: !r.enabled } : r))} />
                        <span className="navi-filter-group-paren">(</span>
                        {idx < filterRules.length - 1 && <span className="navi-filter-connector">and</span>}
                      </div>
                      <div className="navi-filter-group-children">
                        {rule.children.map((child, ci) => renderFilterRule(child, ci === rule.children!.length - 1, rule.id))}
                        <div className="navi-filter-row">
                          <span className="navi-filter-group-paren">)</span>
                          <span className="navi-filter-btn-icon" title="添加条件" onClick={() => {
                            setFilterRules(prev => prev.map(r => r.id === rule.id && r.children
                              ? { ...r, children: [...r.children, { id: crypto.randomUUID(), enabled: true, col: tab.columns[0] || '', op: '=', value: null, connector: 'and' }] }
                              : r))
                          }}>+</span>
                          <button className="navi-filter-remove" onClick={() => removeFilterRule(rule.id)} title="删除分组">✕</button>
                        </div>
                      </div>
                    </div>
                  )
                }
                return renderFilterRule(rule, idx === filterRules.length - 1)
              })}
            </div>
            {/* Filter toolbar */}
            <div className="navi-filter-toolbar">
              <span className="navi-filter-apply" onClick={applyFilterRules}>✓ 应用</span>
              {filterWhere && <span className="navi-filter-status">已编辑准则</span>}
            </div>
          </div>
        )}

        {/* Value picker popup */}
        {filterValuePicker && (
          <div className="navi-picker-overlay" onMouseDown={() => setFilterValuePicker(null)}>
            <div className="navi-picker" onMouseDown={e => e.stopPropagation()}>
              <input
                ref={pickerInputRef}
                className="navi-picker-input"
                autoFocus
                placeholder=""
                onKeyDown={e => {
                  if (e.key === 'Enter') {
                    const v = (e.target as HTMLInputElement).value.trim()
                    if (!v) return
                    setFilterValuePicker(prev => prev ? {
                      ...prev,
                      selected: new Set([...prev.selected, v]),
                    } : null)
                    ;(e.target as HTMLInputElement).value = ''
                  }
                }}
              />
              <div className="navi-picker-section">建议值</div>
              <div className="navi-picker-list">
                {filterValuePicker.loading ? (
                  <div className="navi-picker-loading">加载中...</div>
                ) : (
                  filterValuePicker.values
                    .filter(v => !filterValuePicker.search || (v === '__NULL__' ? 'NULL' : v).toLowerCase().includes(filterValuePicker.search.toLowerCase()))
                    .map(v => {
                      const display = v === '__NULL__' ? '(NULL)' : v
                      const isChecked = filterValuePicker.selected.has(v)
                      return (
                        <label key={v} className="navi-picker-item">
                          <input
                            type="checkbox"
                            checked={isChecked}
                            onChange={() => {
                              setFilterValuePicker(prev => {
                                if (!prev) return null
                                const next = new Set(prev.selected)
                                if (isChecked) next.delete(v)
                                else next.add(v)
                                return { ...prev, selected: next }
                              })
                            }}
                          />
                          <span className="navi-picker-text" title={display}>{display}</span>
                        </label>
                      )
                    })
                )}
              </div>
              <div className="navi-picker-search-row">
                <span className="navi-picker-search-icon">🔍</span>
                <input
                  className="navi-picker-search"
                  placeholder="搜索"
                  value={filterValuePicker.search}
                  onChange={e => setFilterValuePicker(prev => prev ? { ...prev, search: e.target.value } : null)}
                />
              </div>
              <div className="navi-picker-actions">
                <button className="navi-picker-btn" onClick={() => setFilterValuePicker(null)}>取消</button>
                <button className="navi-picker-btn navi-picker-btn-ok" onClick={applyValuePicker}>好</button>
              </div>
            </div>
          </div>
        )}

        <VirtualDataGrid
          columns={tab.columns}
          rows={allRows}
          rowOffset={rowOffset}
          resetScrollKey={tab.rows}
          selectedRows={selectedRows}
          deletedRows={deletedRows}
          editingCell={editingCell}
          orderBy={orderBy}
          colSortMenu={colSortMenu}
          onSetSortMenu={setColSortMenu}
          onApplySort={applySort}
          onRowClick={handleRowClick}
          onCellContextMenu={cellContextMenu}
          onStartEdit={(row, col) => setEditingCell({ row, col })}
          onCommitCell={commitGridCell}
          onCancelEdit={() => setEditingCell(null)}
          getCellValue={getGridCellValue}
          isCellModified={isGridCellModified}
        />
        <div className="data-footer">
          <div className="data-action-bar">
            <button className="action-btn" onClick={addNewRow} title="添加一行">+</button>
            <button className="action-btn" onClick={deleteSelectedRows} title="删除选中行">−</button>
            <button className="action-btn apply" onClick={() => void applyChanges()} disabled={!hasPendingEdits} title="提交修改">✓</button>
            <button className="action-btn cancel" onClick={discardChanges} disabled={!hasPendingEdits} title="取消修改">✗</button>
            <button className="action-btn" onClick={() => goToPage(tab.page)} title="刷新">↻</button>
            {selectedRows.size > 0 && (
              <span className="action-status">已选择 {selectedRows.size} 行</span>
            )}
          </div>
          <div className="data-footer-right">
            <div className="pagination">
              <button
                className="page-btn"
                disabled={tab.page <= 1}
                onClick={() => goToPage(1)}
                title="首页"
              >⏮</button>
              <button
                className="page-btn"
                disabled={tab.page <= 1}
                onClick={() => goToPage(tab.page - 1)}
                title="上一页"
              >◀</button>
              <span className="page-info">
                第
                <input
                  className="page-input"
                  type="text"
                  value={tab.page}
                  onChange={e => {
                    const val = e.target.value.replace(/\D/g, '')
                    if (val === '') {
                      updateTab<DataTab>(tab.id, { page: 0 as unknown as number })
                      return
                    }
                    updateTab<DataTab>(tab.id, { page: parseInt(val, 10) })
                  }}
                  onKeyDown={e => {
                    if (e.key === 'Enter') {
                      const p = Math.max(1, tab.page)
                      goToPage(p)
                    }
                  }}
                  onBlur={() => {
                    const p = Math.max(1, tab.page)
                    if (p !== tab.page) {
                      updateTab<DataTab>(tab.id, { page: p })
                    }
                    goToPage(p)
                  }}
                />
                页
              </span>
              <button
                className="page-btn"
                disabled={!tab.hasMore}
                onClick={() => goToPage(tab.page + 1)}
                title="下一页"
              >▶</button>
            </div>
            <span className="page-count">{tab.rows.length} 条记录{tab.hasMore ? '+' : ''}</span>
          </div>
        </div>
      </div>
    )
  }

  /* ── Query tab content ──────────────────────── */
  function renderQueryTab(tab: QueryTab) {
    const conn = liveConnections.get(tab.connectionId)
    const schemas = conn?.schemas ?? []

    return (
      <QueryTabPane
        key={tab.id}
        tab={tab}
        schemas={schemas}
        history={history}
        flash={flash}
        onPersistSql={persistQuerySql}
        onChangeSchema={changeQuerySchema}
        onRunQuery={runQuery}
        onCancelRunningQuery={cancelRunningQuery}
        onClearHistory={clearQueryHistory}
        onSetActiveResult={setQueryActiveResult}
      />
    )
  }

  /* ── CLI tab content ──────────────────────────── */
  function renderCliTab(tab: CliTab) {
    const conn = liveConnections.get(tab.connectionId)
    if (!conn) return <div className="empty-state"><span className="empty-title">未连接</span></div>

    const inputHistory = tab.history.map(h => h.input).filter(Boolean)

    function scrollToBottom() {
      setTimeout(() => {
        if (cliScrollRef.current) {
          cliScrollRef.current.scrollTop = cliScrollRef.current.scrollHeight
        }
      }, 50)
    }

    async function executeCli(sql: string) {
      if (!sql.trim()) return
      updateTab<CliTab>(tab.id, { loading: true })

      // Detect USE xxx command
      const useMatch = sql.trim().match(/^use\s+[`]?(\w+)[`]?\s*;?\s*$/i)
      const effectiveSchema = useMatch ? useMatch[1] : tab.schemaName

      let output = ''
      let isError = false
      try {
        const resp = await api.runQuery(conn!.profile, sql, effectiveSchema)
        const result = resp.results[0]
        if (!result) {
          output = 'OK'
        } else if (result.kind === 'rows') {
          if (result.rows.length === 0) {
            output = 'Empty set'
          } else {
            // Format as ASCII table
            const cols = result.columns
            const widths = cols.map(c => c.length)
            const strRows = result.rows.map(row =>
              cols.map((c, i) => {
                const v = row[c] === null || row[c] === undefined ? 'NULL' : String(row[c])
                widths[i] = Math.max(widths[i], v.length)
                return v
              })
            )
            const sep = '+' + widths.map(w => '-'.repeat(w + 2)).join('+') + '+'
            const hdr = '|' + cols.map((c, i) => ` ${c.padEnd(widths[i])} `).join('|') + '|'
            const dataLines = strRows.map(r =>
              '|' + r.map((v, i) => ` ${v.padEnd(widths[i])} `).join('|') + '|'
            )
            const lines = [sep, hdr, sep, ...dataLines, sep]
            lines.push(`${result.rows.length} row${result.rows.length > 1 ? 's' : ''} in set`)
            output = lines.join('\n')
          }
        } else if (result.kind === 'mutation') {
          output = `Query OK, ${result.affectedRows} row${result.affectedRows !== 1 ? 's' : ''} affected`
          if (result.info) output += `\n${result.info}`
        } else if (result.kind === 'message') {
          output = result.message
        }

        // If USE succeeded, switch the CLI's active database
        if (useMatch && !isError) {
          output = `Database changed to ${effectiveSchema}`
        }
      } catch (err) {
        output = `ERROR: ${err instanceof Error ? err.message : String(err)}`
        isError = true
      }

      updateTab<CliTab>(tab.id, {
        history: [...tab.history, { input: sql, output, isError }],
        loading: false,
        ...(useMatch && !isError ? { schemaName: effectiveSchema } : {}),
      })
      setCliInput('')
      setCliHistoryIdx(-1)
      scrollToBottom()
      // Re-focus input after execution
      setTimeout(() => cliInputRef.current?.focus(), 60)
    }

    function handleCliKeyDown(e: React.KeyboardEvent) {
      if (e.key === 'Enter' && !tab.loading) {
        e.preventDefault()
        void executeCli(cliInput)
      } else if (e.key === 'ArrowUp') {
        e.preventDefault()
        const newIdx = cliHistoryIdx === -1 ? inputHistory.length - 1 : Math.max(0, cliHistoryIdx - 1)
        if (newIdx >= 0 && newIdx < inputHistory.length) {
          setCliHistoryIdx(newIdx)
          setCliInput(inputHistory[newIdx])
        }
      } else if (e.key === 'ArrowDown') {
        e.preventDefault()
        if (cliHistoryIdx === -1) return
        const newIdx = cliHistoryIdx + 1
        if (newIdx >= inputHistory.length) {
          setCliHistoryIdx(-1)
          setCliInput('')
        } else {
          setCliHistoryIdx(newIdx)
          setCliInput(inputHistory[newIdx])
        }
      } else if (e.key === 'l' && e.ctrlKey) {
        e.preventDefault()
        updateTab<CliTab>(tab.id, { history: [] })
      }
    }

    return (
      <div className="cli-container">
        <div className="cli-output" ref={cliScrollRef}>
          <div className="cli-welcome">
            MySQL [{conn.profile.label}] — {tab.schemaName}
          </div>
          {tab.history.map((entry, i) => (
            <div key={i} className="cli-entry">
              <div className="cli-prompt-line">
                <span className="cli-prompt">mysql&gt; </span>
                <span className="cli-cmd">{entry.input}</span>
              </div>
              <pre className={`cli-result${entry.isError ? ' cli-error' : ''}`}>{entry.output}</pre>
            </div>
          ))}
          <div className="cli-input-line" onClick={() => cliInputRef.current?.focus()}>
            <span className="cli-prompt">mysql&gt; </span>
            <input
              ref={cliInputRef}
              className="cli-input"
              value={cliInput}
              onChange={e => setCliInput(e.target.value)}
              onKeyDown={handleCliKeyDown}
              placeholder={tab.loading ? '执行中...' : '输入 SQL 命令...'}
              disabled={tab.loading}
              autoFocus
              spellCheck={false}
            />
          </div>
        </div>
      </div>
    )
  }

  function renderDesignTab(tab: DesignTab) {
    const conn = liveConnections.get(tab.connectionId)
    if (!conn) return <div className="empty-state"><span className="empty-title">未连接</span></div>

    return (
      <DesignTabWrapper
        key={tab.id}
        tab={tab}
        profile={conn.profile}
        onSuccess={(message) => {
          flash('success', message)
          closeTab(tab.id)
          void handleConnect(conn.profile)
        }}
        onError={(message) => flash('error', message)}
        onCancel={() => closeTab(tab.id)}
      />
    )
  }

  /* ── Tab content router ─────────────────────── */
  function renderTabContent() {
    if (!activeTab) {
      return (
        <div className="empty-state">
          <span className="empty-icon">🐕</span>
          <span className="empty-title">NaviDog</span>
          <span>连接数据库开始使用，或新建查询</span>
        </div>
      )
    }
    if (activeTab.kind === 'objects') return renderObjectsTab(activeTab as AppTab & { kind: 'objects' })
    if (activeTab.kind === 'data') return renderDataTab(activeTab as DataTab)
    if (activeTab.kind === 'query') return renderQueryTab(activeTab as QueryTab)
    if (activeTab.kind === 'cli') return renderCliTab(activeTab as CliTab)
    if (activeTab.kind === 'design') return renderDesignTab(activeTab as DesignTab)
    return null
  }

  /* ── Info panel ─────────────────────────────── */
  function renderInfoPanel() {
    if (!infoPanelContent) {
      return (
        <div className="info-body" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#ccc' }}>
          <span>选择左侧项目查看详情</span>
        </div>
      )
    }

    if (infoPanelContent.kind === 'connection') {
      const { profile, version, online, connecting } = infoPanelContent
      return (
        <>
          <div className="info-header">
            <div className="info-icon conn-icon">🔌</div>
            <div>
              <div className="info-title">{profile.label}</div>
              <div className="info-subtitle">MySQL</div>
            </div>
          </div>
          <div className="info-body">
            {version && <div className="info-row"><span className="info-label">服务器版本</span><span className="info-value">{version}</span></div>}
            <div className="info-row"><span className="info-label">状态</span><span className="info-value">{connecting ? '🟦 正在连接...' : online ? '🟢 已连接' : '⚪ 未连接'}</span></div>
            <div className="info-row"><span className="info-label">主机</span><span className="info-value">{profile.host}</span></div>
            <div className="info-row"><span className="info-label">端口</span><span className="info-value">{profile.port}</span></div>
            <div className="info-row"><span className="info-label">用户名</span><span className="info-value">{profile.user}</span></div>
            {profile.database && <div className="info-row"><span className="info-label">默认数据库</span><span className="info-value">{profile.database}</span></div>}
          </div>
        </>
      )
    }

    if (infoPanelContent.kind === 'database') {
      const { profile, schemaName, tableCount } = infoPanelContent
      return (
        <>
          <div className="info-header">
            <div className="info-icon db-icon">📦</div>
            <div>
              <div className="info-title">{schemaName}</div>
              <div className="info-subtitle">已打开</div>
            </div>
          </div>
          <div className="info-body">
            <div className="info-row"><span className="info-label">连接</span><span className="info-value">🔗 {profile.label}</span></div>
            <div className="info-row"><span className="info-label">字符集</span><span className="info-value">utf8</span></div>
            <div className="info-row"><span className="info-label">排序规则</span><span className="info-value">utf8_general_ci</span></div>
            <div className="info-row"><span className="info-label">表数量</span><span className="info-value">{tableCount}</span></div>
          </div>
        </>
      )
    }

    if (infoPanelContent.kind === 'table') {
      const { profile, schemaName, tableName, ddl, columns, metadata } = infoPanelContent
      const statsKey = `${profile.id}:${schemaName}.${tableName}`
      const stats = tableStatsCache[statsKey]

      function formatBytes(bytes: unknown): string {
        const n = Number(bytes)
        if (!n || isNaN(n)) return '0 B (0)'
        if (n < 1024) return `${n} B (${n})`
        if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB (${n.toLocaleString()})`
        return `${(n / (1024 * 1024)).toFixed(1)} MB (${n.toLocaleString()})`
      }

      function ddlContextMenu(e: React.MouseEvent) {
        showContextMenu(e, [
          { icon: '📋', label: '复制', shortcut: '⌘C', action: () => {
            closeContextMenu()
            const sel = window.getSelection()?.toString()
            void navigator.clipboard.writeText(sel || ddl || '')
          } },
          { icon: '☑️', label: '全选', shortcut: '⌘A', action: () => {
            closeContextMenu()
            const ddlEl = document.querySelector('.ddl-area')
            if (ddlEl) {
              const range = document.createRange()
              range.selectNodeContents(ddlEl)
              const sel = window.getSelection()
              sel?.removeAllRanges()
              sel?.addRange(range)
            }
          } },
          'separator',
          { icon: '✨', label: '美化 SQL', action: () => { closeContextMenu(); flash('success', '已是格式化状态') } },
        ])
      }

      function refreshTableIntrospection() {
        setTableMetaCache(prev => {
          const next = { ...prev }
          delete next[statsKey]
          return next
        })
        setTableStatsCache(prev => {
          const next = { ...prev }
          delete next[statsKey]
          return next
        })
        setDdlCache(prev => {
          const next = { ...prev }
          delete next[statsKey]
          return next
        })
        void fetchTableMetadata(profile, schemaName, tableName)
        void fetchTableStats(profile, schemaName, tableName)
        void fetchDdl(profile, schemaName, tableName)
      }

      async function createIndex() {
        if (!columns || columns.length === 0) {
          flash('error', '列信息尚未加载完成')
          return
        }

        const defaultName = `idx_${tableName}_${columns[0].name}`
        const name = prompt('索引名称', defaultName)?.trim()
        if (!name) return

        const columnInput = prompt(
          `索引列（可用列: ${columns.map(column => column.name).join(', ')}）`,
          columns[0].name,
        )?.trim()
        if (!columnInput) return

        const selectedColumns = columnInput
          .split(',')
          .map(column => column.trim())
          .filter(Boolean)

        const unknownColumns = selectedColumns.filter(column =>
          !columns.some(existingColumn => existingColumn.name === column),
        )
        if (selectedColumns.length === 0 || unknownColumns.length > 0) {
          flash('error', unknownColumns.length > 0
            ? `不存在的列: ${unknownColumns.join(', ')}`
            : '至少需要一个索引列')
          return
        }

        const unique = confirm('是否创建为唯一索引？')
        const sql = `ALTER TABLE ${q(schemaName)}.${q(tableName)} ADD ${unique ? 'UNIQUE ' : ''}INDEX ${q(name)} (${selectedColumns.map(q).join(', ')})`

        try {
          await api.runQuery(profile, sql, schemaName)
          flash('success', `已创建索引 ${name}`)
          refreshTableIntrospection()
        } catch (err) {
          flash('error', `创建索引失败: ${err instanceof Error ? err.message : String(err)}`)
        }
      }

      async function dropIndex(indexName: string) {
        if (!confirm(`确定要删除索引 ${indexName} 吗？`)) return

        try {
          await api.runQuery(profile, `ALTER TABLE ${q(schemaName)}.${q(tableName)} DROP INDEX ${q(indexName)}`, schemaName)
          flash('success', `已删除索引 ${indexName}`)
          refreshTableIntrospection()
        } catch (err) {
          flash('error', `删除索引失败: ${err instanceof Error ? err.message : String(err)}`)
        }
      }

      async function createForeignKey() {
        if (!columns || columns.length === 0) {
          flash('error', '列信息尚未加载完成')
          return
        }

        const defaultName = `fk_${tableName}_${columns[0].name}`
        const name = prompt('外键名称', defaultName)?.trim()
        if (!name) return

        const localColumnInput = prompt(
          `本表列（可用列: ${columns.map(column => column.name).join(', ')}）`,
          columns[0].name,
        )?.trim()
        if (!localColumnInput) return

        const localColumns = localColumnInput
          .split(',')
          .map(column => column.trim())
          .filter(Boolean)
        const invalidLocalColumns = localColumns.filter(column =>
          !columns.some(existingColumn => existingColumn.name === column),
        )
        if (localColumns.length === 0 || invalidLocalColumns.length > 0) {
          flash('error', invalidLocalColumns.length > 0
            ? `不存在的本表列: ${invalidLocalColumns.join(', ')}`
            : '至少需要一个本表列')
          return
        }

        const referencedTableInput = prompt('引用表（格式: schema.table）')?.trim()
        if (!referencedTableInput || !referencedTableInput.includes('.')) {
          flash('error', '引用表格式必须为 schema.table')
          return
        }
        const [referencedSchema, referencedTable] = referencedTableInput.split('.', 2).map(part => part.trim())
        if (!referencedSchema || !referencedTable) {
          flash('error', '引用表格式必须为 schema.table')
          return
        }

        let referencedColumns: string[] = []
        try {
          const referencedColumnList = await api.fetchTableColumns(profile, referencedSchema, referencedTable)
          const referencedColumnInput = prompt(
            `引用列（可用列: ${referencedColumnList.columns.map(column => column.name).join(', ')}）`,
            referencedColumnList.columns[0]?.name ?? '',
          )?.trim()
          if (!referencedColumnInput) return

          referencedColumns = referencedColumnInput
            .split(',')
            .map(column => column.trim())
            .filter(Boolean)

          const invalidReferencedColumns = referencedColumns.filter(column =>
            !referencedColumnList.columns.some(existingColumn => existingColumn.name === column),
          )
          if (referencedColumns.length === 0 || invalidReferencedColumns.length > 0) {
            flash('error', invalidReferencedColumns.length > 0
              ? `不存在的引用列: ${invalidReferencedColumns.join(', ')}`
              : '至少需要一个引用列')
            return
          }
        } catch (err) {
          flash('error', `无法读取引用表结构: ${err instanceof Error ? err.message : String(err)}`)
          return
        }

        if (localColumns.length !== referencedColumns.length) {
          flash('error', '本表列与引用列数量必须一致')
          return
        }

        const normalizeRule = (value: string | null, fallback: string) =>
          (value?.trim().toUpperCase() || fallback).replace(/\s+/g, ' ')

        const onUpdate = normalizeRule(prompt('ON UPDATE 规则', 'RESTRICT'), 'RESTRICT')
        const onDelete = normalizeRule(prompt('ON DELETE 规则', 'RESTRICT'), 'RESTRICT')
        const allowedRules = new Set(['RESTRICT', 'CASCADE', 'SET NULL', 'NO ACTION'])
        if (!allowedRules.has(onUpdate) || !allowedRules.has(onDelete)) {
          flash('error', '规则仅支持 RESTRICT / CASCADE / SET NULL / NO ACTION')
          return
        }

        const sql = `ALTER TABLE ${q(schemaName)}.${q(tableName)} ADD CONSTRAINT ${q(name)} FOREIGN KEY (${localColumns.map(q).join(', ')}) REFERENCES ${q(referencedSchema)}.${q(referencedTable)} (${referencedColumns.map(q).join(', ')}) ON UPDATE ${onUpdate} ON DELETE ${onDelete}`
        try {
          await api.runQuery(profile, sql, schemaName)
          flash('success', `已创建外键 ${name}`)
          refreshTableIntrospection()
        } catch (err) {
          flash('error', `创建外键失败: ${err instanceof Error ? err.message : String(err)}`)
        }
      }

      async function dropForeignKey(constraintName: string) {
        if (!confirm(`确定要删除外键 ${constraintName} 吗？`)) return

        try {
          await api.runQuery(profile, `ALTER TABLE ${q(schemaName)}.${q(tableName)} DROP FOREIGN KEY ${q(constraintName)}`, schemaName)
          flash('success', `已删除外键 ${constraintName}`)
          refreshTableIntrospection()
        } catch (err) {
          flash('error', `删除外键失败: ${err instanceof Error ? err.message : String(err)}`)
        }
      }

      return (
        <>
          <div className="info-header">
            <div className="info-icon table-icon">📄</div>
            <div>
              <div className="info-title">{tableName}</div>
              <div className="info-subtitle">表</div>
            </div>
          </div>
          <div className="info-tab-bar">
            <button
              className={`info-tab${infoPanelTab === 'info' ? ' active' : ''}`}
              onClick={() => setInfoPanelTab('info')}
              title="信息"
            >ℹ️</button>
            <button
              className={`info-tab${infoPanelTab === 'ddl' ? ' active' : ''}`}
              onClick={() => setInfoPanelTab('ddl')}
              title="DDL"
            >DDL</button>
            <button
              className={`info-tab${infoPanelTab === 'columns' ? ' active' : ''}`}
              onClick={() => setInfoPanelTab('columns')}
              title="列"
            >📊</button>
            <button
              className={`info-tab${infoPanelTab === 'indexes' ? ' active' : ''}`}
              onClick={() => setInfoPanelTab('indexes')}
              title="索引"
            >🔑</button>
            <button
              className={`info-tab${infoPanelTab === 'foreignKeys' ? ' active' : ''}`}
              onClick={() => setInfoPanelTab('foreignKeys')}
              title="外键"
            >🔗</button>
          </div>
          {infoPanelTab === 'info' ? (
            <div className="info-body">
              <div className="info-row"><span className="info-label">🔗 {schemaName}</span></div>
              <div className="info-row"><span className="info-label">🗄️ {profile.label}</span></div>
              {stats ? (
                <>
                  <div className="info-row"><span className="info-label">行格式</span><span className="info-value">{String(stats.ROW_FORMAT ?? '--')}</span></div>
                  <div className="info-row"><span className="info-label">平均行长度</span><span className="info-value">{formatBytes(stats.AVG_ROW_LENGTH)}</span></div>
                  <div className="info-row"><span className="info-label">最大数据长度</span><span className="info-value">{formatBytes(stats.MAX_DATA_LENGTH)}</span></div>
                  <div className="info-row"><span className="info-label">索引长度</span><span className="info-value">{formatBytes(stats.INDEX_LENGTH)}</span></div>
                  <div className="info-row"><span className="info-label">检查时间</span><span className="info-value">{String(stats.CHECK_TIME ?? '--')}</span></div>
                  <div className="info-row"><span className="info-label">自动递增</span><span className="info-value">{String(stats.AUTO_INCREMENT ?? '0')}</span></div>
                  <div className="info-row"><span className="info-label">数据可用空间</span><span className="info-value">{formatBytes(stats.DATA_FREE)}</span></div>
                  <div className="info-row"><span className="info-label">创建选项</span><span className="info-value">{String(stats.CREATE_OPTIONS || '--')}</span></div>
                  <div className="info-row"><span className="info-label">排序规则</span><span className="info-value">{String(stats.TABLE_COLLATION ?? '--')}</span></div>
                  <div className="info-row"><span className="info-label">引擎</span><span className="info-value">{String(stats.ENGINE ?? '--')}</span></div>
                  <div className="info-row"><span className="info-label">数据量</span><span className="info-value">{formatBytes(stats.DATA_LENGTH)}</span></div>
                  <div className="info-row"><span className="info-label">行数 (估算)</span><span className="info-value">{Number(stats.TABLE_ROWS ?? 0).toLocaleString()}</span></div>
                  <div className="info-row"><span className="info-label">注释</span><span className="info-value">{String(stats.TABLE_COMMENT || '--')}</span></div>
                </>
              ) : (
                <div style={{ color: '#aaa', fontSize: 12, padding: 8 }}>加载中...</div>
              )}
            </div>
          ) : infoPanelTab === 'ddl' ? (
            <div className="info-body ddl-body">
              {ddl ? (
                <div className="ddl-area" onContextMenu={ddlContextMenu} dangerouslySetInnerHTML={{ __html: highlightSql(ddl) }} />
              ) : (
                <div style={{ color: '#aaa', fontSize: 12, padding: 8 }}>加载中...</div>
              )}
            </div>
          ) : infoPanelTab === 'columns' ? (
            <div className="info-body columns-body">
              {columns ? (
                <table className="info-columns-table">
                  <thead>
                    <tr>
                      <th>列名</th>
                      <th>类型</th>
                      <th>可空</th>
                      <th>键</th>
                    </tr>
                  </thead>
                  <tbody>
                    {columns.map(col => (
                      <tr key={col.name}>
                        <td className="col-name">{col.name}</td>
                        <td className="col-type">{col.type}</td>
                        <td>{col.nullable ? '✓' : ''}</td>
                        <td className="col-key">{col.key}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div style={{ color: '#aaa', fontSize: 12, padding: 8 }}>加载中...</div>
              )}
            </div>
          ) : infoPanelTab === 'indexes' ? (
            <div className="info-body columns-body">
              {metadata ? (
                metadata.indexes.length > 0 ? (
                  <>
                    <div className="info-inline-actions">
                      <button className="btn btn-sm btn-primary" onClick={() => void createIndex()}>新增索引</button>
                    </div>
                    <table className="info-columns-table">
                      <thead>
                        <tr>
                          <th>名称</th>
                          <th>列</th>
                          <th>类型</th>
                          <th>属性</th>
                          <th>操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {metadata.indexes.map(index => (
                          <tr key={index.name}>
                            <td className="col-name">{index.name}</td>
                            <td className="col-type">{index.columns.join(', ')}</td>
                            <td>{index.type || '--'}</td>
                            <td>{index.primary ? 'PRIMARY' : index.unique ? 'UNIQUE' : 'INDEX'}</td>
                            <td>
                              {!index.primary && (
                                <button className="btn btn-sm btn-danger" onClick={() => void dropIndex(index.name)}>删除</button>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                ) : (
                  <div style={{ padding: 8 }}>
                    <div style={{ color: '#aaa', fontSize: 12, marginBottom: 8 }}>暂无索引信息</div>
                    <button className="btn btn-sm btn-primary" onClick={() => void createIndex()}>新增索引</button>
                  </div>
                )
              ) : (
                <div style={{ color: '#aaa', fontSize: 12, padding: 8 }}>加载中...</div>
              )}
            </div>
          ) : (
            <div className="info-body columns-body">
              {metadata ? (
                metadata.foreignKeys.length > 0 ? (
                  <>
                    <div className="info-inline-actions">
                      <button className="btn btn-sm btn-primary" onClick={() => void createForeignKey()}>新增外键</button>
                    </div>
                    <table className="info-columns-table">
                      <thead>
                        <tr>
                          <th>名称</th>
                          <th>列</th>
                          <th>引用</th>
                          <th>规则</th>
                          <th>操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {metadata.foreignKeys.map(foreignKey => (
                          <tr key={foreignKey.name}>
                            <td className="col-name">{foreignKey.name}</td>
                            <td className="col-type">{foreignKey.columns.join(', ')}</td>
                            <td>{`${foreignKey.referencedSchema}.${foreignKey.referencedTable} (${foreignKey.referencedColumns.join(', ')})`}</td>
                            <td>{`ON UPDATE ${foreignKey.onUpdate} / ON DELETE ${foreignKey.onDelete}`}</td>
                            <td>
                              <button className="btn btn-sm btn-danger" onClick={() => void dropForeignKey(foreignKey.name)}>删除</button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                ) : (
                  <div style={{ padding: 8 }}>
                    <div style={{ color: '#aaa', fontSize: 12, marginBottom: 8 }}>暂无外键信息</div>
                    <button className="btn btn-sm btn-primary" onClick={() => void createForeignKey()}>新增外键</button>
                  </div>
                )
              ) : (
                <div style={{ color: '#aaa', fontSize: 12, padding: 8 }}>加载中...</div>
              )}
            </div>
          )}
        </>
      )
    }

    return null
  }

  /* ═══════════════════════════════════════════════
     Connection Modal
     ═══════════════════════════════════════════════ */

  function renderConnectionModal() {
    if (!showModal) return null

    return (
      <div className="modal-overlay" onMouseDown={e => { if (e.target === e.currentTarget) setShowModal(false) }}>
        <div className="modal-card" onKeyDown={e => e.stopPropagation()}>
          <div className="modal-title-bar">
            <h3>{draft.id ? `${draft.label} — 编辑连接 (MySQL)` : '新建连接 (MySQL)'}</h3>
            <button className="modal-close" onClick={() => setShowModal(false)}>✕</button>
          </div>

          <div className="modal-tabs">
            <button className={`modal-tab${modalTab === 'general' ? ' active' : ''}`} onClick={() => setModalTab('general')}>常规</button>
            <button className={`modal-tab${modalTab === 'ssh' ? ' active' : ''}`} onClick={() => setModalTab('ssh')}>SSH</button>
          </div>

          <div className="modal-body">
            {modalTab === 'general' && (<>
              {/* Diagram */}
              <div className="modal-diagram">
                <div className="modal-diagram-node">
                  <span className="modal-diagram-icon">🖥️</span>
                  <span>Navidog</span>
                </div>
                {draft.useSSH && (<>
                  <div className="modal-diagram-line" />
                  <div className="modal-diagram-node">
                    <span className="modal-diagram-icon">🔒</span>
                    <span>SSH</span>
                  </div>
                </>)}
                <div className="modal-diagram-line" />
                <div className="modal-diagram-node">
                  <span className="modal-diagram-icon">🗄️</span>
                  <span>数据库</span>
                </div>
              </div>

              <div className="modal-row">
                <label>连接名</label>
                <input value={draft.label} onChange={e => setDraft(d => ({ ...d, label: e.target.value }))} placeholder="My Connection" />
              </div>
              <div className="modal-row-split">
                <label>主机</label>
                <input value={draft.host} onChange={e => setDraft(d => ({ ...d, host: e.target.value }))} placeholder="127.0.0.1" />
                <label>端口</label>
                <input value={draft.port} onChange={e => setDraft(d => ({ ...d, port: e.target.value }))} placeholder="3306" style={{ width: '100%' }} />
              </div>
              <div className="modal-row">
                <label>用户名</label>
                <input value={draft.user} onChange={e => setDraft(d => ({ ...d, user: e.target.value }))} placeholder="root" />
              </div>
              <div className="modal-row">
                <label>密码</label>
                <input type="password" value={draft.password} onChange={e => setDraft(d => ({ ...d, password: e.target.value }))} placeholder="••••••" />
              </div>
            </>)}

            {modalTab === 'ssh' && (<>
              <div className="modal-checkbox">
                <input type="checkbox" checked={draft.useSSH} onChange={e => setDraft(d => ({ ...d, useSSH: e.target.checked }))} />
                <span>使用 SSH 隧道</span>
              </div>

              {draft.useSSH && (<>
                <div className="modal-row-split">
                  <label>主机</label>
                  <input value={draft.sshHost} onChange={e => setDraft(d => ({ ...d, sshHost: e.target.value }))} placeholder="例: 192.168.1.100" />
                  <label>端口</label>
                  <input value={draft.sshPort} onChange={e => setDraft(d => ({ ...d, sshPort: e.target.value }))} placeholder="22" style={{ width: '100%' }} />
                </div>
                <div className="modal-row">
                  <label>用户名</label>
                  <input value={draft.sshUser} onChange={e => setDraft(d => ({ ...d, sshUser: e.target.value }))} placeholder="root" />
                </div>
                <div className="modal-row">
                  <label>验证方法</label>
                  <select value={draft.sshAuthMethod} onChange={e => setDraft(d => ({ ...d, sshAuthMethod: e.target.value as 'password' | 'privateKey' }))}>
                    <option value="privateKey">公钥</option>
                    <option value="password">密码</option>
                  </select>
                </div>
                {draft.sshAuthMethod === 'privateKey' && (<>
                  <div className="modal-row">
                    <label>私钥</label>
                    <input value={draft.sshPrivateKey} onChange={e => setDraft(d => ({ ...d, sshPrivateKey: e.target.value }))} placeholder="~/.ssh/id_rsa" />
                  </div>
                  <div className="modal-row">
                    <label>通行短语</label>
                    <input type="password" value={draft.sshPassphrase} onChange={e => setDraft(d => ({ ...d, sshPassphrase: e.target.value }))} placeholder="可选" />
                  </div>
                </>)}
                {draft.sshAuthMethod === 'password' && (
                  <div className="modal-row">
                    <label>SSH 密码</label>
                    <input type="password" value={draft.sshPassword} onChange={e => setDraft(d => ({ ...d, sshPassword: e.target.value }))} placeholder="••••••" />
                  </div>
                )}
              </>)}
            </>)}

            {/* Saved profiles */}
            {profiles.length > 0 && (
              <div className="profiles-section">
                <h4>已保存的连接 ({profiles.length})</h4>
                {profiles.map(p => (
                  <div className="profile-item" key={p.id} onClick={() => { setDraft(profileToDraft(p)) }}>
                    <div>
                      <div className="profile-name">{p.label}{p.useSSH ? ' 🔒' : ''}</div>
                      <div className="profile-host">{p.user}@{p.host}:{p.port}{p.useSSH ? ` via SSH ${p.sshHost}` : ''}</div>
                    </div>
                    <div className="profile-actions">
                      <button className="btn btn-sm btn-danger" onClick={e => { e.stopPropagation(); handleDeleteProfile(p.id) }}>删除</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="modal-footer">
            <div className="modal-footer-left">
              <button className="btn-link" onClick={openNewConnectionModal}>+ 新建连接配置文件</button>
            </div>
            <div className="modal-footer-right">
              <button className="btn" onClick={() => void handleTestConnection()} disabled={modalBusy}>
                {modalBusy ? '测试中...' : '测试连接'}
              </button>
              <button className="btn" onClick={() => setShowModal(false)}>取消</button>
              <button className="btn" onClick={handleSaveProfile}>保存</button>
              <button className="btn btn-primary" onClick={() => void handleSaveAndConnect()}>
                保存并连接
              </button>
            </div>
          </div>
        </div>
      </div>
    )
  }

  /* ═══════════════════════════════════════════════
     Main render
     ═══════════════════════════════════════════════ */

  // Determine the current "context" for new query button
  const currentConnectionId = selectedNode?.connectionId ?? (liveConnections.size > 0 ? Array.from(liveConnections.keys())[0] : null)
  const connectingProfileLabels = profiles
    .filter(profile => connectingProfiles.has(profile.id))
    .map(profile => profile.label)
  const currentSchemaName = (() => {
    if (selectedNode?.kind === 'database' || selectedNode?.kind === 'table') return selectedNode.schemaName
    if (currentConnectionId) {
      const conn = liveConnections.get(currentConnectionId)
      if (conn && conn.schemas.length > 0) return conn.schemas[0].name
    }
    return ''
  })()

  return (
    <div className="app-shell">
      {/* ── Toolbar ──────────────────────────────── */}
      <div className="toolbar">
        <button className="toolbar-btn" onClick={openNewConnectionModal}>
          <span className="tb-icon">🔗</span>
          <span className="tb-label">连接</span>
        </button>
        <div className="toolbar-sep" />
        <button className="toolbar-btn" onClick={() => {
          if (currentConnectionId && currentSchemaName) {
            openQueryTab(currentConnectionId, currentSchemaName)
          } else {
            flash('error', '请先连接数据库')
          }
        }}>
          <span className="tb-icon">📝</span>
          <span className="tb-label">新建查询</span>
        </button>
        <div className="toolbar-sep" />
        <button className="toolbar-btn" onClick={() => ncxFileInputRef.current?.click()}>
          <span className="tb-icon">📥</span>
          <span className="tb-label">导入连接</span>
        </button>
        <button className="toolbar-btn" onClick={handleExportNcx}>
          <span className="tb-icon">📤</span>
          <span className="tb-label">导出连接</span>
        </button>
        <div className="toolbar-sep" />
        <button className="toolbar-btn" onClick={() => {
          // Refresh current connection tree
          if (currentConnectionId) {
            const conn = liveConnections.get(currentConnectionId)
            if (conn) void handleConnect(conn.profile)
          }
        }}>
          <span className="tb-icon">🔄</span>
          <span className="tb-label">刷新</span>
        </button>
        <div className="toolbar-spacer" />
        <button
          className={`toolbar-btn${showSidebar ? ' active' : ''}`}
          onClick={() => setShowSidebar(v => !v)}
          title={showSidebar ? '隐藏左侧栏' : '显示左侧栏'}
        >
          <span className="tb-icon">◧</span>
        </button>
        <button
          className={`toolbar-btn${showInfoPanel ? ' active' : ''}`}
          onClick={() => setShowInfoPanel(v => !v)}
          title={showInfoPanel ? '隐藏右侧栏' : '显示右侧栏'}
        >
          <span className="tb-icon">◨</span>
        </button>
        <span style={{ fontSize: 11, color: '#999', marginLeft: 8, marginRight: 8 }}>NaviDog 🐕</span>
      </div>

      {/* ── Workspace ────────────────────────────── */}
      <div className="workspace">
        {/* ── Sidebar ──────────────────────────────── */}
        {showSidebar && <>
        <div className="sidebar">
          <SearchWithHistory
            wrapperClassName="sidebar-search-wrap"
            className="sidebar-search"
            placeholder="🔍 搜索"
            value={treeFilter}
            onChange={setTreeFilter}
            history={searchHistory.tree}
            onCommit={v => setSearchHistory(prev => ({ ...prev, tree: pushRecentSearch(prev.tree, v) }))}
          />
          <div className="tree-container">
            {profiles.length === 0 ? (
              <div style={{ padding: '20px 12px', color: '#aaa', fontSize: 12, textAlign: 'center' }}>
                点击工具栏"连接"创建<br />你的第一个连接
              </div>
            ) : (
              renderTree()
            )}
          </div>
          <div style={{ borderTop: '1px solid #e6e6e6', padding: '4px 8px', fontSize: 11, color: '#999' }}>
            {profiles.length} 个连接，{liveConnections.size} 个活跃{connectingProfiles.size > 0 ? `，${connectingProfiles.size} 个连接中` : ''}
          </div>
        </div>

        {/* ── Sidebar resize handle ───────────────── */}
        <div
          className="panel-resize-handle vertical"
          onMouseDown={e => {
            e.preventDefault()
            const sidebar = e.currentTarget.previousElementSibling as HTMLElement
            if (!sidebar) return
            const startX = e.clientX
            const startW = sidebar.offsetWidth
            const widthUpdater = createRafUpdater<number>((width) => {
              sidebar.style.width = `${width}px`
            })
            function onMove(ev: MouseEvent) {
              const w = Math.max(140, Math.min(startW + ev.clientX - startX, 500))
              widthUpdater.schedule(w)
            }
            function onUp() {
              widthUpdater.flush()
              document.removeEventListener('mousemove', onMove)
              document.removeEventListener('mouseup', onUp)
              document.body.style.userSelect = ''
              document.body.style.cursor = ''
            }
            document.body.style.userSelect = 'none'
            document.body.style.cursor = 'col-resize'
            document.addEventListener('mousemove', onMove)
            document.addEventListener('mouseup', onUp)
          }}
        />
        </>}

        {/* ── Main area ────────────────────────────── */}
        <div className="main-area">
          {/* Tab bar */}
          <div className="tab-bar">
            {tabs.map(tab => (
              <div
                key={tab.id}
                className={`tab-item${tab.id === activeTabId ? ' active' : ''}`}
                onClick={() => setActiveTabId(tab.id)}
                onContextMenu={e => {
                  showContextMenu(e, [
                    { icon: '✕', label: '关闭', action: () => { closeContextMenu(); closeTab(tab.id) } },
                    { icon: '✕', label: '关闭其他选项卡', action: () => {
                      closeContextMenu()
                      setTabs(prev => prev.filter(t => t.id === tab.id))
                      setActiveTabId(tab.id)
                    } },
                    { icon: '✕', label: '关闭右侧的选项卡', action: () => {
                      closeContextMenu()
                      const idx = tabs.findIndex(t => t.id === tab.id)
                      setTabs(prev => prev.filter((_, i) => i <= idx))
                      if (activeTabId) {
                        const activeIdx = tabs.findIndex(t => t.id === activeTabId)
                        if (activeIdx > idx) setActiveTabId(tab.id)
                      }
                    } },
                    { icon: '✕', label: '全部关闭', action: () => { closeContextMenu(); setTabs([]); setActiveTabId(null) } },
                  ])
                }}
              >
                <span className="tab-icon">
                  {tab.kind === 'objects'
                    ? '📋'
                    : tab.kind === 'data'
                      ? '📊'
                      : tab.kind === 'cli'
                        ? '💻'
                        : '📝'}
                </span>
                <span className="tab-title">{tab.title}</span>
                <button className="tab-close" onClick={e => { e.stopPropagation(); closeTab(tab.id) }}>✕</button>
              </div>
            ))}
          </div>

          {/* Tab content */}
          <div className="tab-content">
            {renderTabContent()}
          </div>
        </div>

        {showInfoPanel && <>
        {/* ── Info panel resize handle ────────────── */}
        <div
          className="panel-resize-handle vertical"
          onMouseDown={e => {
            e.preventDefault()
            const infoPanel = e.currentTarget.nextElementSibling as HTMLElement
            if (!infoPanel) return
            const startX = e.clientX
            const startW = infoPanel.offsetWidth
            const widthUpdater = createRafUpdater<number>((width) => {
              infoPanel.style.width = `${width}px`
            })
            function onMove(ev: MouseEvent) {
              const w = Math.max(180, Math.min(startW - (ev.clientX - startX), 600))
              widthUpdater.schedule(w)
            }
            function onUp() {
              widthUpdater.flush()
              document.removeEventListener('mousemove', onMove)
              document.removeEventListener('mouseup', onUp)
              document.body.style.userSelect = ''
              document.body.style.cursor = ''
            }
            document.body.style.userSelect = 'none'
            document.body.style.cursor = 'col-resize'
            document.addEventListener('mousemove', onMove)
            document.addEventListener('mouseup', onUp)
          }}
        />

        {/* ── Info panel ───────────────────────────── */}
        <div className="info-panel">
          {renderInfoPanel()}
        </div>
        </>}
      </div>

      {/* ── Status bar ───────────────────────────── */}
      <div className="status-bar">
        <span>
          <span className={`status-dot ${connectingProfiles.size > 0 ? 'connecting' : liveConnections.size > 0 ? 'online' : 'offline'}`} />
          {connectingProfiles.size > 0 ? '正在连接' : liveConnections.size > 0 ? '已连接' : '未连接'}
        </span>
        {connectingProfileLabels.length > 0 && (
          <span className="status-pill">
            {connectingProfileLabels.length === 1
              ? `正在连接 ${connectingProfileLabels[0]}...`
              : `${connectingProfileLabels.length} 个连接正在建立`}
          </span>
        )}
        <span>{profiles.length} 个连接</span>
        {activeTab?.kind === 'data' && (
          <span>{(activeTab as DataTab).rows.length} 条记录</span>
        )}
      </div>

      {/* ── Context menu ────────────────────────── */}
      {ctxMenu && (
        <>
          <div className="context-menu-overlay" onClick={closeContextMenu} onContextMenu={e => { e.preventDefault(); closeContextMenu() }} />
          <div ref={ctxMenuRef} className="context-menu" style={{ left: ctxMenu.x, top: ctxMenu.y }}>
            {ctxMenu.items.map((item, i) =>
              item === 'separator' ? (
                <div key={`sep-${i}`} className="ctx-sep" />
              ) : (
                <div
                  key={`${item.label}-${i}`}
                  className={`ctx-item${item.danger ? ' danger' : ''}${item.disabled ? ' disabled' : ''}${item.children ? ' has-submenu' : ''}`}
                  onClick={() => { if (!item.disabled && !item.children) item.action() }}
                >
                  <span className="ctx-icon">{item.icon}</span>
                  <span className="ctx-label">{item.label}</span>
                  {item.shortcut && !item.children && <span className="ctx-shortcut">{item.shortcut}</span>}
                  {item.children && (
                    <>
                      <span className="ctx-shortcut">▸</span>
                      <div className="ctx-submenu">
                        {item.children.map((sub, j) =>
                          sub === 'separator' ? (
                            <div key={`ssep-${j}`} className="ctx-sep" />
                          ) : (
                            <div
                              key={`${sub.label}-${j}`}
                              className={`ctx-item${sub.disabled ? ' disabled' : ''}`}
                              onClick={() => { if (!sub.disabled) sub.action() }}
                            >
                              <span className="ctx-icon">{sub.icon}</span>
                              <span className="ctx-label">{sub.label}</span>
                              {sub.shortcut && <span className="ctx-shortcut">{sub.shortcut}</span>}
                            </div>
                          )
                        )}
                      </div>
                    </>
                  )}
                </div>
              )
            )}
          </div>
        </>
      )}

      {/* ── Connection Modal ─────────────────────── */}
      {renderConnectionModal()}

      {/* ── Hidden file input for NCX import ────── */}
      <input
        ref={ncxFileInputRef}
        type="file"
        accept=".ncx"
        style={{ display: 'none' }}
        onChange={handleImportNcxFile}
      />

      {/* ── Import Wizard ─────────────────────────── */}
      {importWizard && (() => {
        const conn = liveConnections.get(importWizard.connectionId)
        if (!conn) return null
        return (
          <ImportWizard
            connection={conn.profile}
            schemas={conn.schemas}
            initialSchema={importWizard.schemaName}
            initialTable={importWizard.tableName}
            onClose={() => setImportWizard(null)}
            onFlash={flash}
          />
        )
      })()}

      {/* ── Export Wizard ─────────────────────────── */}
      {exportWizard && (() => {
        const conn = liveConnections.get(exportWizard.connectionId)
        if (!conn) return null
        return (
          <ExportWizard
            connection={conn.profile}
            schemas={conn.schemas}
            initialSchema={exportWizard.schemaName}
            initialTable={exportWizard.tableName}
            onClose={() => setExportWizard(null)}
            onFlash={flash}
          />
        )
      })()}

      {/* ── Notice toast ─────────────────────────── */}
      {notice && (
        <div className={`notice-toast ${notice.tone}`}>
          {notice.tone === 'success' ? '✅ ' : '❌ '}{notice.message}
        </div>
      )}
    </div>
  )
}
