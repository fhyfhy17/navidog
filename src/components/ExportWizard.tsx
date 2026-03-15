import { useEffect, useRef, useState } from 'react'
import { api } from '../api'
import {
  buildBatchSelectQuery,
  getPrimaryKeyColumns,
  readPrimaryKeyCursor,
} from '../sqlPaging'
import type { ConnectionProfile, SchemaNode, TableColumn } from '../types'

/* ── Types ───────────────────────────────────── */

type ExportFormat = 'csv' | 'txt' | 'xlsx' | 'json' | 'sql'

const FORMAT_EXT: Record<ExportFormat, string> = {
  csv: 'csv',
  txt: 'txt',
  xlsx: 'xlsx',
  json: 'json',
  sql: 'sql',
}

type TableExportConfig = {
  name: string
  enabled: boolean
  fileName: string
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

type TextExportTarget = {
  kind: 'download' | 'file'
  write: (chunk: string) => Promise<void>
  close: () => Promise<Blob | null>
}

function escapeCsvValue(
  value: unknown,
  separator: string,
  qualifier: string,
) {
  if (value === null || value === undefined) return ''
  const text = String(value)

  if (!qualifier) {
    return text
  }

  if (
    text.includes(separator) ||
    text.includes(qualifier) ||
    text.includes('\n') ||
    text.includes('\r')
  ) {
    const escapedQualifier = qualifier.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    return `${qualifier}${text.replace(new RegExp(escapedQualifier, 'g'), qualifier + qualifier)}${qualifier}`
  }

  return text
}

function toSqlLiteral(value: unknown) {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'number' || typeof value === 'bigint') return String(value)
  if (typeof value === 'boolean') return value ? '1' : '0'

  return `'${String(value)
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t')}'`
}

async function createTextExportTarget(
  fileName: string,
  mimeType: string,
  useFileSystemAccess: boolean,
): Promise<TextExportTarget> {
  if (useFileSystemAccess) {
    const pickerWindow = window as SavePickerWindow
    const handle = await pickerWindow.showSaveFilePicker?.({
      suggestedName: fileName,
      types: [{
        description: 'Export Files',
        accept: { [mimeType]: [`.${fileName.split('.').pop() ?? 'txt'}`] },
      }],
    })

    if (handle) {
      const writable = await handle.createWritable()
      return {
        kind: 'file',
        async write(chunk) {
          await writable.write(chunk)
        },
        async close() {
          await writable.close()
          return null
        },
      }
    }
  }

  const parts: BlobPart[] = []
  return {
    kind: 'download',
    async write(chunk) {
      parts.push(chunk)
    },
    async close() {
      return new Blob(parts, { type: mimeType })
    },
  }
}

type Props = {
  connection: ConnectionProfile
  schemas: SchemaNode[]
  initialSchema?: string
  initialTable?: string
  onClose: () => void
  onFlash: (tone: 'success' | 'error', msg: string) => void
}

/* ── Component ───────────────────────────────── */

export default function ExportWizard({
  connection,
  schemas,
  initialSchema,
  initialTable,
  onClose,
  onFlash,
}: Props) {
  /* Steps: 0=format, 1=tables, 2=columns, 3=options, 4=execute */
  const [step, setStep] = useState(0)

  /* Step 0: Format */
  const [format, setFormat] = useState<ExportFormat>('csv')

  /* Step 1: Tables & files */
  const [selectedSchema, setSelectedSchema] = useState(initialSchema ?? '')
  const [tableConfigs, setTableConfigs] = useState<TableExportConfig[]>([])

  /* Step 2: Columns */
  const [activeTable, setActiveTable] = useState('')
  const [tableColumns, setTableColumns] = useState<Record<string, TableColumn[]>>({})
  const [selectedColumns, setSelectedColumns] = useState<Record<string, Set<string>>>({})
  const [loadingCols, setLoadingCols] = useState(false)
  const [tableSearch, setTableSearch] = useState('')

  /* Step 3: Options */
  const [includeHeader, setIncludeHeader] = useState(true)
  const [delimiter, setDelimiter] = useState(',')
  const [textQualifier, setTextQualifier] = useState('"')
  const [lineSeparator, setLineSeparator] = useState('LF')
  const [encoding] = useState('UTF-8')

  /* Step 4: Execute */
  const [exporting, setExporting] = useState(false)
  const [exportLog, setExportLog] = useState<string[]>([])
  const [progress, setProgress] = useState({ total: 0, processed: 0, tables: 0, tablesTotal: 0 })
  const [exportDone, setExportDone] = useState(false)

  /* ── Init tables when schema changes ───── */
  useEffect(() => {
    if (selectedSchema) {
      const schema = schemas.find((s) => s.name === selectedSchema)
      if (schema) {
        const configs: TableExportConfig[] = schema.tables.map((t) => ({
          name: t.name,
          enabled: initialTable ? t.name === initialTable : true,
          fileName: `${t.name}.${FORMAT_EXT[format]}`,
        }))
        setTableConfigs(configs)
        if (initialTable) {
          setActiveTable(initialTable)
        } else if (configs.length > 0) {
          setActiveTable(configs[0].name)
        }
      }
    }
  }, [selectedSchema, schemas, initialTable, format])

  /* Update file extensions and defaults when format changes */
  useEffect(() => {
    setTableConfigs((prev) =>
      prev.map((tc) => ({
        ...tc,
        fileName: `${tc.name}.${FORMAT_EXT[format]}`,
      })),
    )
    // Auto-set delimiter for TXT (tab) / CSV (comma) consistency
    if (format === 'txt') {
      setDelimiter('\t')
    } else if (format === 'csv') {
      setDelimiter(',')
    }
  }, [format])

  /* ── Load columns for a table ──────────── */
  const loadedColsRef = useRef<Set<string>>(new Set())

  async function loadColumns(tableName: string) {
    if (loadedColsRef.current.has(tableName)) return
    loadedColsRef.current.add(tableName)
    setLoadingCols(true)
    try {
      const res = await api.fetchTableColumns(connection, selectedSchema, tableName)
      setTableColumns((prev) => ({ ...prev, [tableName]: res.columns }))
      setSelectedColumns((prev) => ({
        ...prev,
        [tableName]: new Set(res.columns.map((c) => c.name)),
      }))
    } catch (err) {
      loadedColsRef.current.delete(tableName)
      onFlash('error', `加载列信息失败: ${err instanceof Error ? err.message : String(err)}`)
    } finally {
      setLoadingCols(false)
    }
  }

  /* Auto-load columns when entering step 2 or switching active table */
  useEffect(() => {
    if (step === 2 && activeTable) {
      void loadColumns(activeTable)
    }
    // `loadColumns` is intentionally not included to avoid reloading on every render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [step, activeTable])

  /* ── Column toggle ─────────────────────── */
  function toggleColumn(tableName: string, colName: string) {
    setSelectedColumns((prev) => {
      const current = new Set(prev[tableName] ?? [])
      if (current.has(colName)) {
        current.delete(colName)
      } else {
        current.add(colName)
      }
      return { ...prev, [tableName]: current }
    })
  }

  function toggleAllColumns(tableName: string, selectAll: boolean) {
    const cols = tableColumns[tableName] ?? []
    setSelectedColumns((prev) => ({
      ...prev,
      [tableName]: selectAll ? new Set(cols.map((c) => c.name)) : new Set(),
    }))
  }

  /* ── Execute export ────────────────────── */
  async function executeExport() {
    const enabledTables = tableConfigs.filter((t) => t.enabled)
    if (enabledTables.length === 0) {
      onFlash('error', '没有选中任何表')
      return
    }

    setExporting(true)
    setExportDone(false)
    setExportLog([])
    setProgress({ total: 0, processed: 0, tables: 0, tablesTotal: enabledTables.length })

    // Build a local snapshot of columns + selected columns, loading any missing ones
    const resolvedCols: Record<string, TableColumn[]> = { ...tableColumns }
    const resolvedSelected: Record<string, Set<string>> = { ...selectedColumns }

    for (const t of enabledTables) {
      if (!resolvedCols[t.name]) {
        try {
          const res = await api.fetchTableColumns(connection, selectedSchema, t.name)
          resolvedCols[t.name] = res.columns
          resolvedSelected[t.name] = new Set(res.columns.map((c) => c.name))
          // Also update state for UI consistency
          setTableColumns((prev) => ({ ...prev, [t.name]: res.columns }))
          setSelectedColumns((prev) => ({
            ...prev,
            [t.name]: new Set(res.columns.map((c) => c.name)),
          }))
          loadedColsRef.current.add(t.name)
        } catch {
          // skip this table
        }
      }
    }

    const eol = lineSeparator === 'CRLF' ? '\r\n' : '\n'
    const allFiles: { name: string; blob: Blob }[] = []
    const useFileSystemAccess =
      enabledTables.length === 1 &&
      format !== 'xlsx' &&
      typeof window !== 'undefined' &&
      'showSaveFilePicker' in window
    let totalProcessed = 0
    let completedExports = 0

    for (let ti = 0; ti < enabledTables.length; ti++) {
      const tc = enabledTables[ti]
      const cols = resolvedCols[tc.name] ?? []
      const selCols = resolvedSelected[tc.name] ?? new Set()
      const activeCols = cols.filter((c) => selCols.has(c.name))
      const fileName = tc.fileName.trim() || `${tc.name}.${FORMAT_EXT[format]}`

      if (activeCols.length === 0) {
        setExportLog((prev) => [...prev, `⏭️ 跳过 ${tc.name}（无选中列）`])
        continue
      }

      setExportLog((prev) => [...prev, `📤 正在导出 ${tc.name}...`])

      const BATCH = 5000
      let offset = 0
      let hasMore = true
      let rowCount = 0
      const colNames = activeCols.map((c) => c.name)
      const primaryKeyColumns = getPrimaryKeyColumns(cols)
      let cursor: Record<string, unknown> | null = null
      let blob: Blob | null = null

      try {
        if (format === 'xlsx') {
          const XLSX = await import('xlsx')
          const MAX_XLSX_ROWS = 500000
          const wsData: unknown[][] = []
          if (includeHeader) {
            wsData.push(colNames)
          }
          setExportLog((prev) => [...prev, `⚠️ ${tc.name}: xlsx 导出会在浏览器内存中组装文件，大表建议改用 CSV / SQL`])

          while (hasMore) {
            const { mode, sql } = buildBatchSelectQuery({
              schemaName: selectedSchema,
              tableName: tc.name,
              selectColumns: colNames,
              primaryKeyColumns,
              batchSize: BATCH,
              fallbackOffset: offset,
              cursor,
            })
            const result = await api.runQuery(connection, sql, selectedSchema)
            const dataResult = result.results?.[0]

            if (dataResult?.kind === 'rows' && dataResult.rows.length > 0) {
              for (const row of dataResult.rows) {
                wsData.push(colNames.map((c) => row[c] ?? ''))
              }
              rowCount += dataResult.rows.length
              if (mode === 'primaryKey') {
                cursor = readPrimaryKeyCursor(dataResult.rows[dataResult.rows.length - 1], primaryKeyColumns)
              } else {
                offset += dataResult.rows.length
              }
              totalProcessed += dataResult.rows.length
              setProgress((prev) => ({ ...prev, processed: totalProcessed, tables: ti }))

              if (dataResult.rows.length < BATCH) {
                hasMore = false
              }
              if (rowCount >= MAX_XLSX_ROWS) {
                setExportLog((prev) => [...prev, `⚠️ ${tc.name}: xlsx 达到 ${MAX_XLSX_ROWS.toLocaleString()} 行限制`])
                hasMore = false
              }
            } else {
              hasMore = false
            }
          }

          const ws = XLSX.utils.aoa_to_sheet(wsData)
          const wb = XLSX.utils.book_new()
          XLSX.utils.book_append_sheet(wb, ws, tc.name.slice(0, 31))
          const xlsxData = XLSX.write(wb, { bookType: 'xlsx', type: 'array' }) as ArrayBuffer
          blob = new Blob([xlsxData], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
        } else {
          const mimeType = format === 'json'
            ? 'application/json;charset=utf-8'
            : 'text/plain;charset=utf-8'
          const target = await createTextExportTarget(fileName, mimeType, useFileSystemAccess)
          let firstJsonRow = true

          if (format === 'csv' || format === 'txt') {
            if (includeHeader) {
              await target.write(colNames.map((c) => escapeCsvValue(c, delimiter, textQualifier)).join(delimiter) + eol)
            }
          } else if (format === 'json') {
            await target.write('[')
          } else {
            await target.write(`-- Export of ${tc.name}${eol}-- Date: ${new Date().toISOString()}${eol}${eol}`)
          }

          while (hasMore) {
            const { mode, sql } = buildBatchSelectQuery({
              schemaName: selectedSchema,
              tableName: tc.name,
              selectColumns: colNames,
              primaryKeyColumns,
              batchSize: BATCH,
              fallbackOffset: offset,
              cursor,
            })
            const result = await api.runQuery(connection, sql, selectedSchema)
            const dataResult = result.results?.[0]

            if (dataResult?.kind === 'rows' && dataResult.rows.length > 0) {
              rowCount += dataResult.rows.length
              if (mode === 'primaryKey') {
                cursor = readPrimaryKeyCursor(dataResult.rows[dataResult.rows.length - 1], primaryKeyColumns)
              } else {
                offset += dataResult.rows.length
              }
              totalProcessed += dataResult.rows.length
              setProgress((prev) => ({ ...prev, processed: totalProcessed, tables: ti }))

              let chunk = ''
              if (format === 'csv' || format === 'txt') {
                chunk = dataResult.rows.map((row) =>
                  colNames
                    .map((columnName) => escapeCsvValue(row[columnName], delimiter, textQualifier))
                    .join(delimiter),
                ).join(eol)
                if (chunk) {
                  chunk += eol
                }
              } else if (format === 'json') {
                chunk = dataResult.rows.map((row) => {
                  const obj: Record<string, unknown> = {}
                  for (const columnName of colNames) {
                    obj[columnName] = row[columnName] ?? null
                  }
                  const prefix = firstJsonRow ? `${eol}  ` : `,${eol}  `
                  firstJsonRow = false
                  return prefix + JSON.stringify(obj)
                }).join('')
              } else {
                chunk = dataResult.rows.map((row) =>
                  `INSERT INTO \`${tc.name}\` (${colNames.map((columnName) => `\`${columnName}\``).join(', ')}) VALUES (${colNames.map((columnName) => toSqlLiteral(row[columnName])).join(', ')});`,
                ).join(eol)
                if (chunk) {
                  chunk += eol
                }
              }

              if (chunk) {
                await target.write(chunk)
              }

              if (dataResult.rows.length < BATCH) {
                hasMore = false
              }
            } else {
              hasMore = false
            }
          }

          if (format === 'json') {
            await target.write((firstJsonRow ? '' : eol) + ']' + eol)
          }

          blob = await target.close()
        }
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') {
          setExportLog((prev) => [...prev, '⏹️ 已取消导出'])
          setExporting(false)
          return
        }

        setExportLog((prev) => [...prev, `❌ ${tc.name}: ${err instanceof Error ? err.message : String(err)}`])
        continue
      }

      if (blob) {
        allFiles.push({ name: fileName, blob })
      }
      completedExports += 1
      setExportLog((prev) => [...prev, `✅ ${tc.name}: ${rowCount.toLocaleString()} 行`])
      setProgress((prev) => ({ ...prev, total: prev.total + rowCount, tables: ti + 1 }))
    }

    // Download files
    for (const file of allFiles) {
      triggerDownloadBlob(file.name, file.blob)
      if (allFiles.length > 1) {
        await new Promise((r) => setTimeout(r, 300))
      }
    }

    setExportDone(true)
    setExporting(false)
    onFlash('success', `导出完成: ${completedExports} 个文件`)
  }

  function triggerDownloadBlob(fileName: string, blob: Blob) {
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = fileName
    a.click()
    URL.revokeObjectURL(url)
  }

  /* ── Enabled table count ───────────────── */
  const enabledCount = tableConfigs.filter((t) => t.enabled).length

  /* ── Render steps ──────────────────────── */

  function renderStep0() {
    const formats: { value: ExportFormat; label: string; desc: string }[] = [
      { value: 'csv', label: 'CSV 文件 (*.csv)', desc: '逗号分隔值，通用格式' },
      { value: 'txt', label: 'TXT 文件 (*.txt)', desc: '制表符分隔文本' },
      { value: 'xlsx', label: 'Excel 文件 (*.xlsx)', desc: 'Microsoft Excel 工作簿' },
      { value: 'json', label: 'JSON 文件 (*.json)', desc: 'JavaScript 对象表示' },
      { value: 'sql', label: 'SQL 脚本文件 (*.sql)', desc: 'INSERT INTO 语句' },
    ]

    return (
      <div className="export-step">
        <div className="export-step-title">选择导出格式</div>
        <div className="export-step-desc">向导可以让你指定导出数据的细节。你要使用哪一种导出格式？</div>

        <div className="export-format-list">
          {formats.map((f) => (
            <label
              key={f.value}
              className={`export-format-item ${format === f.value ? 'selected' : ''}`}
            >
              <input
                type="radio"
                name="format"
                checked={format === f.value}
                onChange={() => setFormat(f.value)}
              />
              <div className="export-format-info">
                <div className="export-format-label">{f.label}</div>
                <div className="export-format-desc">{f.desc}</div>
              </div>
            </label>
          ))}
        </div>
      </div>
    )
  }

  function renderStep1() {
    const keyword = tableSearch.toLowerCase()
    const filteredConfigs = keyword
      ? tableConfigs.filter((tc) => tc.name.toLowerCase().includes(keyword))
      : tableConfigs
    const filteredEnabled = filteredConfigs.filter((t) => t.enabled).length

    return (
      <div className="export-step">
        <div className="export-step-title">选择表和导出文件</div>
        <div className="export-step-desc">你可以选择导出文件并定义一些附加选项。</div>

        <div className="export-form-row">
          <label>数据库：</label>
          <select
            value={selectedSchema}
            onChange={(e) => setSelectedSchema(e.target.value)}
          >
            <option value="">-- 选择数据库 --</option>
            {schemas.map((s) => (
              <option key={s.name} value={s.name}>{s.name}</option>
            ))}
          </select>
        </div>

        {tableConfigs.length > 0 && (
          <>
            <div className="export-table-search-row">
              <input
                type="text"
                className="export-table-search"
                placeholder="🔍 搜索表名..."
                value={tableSearch}
                onChange={(e) => setTableSearch(e.target.value)}
              />
              {tableSearch && (
                <button
                  className="export-table-search-clear"
                  onClick={() => setTableSearch('')}
                >✕</button>
              )}
            </div>
            <div className="export-table-list-header">
              <label>
                <input
                  type="checkbox"
                  checked={filteredEnabled === filteredConfigs.length && filteredConfigs.length > 0}
                  onChange={(e) => {
                    const names = new Set(filteredConfigs.map((t) => t.name))
                    setTableConfigs((prev) =>
                      prev.map((tc) => names.has(tc.name) ? { ...tc, enabled: e.target.checked } : tc),
                    )
                  }}
                />
                {' '}{keyword ? '选中筛选结果' : '全选'} ({enabledCount}/{tableConfigs.length})
                {keyword && <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>显示 {filteredConfigs.length} 个</span>}
              </label>
            </div>
            <div className="export-table-list">
              <div className="export-table-list-row export-table-list-head">
                <div style={{ width: 30 }}></div>
                <div style={{ flex: 1 }}>表名</div>
                <div style={{ flex: 1 }}>导出文件名</div>
              </div>
              {filteredConfigs.map((tc) => {
                const i = tableConfigs.findIndex((t) => t.name === tc.name)
                return (
                  <div key={tc.name} className={`export-table-list-row ${tc.enabled ? '' : 'disabled'}`}>
                    <div style={{ width: 30 }}>
                      <input
                        type="checkbox"
                        checked={tc.enabled}
                        onChange={(e) => {
                          setTableConfigs((prev) => {
                            const next = [...prev]
                            next[i] = { ...next[i], enabled: e.target.checked }
                            return next
                          })
                        }}
                      />
                    </div>
                    <div style={{ flex: 1 }} className="export-table-name">
                      <span className="export-table-icon">📊</span>
                      {tc.name}
                    </div>
                    <div style={{ flex: 1 }}>
                      <input
                        type="text"
                        className="export-filename-input"
                        value={tc.fileName}
                        onChange={(e) => {
                          setTableConfigs((prev) => {
                            const next = [...prev]
                            next[i] = { ...next[i], fileName: e.target.value }
                            return next
                          })
                        }}
                      />
                    </div>
                  </div>
                )
              })}
            </div>
          </>
        )}
      </div>
    )
  }

  function renderStep2() {
    const enabledTables = tableConfigs.filter((t) => t.enabled)
    const currentCols = tableColumns[activeTable] ?? []
    const currentSelected = selectedColumns[activeTable] ?? new Set()
    const allSelected = currentCols.length > 0 && currentCols.every((c) => currentSelected.has(c.name))

    return (
      <div className="export-step">
        <div className="export-step-title">选择导出列</div>
        <div className="export-step-desc">你可以选择导出哪些列。</div>

        <div className="export-form-row">
          <label>源表：</label>
          <select
            value={activeTable}
            onChange={(e) => setActiveTable(e.target.value)}
          >
            {enabledTables.map((t) => (
              <option key={t.name} value={t.name}>{t.name}</option>
            ))}
          </select>
        </div>

        {loadingCols ? (
          <div className="export-loading">加载列信息中...</div>
        ) : (
          <>
            <div className="export-col-list">
              {currentCols.map((col) => (
                <label key={col.name} className="export-col-item">
                  <input
                    type="checkbox"
                    checked={currentSelected.has(col.name)}
                    onChange={() => toggleColumn(activeTable, col.name)}
                  />
                  <span className="export-col-name">{col.name}</span>
                  <span className="export-col-type">{col.type}</span>
                  {col.key === 'PRI' && <span className="export-col-key">🔑</span>}
                </label>
              ))}
            </div>
            <div className="export-col-actions">
              <button onClick={() => toggleAllColumns(activeTable, true)}>全选</button>
              <button onClick={() => toggleAllColumns(activeTable, false)}>取消全选</button>
              <label>
                <input
                  type="checkbox"
                  checked={allSelected}
                  onChange={(e) => toggleAllColumns(activeTable, e.target.checked)}
                />
                {' '}全部字段
              </label>
            </div>
          </>
        )}
      </div>
    )
  }

  function renderStep3() {
    return (
      <div className="export-step">
        <div className="export-step-title">附加选项</div>
        <div className="export-step-desc">你可以定义一些附加的选项。</div>

        {(format === 'csv' || format === 'txt') && (
          <>
            <div className="export-options-section">
              <div className="export-options-title">文件格式</div>
              <div className="export-options-grid">
                <label>
                  <input
                    type="checkbox"
                    checked={includeHeader}
                    onChange={(e) => setIncludeHeader(e.target.checked)}
                  />
                  {' '}包含列的标题
                </label>

                <div className="export-option-row">
                  <span>记录分隔符：</span>
                  <select value={lineSeparator} onChange={(e) => setLineSeparator(e.target.value)}>
                    <option value="LF">LF (Unix)</option>
                    <option value="CRLF">CRLF (Windows)</option>
                  </select>
                </div>

                <div className="export-option-row">
                  <span>字段分隔符：</span>
                  <select value={delimiter} onChange={(e) => setDelimiter(e.target.value)}>
                    <option value=",">逗号 (,)</option>
                    <option value={'\t'}>制表符</option>
                    <option value=";">分号 (;)</option>
                    <option value="|">竖线 (|)</option>
                  </select>
                </div>

                <div className="export-option-row">
                  <span>文本识别符号：</span>
                  <select value={textQualifier} onChange={(e) => setTextQualifier(e.target.value)}>
                    <option value='"'>双引号 (")</option>
                    <option value="'">单引号 (')</option>
                    <option value="">无</option>
                  </select>
                </div>
              </div>
            </div>

            <div className="export-options-section">
              <div className="export-options-title">编码</div>
              <div className="export-option-row">
                <span>文件编码：</span>
                <span style={{ color: 'var(--text-secondary)' }}>{encoding}</span>
              </div>
            </div>
          </>
        )}

        {format === 'xlsx' && (
          <div className="export-options-section">
            <div className="export-options-title">Excel 选项</div>
            <div className="export-options-grid">
              <label>
                <input
                  type="checkbox"
                  checked={includeHeader}
                  onChange={(e) => setIncludeHeader(e.target.checked)}
                />
                {' '}包含列标题作为第一行
              </label>
            </div>
          </div>
        )}

        {format === 'json' && (
          <div className="export-options-section">
            <div className="export-options-title">JSON 选项</div>
            <div className="export-option-row">
              <span>格式：</span>
              <span style={{ color: 'var(--text-secondary)' }}>格式化 JSON（含缩进）</span>
            </div>
          </div>
        )}

        {format === 'sql' && (
          <div className="export-options-section">
            <div className="export-options-title">SQL 选项</div>
            <div className="export-option-row">
              <span>语句类型：</span>
              <span style={{ color: 'var(--text-secondary)' }}>INSERT INTO</span>
            </div>
          </div>
        )}
      </div>
    )
  }

  function renderStep4() {
    const pct = progress.tablesTotal > 0
      ? Math.round((progress.tables / progress.tablesTotal) * 100)
      : 0

    return (
      <div className="export-step">
        <div className="export-step-title">
          {exportDone ? '导出完成' : '正在导出...'}
        </div>
        <div className="export-step-desc">
          {exportDone
            ? '我们已收集向导导出数据时所需的全部信息。'
            : '点击 [开始] 按钮开始导出。'}
        </div>

        <div className="export-stats-row">
          <div className="export-stat-item">
            <span className="export-stat-label">源对象：</span>
            <span>{progress.tablesTotal}</span>
          </div>
          <div className="export-stat-item">
            <span className="export-stat-label">总计：</span>
            <span>{progress.total.toLocaleString()}</span>
          </div>
          <div className="export-stat-item">
            <span className="export-stat-label">已处理：</span>
            <span>{progress.tables} / {progress.tablesTotal} 表</span>
          </div>
        </div>

        <div className="export-log-box">
          {exportLog.map((line, i) => (
            <div key={i} className="export-log-line">{line}</div>
          ))}
          {exportLog.length === 0 && (
            <div className="export-log-line" style={{ opacity: 0.4 }}>等待开始...</div>
          )}
        </div>

        <div className="import-progress-bar-wrap">
          <div className="import-progress-bar" style={{ width: `${pct}%` }} />
        </div>
      </div>
    )
  }

  /* ── Step nav ──────────────────────────── */
  const canNext = (() => {
    switch (step) {
      case 0: return true
      case 1: return !!selectedSchema && enabledCount > 0
      case 2: return Object.values(selectedColumns).some((s) => s.size > 0)
      case 3: return true
      default: return false
    }
  })()

  async function handleNext() {
    if (step === 0) {
      if (initialTable && selectedSchema) {
        // Table pre-selected, skip to columns
        setStep(2)
        return
      }
      setStep(1)
      return
    }
    if (step === 1) {
      // Set activeTable to first enabled table if needed
      const enabledTables = tableConfigs.filter((t) => t.enabled)
      if (enabledTables.length > 0) {
        const currentActive = enabledTables.find((t) => t.name === activeTable)
        if (!currentActive) {
          setActiveTable(enabledTables[0].name)
        }
      }
      setStep(2)
      return
    }
    if (step === 2) {
      setStep(3)
      return
    }
    if (step === 3) {
      setStep(4)
      return
    }
  }

  function handleBack() {
    if (step === 2 && initialTable) {
      setStep(0)
      return
    }
    setStep((s) => Math.max(0, s - 1))
  }

  const stepLabels = ['导出格式', '选择表', '选择列', '附加选项', '执行导出']

  return (
    <div className="import-wizard-overlay" onClick={onClose}>
      <div className="import-wizard-modal" onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="import-wizard-header">
          <div className="import-wizard-title">📤 导出向导</div>
          <button className="import-close-btn" onClick={onClose} disabled={exporting}>✕</button>
        </div>

        {/* Step indicator */}
        <div className="import-step-indicator">
          {stepLabels.map((label, i) => (
            <div
              key={i}
              className={`import-step-dot ${i === step ? 'active' : ''} ${i < step ? 'done' : ''}`}
            >
              <div className="import-step-dot-circle">
                {i < step ? '✓' : i + 1}
              </div>
              <div className="import-step-dot-label">{label}</div>
            </div>
          ))}
        </div>

        {/* Content */}
        <div className="import-wizard-body">
          {step === 0 && renderStep0()}
          {step === 1 && renderStep1()}
          {step === 2 && renderStep2()}
          {step === 3 && renderStep3()}
          {step === 4 && renderStep4()}
        </div>

        {/* Footer */}
        <div className="import-wizard-footer">
          {step > 0 && step < 4 && (
            <button className="import-btn import-btn-secondary" onClick={handleBack}>
              ← 上一步
            </button>
          )}
          {step === 4 && !exportDone && (
            <button className="import-btn import-btn-secondary" onClick={handleBack} disabled={exporting}>
              ← 上一步
            </button>
          )}
          <div style={{ flex: 1 }} />
          {step < 4 && (
            <button
              className="import-btn import-btn-primary"
              disabled={!canNext}
              onClick={() => void handleNext()}
            >
              下一步 →
            </button>
          )}
          {step === 4 && !exportDone && (
            <button
              className="import-btn import-btn-primary"
              style={{ background: '#1e8e3e' }}
              disabled={exporting}
              onClick={() => void executeExport()}
            >
              {exporting ? '导出中...' : '开始'}
            </button>
          )}
          {step === 4 && exportDone && (
            <button className="import-btn import-btn-primary" onClick={onClose}>
              完成
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
