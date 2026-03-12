import { useEffect, useRef, useState } from 'react'
import Papa from 'papaparse'
import * as XLSX from 'xlsx'
import { api } from '../api'
import type { ConnectionProfile, ImportCreateTablePlan, SchemaNode, TableColumn } from '../types'

/* ── Types ───────────────────────────────────── */

type ImportMode = 'append' | 'replace' | 'upsert'

type ColumnMapping = {
  source: string
  target: string
}

type ImportStats = {
  processed: number
  inserted: number
  errors: string[]
  totalBatches: number
  completedBatches: number
  done: boolean
}

type Props = {
  connection: ConnectionProfile
  schemas: SchemaNode[]
  /** Pre-selected target (from table context menu) */
  initialSchema?: string
  initialTable?: string
  onClose: () => void
  onFlash: (tone: 'success' | 'error', msg: string) => void
}

/* ── Helpers ─────────────────────────────────── */

function autoMapColumns(
  sourceFields: string[],
  targetColumns: TableColumn[],
): ColumnMapping[] {
  return sourceFields.map((src) => {
    const exact = targetColumns.find((t) => t.name === src)
    if (exact) return { source: src, target: exact.name }
    const lower = targetColumns.find(
      (t) => t.name.toLowerCase() === src.toLowerCase(),
    )
    if (lower) return { source: src, target: lower.name }
    return { source: src, target: '' }
  })
}

function normalizeGeneratedColumnName(rawName: string, index: number, usedNames: Set<string>) {
  const base = rawName.replaceAll('`', '').trim() || `column_${index + 1}`
  let name = base
  let suffix = 2

  while (usedNames.has(name)) {
    name = `${base}_${suffix}`
    suffix += 1
  }

  usedNames.add(name)
  return name
}

function stringifyImportValue(value: unknown) {
  if (value === null || value === undefined) return ''
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value)
    } catch {
      return String(value)
    }
  }
  return String(value)
}

function isIntegerLike(value: unknown) {
  return typeof value === 'number' && Number.isInteger(value)
}

function isNumberLike(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value)
}

function isBooleanLike(value: unknown) {
  return typeof value === 'boolean'
}

function isJsonLike(value: unknown) {
  return typeof value === 'object' && value !== null
}

function isDateLike(value: unknown) {
  if (typeof value !== 'string') return false
  return /^\d{4}-\d{2}-\d{2}$/.test(value.trim())
}

function isDateTimeLike(value: unknown) {
  if (typeof value !== 'string') return false
  return /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?$/.test(value.trim())
}

function inferStringColumnType(values: unknown[]) {
  const maxLength = values.reduce<number>((max, value) => Math.max(max, stringifyImportValue(value).length), 0)

  if (maxLength <= 32) return 'VARCHAR(32)'
  if (maxLength <= 64) return 'VARCHAR(64)'
  if (maxLength <= 128) return 'VARCHAR(128)'
  if (maxLength <= 255) return 'VARCHAR(255)'
  if (maxLength <= 65535) return 'TEXT'
  return 'LONGTEXT'
}

function inferCreateTablePlan(
  sourceFields: string[],
  rows: Record<string, unknown>[],
): ImportCreateTablePlan {
  const usedNames = new Set<string>()
  const inferredColumns = sourceFields.map((field, index) => {
    const columnName = normalizeGeneratedColumnName(field, index, usedNames)
    const values = rows.map((row) => row[field])
    const nonEmptyValues = values.filter((value) => value !== null && value !== undefined && value !== '')

    let type = 'VARCHAR(255)'
    if (nonEmptyValues.length === 0) {
      type = 'VARCHAR(255)'
    } else if (nonEmptyValues.every(isBooleanLike)) {
      type = 'TINYINT(1)'
    } else if (nonEmptyValues.every(isIntegerLike)) {
      const maxAbs = nonEmptyValues.reduce<number>((max, value) => Math.max(max, Math.abs(Number(value))), 0)
      type = maxAbs > 2147483647 ? 'BIGINT' : 'INT'
    } else if (nonEmptyValues.every(isNumberLike)) {
      type = 'DOUBLE'
    } else if (nonEmptyValues.every(isDateLike)) {
      type = 'DATE'
    } else if (nonEmptyValues.every(isDateTimeLike)) {
      type = 'DATETIME'
    } else if (nonEmptyValues.every(isJsonLike)) {
      type = 'JSON'
    } else {
      type = inferStringColumnType(nonEmptyValues)
    }

    return {
      sourceField: field,
      column: {
        name: columnName,
        type,
        nullable: true,
        key: '',
      },
    }
  })

  const primaryKeys = inferredColumns
    .filter(({ column }) => /^(id|uid)$/i.test(column.name))
    .filter(({ sourceField }) => {
      const seen = new Set<string>()
      for (const row of rows) {
        const rawValue = row[sourceField]
        if (rawValue === null || rawValue === undefined || rawValue === '') return false
        const key = stringifyImportValue(rawValue)
        if (seen.has(key)) return false
        seen.add(key)
      }
      return rows.length > 0
    })
    .map(({ column }) => column.name)

  const pkSet = new Set(primaryKeys)
  const columns = inferredColumns.map(({ column }) => ({
    ...column,
    key: pkSet.has(column.name) ? 'PRI' : '',
  }))

  return {
    columns,
    primaryKeys,
  }
}

/* ── Component ───────────────────────────────── */

export default function ImportWizard({
  connection,
  schemas,
  initialSchema,
  initialTable,
  onClose,
  onFlash,
}: Props) {
  /* Steps: 0=file, 1=preview, 2=target, 3=mapping, 4=execute */
  const [step, setStep] = useState(0)

  /* Step 0: File */
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [fileName, setFileName] = useState('')
  const [rawFile, setRawFile] = useState<File | null>(null)

  /* Step 1: Parse/Preview */
  const [delimiter, setDelimiter] = useState(',')
  const [hasHeader, setHasHeader] = useState(true)
  const [encoding, setEncoding] = useState('UTF-8')
  const [previewRows, setPreviewRows] = useState<string[][]>([])
  const [allParsedRows, setAllParsedRows] = useState<Record<string, unknown>[]>([])
  const [sourceFields, setSourceFields] = useState<string[]>([])
  const [totalRowCount, setTotalRowCount] = useState(0)
  const [fileType, setFileType] = useState<'csv' | 'json' | 'excel' | 'sql'>('csv')

  /* Step 2: Target */
  const [targetSchema, setTargetSchema] = useState(initialSchema ?? '')
  const [targetTable, setTargetTable] = useState(initialTable ?? '')
  const [targetColumns, setTargetColumns] = useState<TableColumn[]>([])
  const [createTablePlan, setCreateTablePlan] = useState<ImportCreateTablePlan | null>(null)
  const [loadingColumns, setLoadingColumns] = useState(false)

  /* Step 3: Mapping */
  const [columnMappings, setColumnMappings] = useState<ColumnMapping[]>([])
  const [importMode, setImportMode] = useState<ImportMode>('append')
  const [primaryKeys, setPrimaryKeys] = useState<string[]>([])

  /* Step 4: Execute */
  const [stats, setStats] = useState<ImportStats>({
    processed: 0, inserted: 0, errors: [], totalBatches: 0, completedBatches: 0, done: false,
  })
  const [importing, setImporting] = useState(false)
  const [tableSearchOpen, setTableSearchOpen] = useState(false)

  /* ── Set defaults ──────────────────────── */
  useEffect(() => {
    if (initialSchema) setTargetSchema(initialSchema)
    if (initialTable) setTargetTable(initialTable)
    if (initialSchema && initialTable) {
      // If table is pre-selected, skip to step 0 (file select) is fine
    }
  }, [initialSchema, initialTable])

  /* ── File handler ──────────────────────── */
  function detectFileType(name: string): 'csv' | 'json' | 'excel' | 'sql' {
    const lower = name.toLowerCase()
    if (lower.endsWith('.json')) return 'json'
    if (lower.endsWith('.xls') || lower.endsWith('.xlsx')) return 'excel'
    if (lower.endsWith('.sql')) return 'sql'
    return 'csv' // csv, txt, tsv, tab
  }

  function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    setRawFile(file)
    setFileName(file.name)
    const ft = detectFileType(file.name)
    setFileType(ft)
    const lower = file.name.toLowerCase()
    if (lower.endsWith('.txt') || lower.endsWith('.tsv') || lower.endsWith('.tab')) {
      setDelimiter('\t')
    } else {
      setDelimiter(',')
    }
  }

  function handleDrop(e: React.DragEvent) {
    e.preventDefault()
    const file = e.dataTransfer.files?.[0]
    if (!file) return
    setRawFile(file)
    setFileName(file.name)
    const ft = detectFileType(file.name)
    setFileType(ft)
    const lower = file.name.toLowerCase()
    if (lower.endsWith('.txt') || lower.endsWith('.tsv') || lower.endsWith('.tab')) {
      setDelimiter('\t')
    }
  }

  /* ── Parse file ────────────────────────── */
  function parseFile() {
    if (!rawFile) return

    if (fileType === 'json') {
      parseJsonFile()
    } else if (fileType === 'excel') {
      parseExcelFile()
    } else if (fileType === 'sql') {
      parseSqlFile()
    } else {
      parseCsvFile()
    }
  }

  function parseCsvFile() {
    if (!rawFile) return
    Papa.parse(rawFile, {
      delimiter: delimiter === 'auto' ? undefined : delimiter,
      header: hasHeader,
      encoding,
      skipEmptyLines: true,
      dynamicTyping: true,
      complete: (results) => {
        if (hasHeader) {
          const fields = results.meta.fields ?? []
          setSourceFields(fields)
          const rows = results.data as Record<string, unknown>[]
          setAllParsedRows(rows)
          setTotalRowCount(rows.length)
          const preview = rows.slice(0, 20).map((row) =>
            fields.map((f) => {
              const v = row[f]
              return v === null || v === undefined ? '' : String(v)
            }),
          )
          setPreviewRows(preview)
        } else {
          const data = results.data as string[][]
          const fields = data[0]?.map((_, i) => `Column ${i + 1}`) ?? []
          setSourceFields(fields)
          const rows = data.map((row) => {
            const obj: Record<string, unknown> = {}
            fields.forEach((f, i) => { obj[f] = row[i] })
            return obj
          })
          setAllParsedRows(rows)
          setTotalRowCount(rows.length)
          setPreviewRows(data.slice(0, 20))
        }
        setStep(1)
      },
      error: (err) => {
        onFlash('error', `解析失败: ${err.message}`)
      },
    })
  }

  function parseJsonFile() {
    if (!rawFile) return
    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const text = e.target?.result as string
        let data = JSON.parse(text)
        // Support both array and {data: [...]} patterns
        if (!Array.isArray(data)) {
          if (data.data && Array.isArray(data.data)) data = data.data
          else if (data.rows && Array.isArray(data.rows)) data = data.rows
          else {
            onFlash('error', 'JSON 文件必须是数组格式')
            return
          }
        }
        if (data.length === 0) {
          onFlash('error', 'JSON 文件为空')
          return
        }
        const fields = Object.keys(data[0])
        setSourceFields(fields)
        setAllParsedRows(data)
        setTotalRowCount(data.length)
        const preview = data.slice(0, 20).map((row: Record<string, unknown>) =>
          fields.map((f) => {
            const v = row[f]
            return v === null || v === undefined ? '' : String(v)
          }),
        )
        setPreviewRows(preview)
        setStep(1)
      } catch (err) {
        onFlash('error', `JSON 解析失败: ${err instanceof Error ? err.message : String(err)}`)
      }
    }
    reader.readAsText(rawFile, encoding)
  }

  function parseExcelFile() {
    if (!rawFile) return
    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const data = new Uint8Array(e.target?.result as ArrayBuffer)
        const workbook = XLSX.read(data, { type: 'array' })
        const sheetName = workbook.SheetNames[0]
        const sheet = workbook.Sheets[sheetName]
        const jsonData = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: '' })

        if (jsonData.length === 0) {
          onFlash('error', 'Excel 文件为空')
          return
        }
        const fields = Object.keys(jsonData[0])
        setSourceFields(fields)
        setAllParsedRows(jsonData)
        setTotalRowCount(jsonData.length)
        const preview = jsonData.slice(0, 20).map((row) =>
          fields.map((f) => {
            const v = row[f]
            return v === null || v === undefined ? '' : String(v)
          }),
        )
        setPreviewRows(preview)
        setStep(1)
      } catch (err) {
        onFlash('error', `Excel 解析失败: ${err instanceof Error ? err.message : String(err)}`)
      }
    }
    reader.readAsArrayBuffer(rawFile)
  }

  function parseSqlFile() {
    if (!rawFile) return
    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const text = e.target?.result as string
        // Extract INSERT statements and parse them into rows
        const insertRegex = /INSERT\s+INTO\s+[`"']?(\w+)[`"']?\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/gi
        const rows: Record<string, unknown>[] = []
        let fields: string[] = []
        let match
        while ((match = insertRegex.exec(text)) !== null) {
          if (fields.length === 0) {
            fields = match[2].split(',').map((f) => f.trim().replace(/[`"']/g, ''))
          }
          const vals = match[3].split(',').map((v) => {
            const trimmed = v.trim()
            if (trimmed === 'NULL') return null
            if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
              return trimmed.slice(1, -1).replace(/\\'/g, "'").replace(/\\\\/g, '\\')
            }
            const num = Number(trimmed)
            return isNaN(num) ? trimmed : num
          })
          const row: Record<string, unknown> = {}
          fields.forEach((f, i) => { row[f] = vals[i] ?? null })
          rows.push(row)
        }
        if (rows.length === 0) {
          onFlash('error', 'SQL 文件中未找到 INSERT 语句')
          return
        }
        setSourceFields(fields)
        setAllParsedRows(rows)
        setTotalRowCount(rows.length)
        const preview = rows.slice(0, 20).map((row) =>
          fields.map((f) => {
            const v = row[f]
            return v === null || v === undefined ? '' : String(v)
          }),
        )
        setPreviewRows(preview)
        setStep(1)
      } catch (err) {
        onFlash('error', `SQL 解析失败: ${err instanceof Error ? err.message : String(err)}`)
      }
    }
    reader.readAsText(rawFile, encoding)
  }

  /* ── Load target table columns ─────────── */
  async function loadTargetColumns(schema: string, table: string) {
    if (!schema || !table) return []
    setLoadingColumns(true)
    try {
      const res = await api.fetchTableColumns(connection, schema, table)
      setCreateTablePlan(null)
      setTargetColumns(res.columns)
      // Auto-map
      if (sourceFields.length > 0) {
        const mappings = autoMapColumns(sourceFields, res.columns)
        setColumnMappings(mappings)
      }
      // Auto-detect primary keys
      const pks = res.columns.filter((c) => c.key === 'PRI').map((c) => c.name)
      setPrimaryKeys(pks)
      return res.columns
    } catch (err) {
      setCreateTablePlan(null)
      setTargetColumns([])
      setColumnMappings([])
      setPrimaryKeys([])
      onFlash('error', `加载列信息失败: ${err instanceof Error ? err.message : String(err)}`)
      return []
    } finally {
      setLoadingColumns(false)
    }
  }

  /* ── Execute import ────────────────────── */
  async function executeImport() {
    const activeMappings = columnMappings.filter((m) => m.target !== '')
    if (activeMappings.length === 0) {
      onFlash('error', '至少需要一个有效的字段映射')
      return
    }

    const uniqueTargets = new Set(activeMappings.map((mapping) => mapping.target))
    if (uniqueTargets.size !== activeMappings.length) {
      onFlash('error', '字段映射中存在重复的目标列，请调整后再导入。')
      return
    }

    const effectiveCreateTablePlan = createTablePlan
      ? {
          columns: createTablePlan.columns.filter((column) => uniqueTargets.has(column.name)),
          primaryKeys: createTablePlan.primaryKeys.filter((name) => uniqueTargets.has(name)),
        }
      : null
    const effectivePrimaryKeys = effectiveCreateTablePlan?.primaryKeys ?? primaryKeys

    setImporting(true)
    const BATCH_SIZE = 2000
    const totalBatches = Math.ceil(allParsedRows.length / BATCH_SIZE)
    setStats({
      processed: 0, inserted: 0, errors: [], totalBatches, completedBatches: 0, done: false,
    })

    let totalProcessed = 0
    let totalInserted = 0
    const allErrors: string[] = []

    for (let i = 0; i < allParsedRows.length; i += BATCH_SIZE) {
      const batch = allParsedRows.slice(i, i + BATCH_SIZE)
      try {
        const result = await api.importBatch(
          connection,
          targetSchema,
          targetTable,
          importMode,
          effectivePrimaryKeys,
          activeMappings,
          batch,
          effectiveCreateTablePlan,
        )
        totalProcessed += result.processed
        totalInserted += result.inserted
        allErrors.push(...result.errors)
      } catch (err) {
        allErrors.push(`Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${err instanceof Error ? err.message : String(err)}`)
        totalProcessed += batch.length
      }

      setStats({
        processed: totalProcessed,
        inserted: totalInserted,
        errors: allErrors,
        totalBatches,
        completedBatches: Math.floor(i / BATCH_SIZE) + 1,
        done: false,
      })
    }

    setStats((prev) => ({ ...prev, done: true }))
    setImporting(false)
    onFlash('success', `导入完成: ${totalInserted} 行已插入`)
  }

  /* ── Available tables for selected schema ─ */
  const availableTables = schemas.find((s) => s.name === targetSchema)?.tables ?? []

  /* ── Render step content ───────────────── */

  function renderStep0() {
    const isCsv = fileType === 'csv'
    return (
      <div className="import-step">
        <div className="import-step-title">选择文件</div>
        <div className="import-step-desc">支持 CSV、TXT、Excel (xls/xlsx)、JSON、SQL 格式</div>

        <div
          className="import-dropzone"
          onClick={() => fileInputRef.current?.click()}
          onDragOver={(e) => e.preventDefault()}
          onDrop={handleDrop}
        >
          {fileName ? (
            <div className="import-file-info">
              <span className="import-file-icon">📄</span>
              <span className="import-file-name">{fileName}</span>
              <span className="import-file-size">{rawFile ? `${(rawFile.size / 1024).toFixed(1)} KB` : ''}</span>
            </div>
          ) : (
            <div className="import-dropzone-hint">
              <span style={{ fontSize: 32 }}>📂</span>
              <div>拖拽文件到此处，或点击选择文件</div>
              <div style={{ fontSize: 11, opacity: 0.5, marginTop: 4 }}>支持 .csv, .txt, .tsv, .xls, .xlsx, .json, .sql</div>
            </div>
          )}
        </div>
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.txt,.tsv,.tab,.xls,.xlsx,.json,.sql"
          style={{ display: 'none' }}
          onChange={handleFileSelect}
        />

        {isCsv && (
          <div className="import-options-row">
            <label>分隔符：</label>
            <select value={delimiter} onChange={(e) => setDelimiter(e.target.value)}>
              <option value=",">逗号 (,)</option>
              <option value={'\t'}>Tab</option>
              <option value=";">分号 (;)</option>
              <option value="|">竖线 (|)</option>
              <option value="auto">自动检测</option>
            </select>

            <label style={{ marginLeft: 16 }}>编码：</label>
            <select value={encoding} onChange={(e) => setEncoding(e.target.value)}>
              <option value="UTF-8">UTF-8</option>
              <option value="GBK">GBK</option>
              <option value="GB2312">GB2312</option>
              <option value="ISO-8859-1">ISO-8859-1</option>
            </select>

            <label style={{ marginLeft: 16 }}>
              <input type="checkbox" checked={hasHeader} onChange={(e) => setHasHeader(e.target.checked)} />
              {' '}第一行为表头
            </label>
          </div>
        )}

        {(fileType === 'json' || fileType === 'sql') && (
          <div className="import-options-row">
            <label>编码：</label>
            <select value={encoding} onChange={(e) => setEncoding(e.target.value)}>
              <option value="UTF-8">UTF-8</option>
              <option value="GBK">GBK</option>
            </select>
            <span style={{ marginLeft: 12, fontSize: 12, color: 'var(--text-muted)' }}>
              {fileType === 'json' ? '📄 JSON 数组格式' : '📄 将解析 INSERT INTO 语句'}
            </span>
          </div>
        )}

        {fileType === 'excel' && (
          <div className="import-options-row">
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>📊 将读取第一个 Sheet 中的数据</span>
          </div>
        )}
      </div>
    )
  }

  function renderStep1() {
    return (
      <div className="import-step">
        <div className="import-step-title">数据预览</div>
        <div className="import-step-desc">
          共 {totalRowCount.toLocaleString()} 行数据，{sourceFields.length} 列
        </div>

        <div className="import-preview-table-wrap">
          <table className="import-preview-table">
            <thead>
              <tr>
                <th className="import-row-num">#</th>
                {sourceFields.map((f) => <th key={f}>{f}</th>)}
              </tr>
            </thead>
            <tbody>
              {previewRows.map((row, i) => (
                <tr key={i}>
                  <td className="import-row-num">{i + 1}</td>
                  {row.map((cell, j) => (
                    <td key={j} className={cell === '' || cell === null ? 'cell-null' : ''}>
                      {cell === '' || cell === null ? 'NULL' : cell}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {totalRowCount > 20 && (
          <div className="import-preview-hint">仅显示前 20 行</div>
        )}
      </div>
    )
  }

  function renderStep2() {

    const tableSearchVal = targetTable
    const filteredTables = availableTables.filter((t) =>
      t.name.toLowerCase().includes(tableSearchVal.toLowerCase()),
    )
    const isNewTable = tableSearchVal && !availableTables.some((t) => t.name === tableSearchVal)

    return (
      <div className="import-step">
        <div className="import-step-title">选择目标</div>
        <div className="import-step-desc">选择数据要导入到的数据库和表</div>

        <div className="import-target-form">
          <div className="import-form-row">
            <label>数据库：</label>
            <select
              value={targetSchema}
              onChange={(e) => {
                setTargetSchema(e.target.value)
                setTargetTable('')
                setCreateTablePlan(null)
                setTargetColumns([])
                setColumnMappings([])
                setPrimaryKeys([])
              }}
            >
              <option value="">-- 选择数据库 --</option>
              {schemas.map((s) => (
                <option key={s.name} value={s.name}>{s.name}</option>
              ))}
            </select>
          </div>

          <div className="import-form-row">
            <label>目标表：</label>
            <div className="combo-box" style={{ flex: 1, position: 'relative' }}>
              <input
                type="text"
                className="combo-box-input"
                placeholder="搜索或输入新表名..."
                value={targetTable}
                onChange={(e) => {
                  setTargetTable(e.target.value)
                  setTableSearchOpen(true)
                  setCreateTablePlan(null)
                  setTargetColumns([])
                  setColumnMappings([])
                  setPrimaryKeys([])
                }}
                onFocus={() => setTableSearchOpen(true)}
                onBlur={() => {
                  // Delay to allow click on dropdown item
                  setTimeout(() => setTableSearchOpen(false), 200)
                }}
              />
              {tableSearchOpen && targetSchema && (
                <div className="combo-box-dropdown">
                  {isNewTable && (
                    <div
                      className="combo-box-item combo-box-new"
                      onMouseDown={() => {
                        setTableSearchOpen(false)
                        setCreateTablePlan(null)
                        setTargetColumns([])
                        setColumnMappings([])
                        setPrimaryKeys([])
                      }}
                    >
                      ➕ 创建新表 "<strong>{targetTable}</strong>"
                    </div>
                  )}
                  {filteredTables.length === 0 && !isNewTable && (
                    <div className="combo-box-empty">无匹配表</div>
                  )}
                  {filteredTables.slice(0, 50).map((t) => (
                    <div
                      key={t.name}
                      className={`combo-box-item ${t.name === targetTable ? 'selected' : ''}`}
                      onMouseDown={() => {
                        setTargetTable(t.name)
                        setTableSearchOpen(false)
                        void loadTargetColumns(targetSchema, t.name)
                      }}
                    >
                      <span>📊 {t.name}</span>
                      {t.rows != null && (
                        <span className="combo-box-meta">{t.rows.toLocaleString()} rows</span>
                      )}
                    </div>
                  ))}
                  {filteredTables.length > 50 && (
                    <div className="combo-box-empty">还有 {filteredTables.length - 50} 个表，请缩小搜索范围</div>
                  )}
                </div>
              )}
            </div>
            {loadingColumns && <span className="import-loading">加载中...</span>}
          </div>

          <div className="import-form-row">
            <label>导入模式：</label>
            <select value={importMode} onChange={(e) => setImportMode(e.target.value as ImportMode)}>
              <option value="append">追加 — 插入新记录</option>
              <option value="replace">替换 — REPLACE INTO（有则替换，无则插入）</option>
              <option value="upsert">更新 — INSERT ON DUPLICATE KEY UPDATE</option>
            </select>
          </div>
        </div>

        {targetColumns.length > 0 && (
          <div className="import-target-info">
            <div className="import-step-desc" style={{ marginTop: 12 }}>
              目标表 <strong>{targetTable}</strong> 包含 {targetColumns.length} 列
              {primaryKeys.length > 0 && (
                <span>，主键：<code>{primaryKeys.join(', ')}</code></span>
              )}
            </div>
          </div>
        )}

        {isNewTable && (
          <div className="import-step-desc" style={{ marginTop: 12, color: 'var(--accent)' }}>
            ⚡ 如果表不存在，将根据源文件字段自动创建 <strong>{targetTable}</strong>
          </div>
        )}
      </div>
    )
  }

  function renderStep3() {
    return (
      <div className="import-step">
        <div className="import-step-title">字段映射</div>
        <div className="import-step-desc">
          将源文件列映射到目标表列。未映射的列将被跳过。
        </div>

        {createTablePlan && (
          <div className="import-step-desc" style={{ marginTop: 8, color: 'var(--accent)' }}>
            ⚡ 导入开始前会自动创建表 <strong>{targetTable}</strong>（{createTablePlan.columns.length} 列）
          </div>
        )}

        <div className="import-mapping-table-wrap">
          <table className="import-mapping-table">
            <thead>
              <tr>
                <th style={{ width: 30 }}></th>
                <th>源字段</th>
                <th style={{ width: 40, textAlign: 'center' }}>→</th>
                <th>目标字段</th>
                <th style={{ width: 120 }}>预览值</th>
              </tr>
            </thead>
            <tbody>
              {columnMappings.map((mapping, i) => (
                <tr key={i} className={mapping.target ? '' : 'import-mapping-skipped'}>
                  <td style={{ textAlign: 'center', opacity: 0.4 }}>{i + 1}</td>
                  <td><code>{mapping.source}</code></td>
                  <td style={{ textAlign: 'center', opacity: 0.3 }}>→</td>
                  <td>
                    <select
                      value={mapping.target}
                      onChange={(e) => {
                        setColumnMappings((prev) => {
                          const next = [...prev]
                          next[i] = { ...next[i], target: e.target.value }
                          return next
                        })
                      }}
                    >
                      <option value="">(跳过)</option>
                      {targetColumns.map((c) => (
                        <option key={c.name} value={c.name}>
                          {c.name} ({c.type})
                          {c.key === 'PRI' ? ' 🔑' : ''}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td className="import-preview-cell">
                    {allParsedRows[0] ? String(allParsedRows[0][mapping.source] ?? '') : ''}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="import-mapping-summary">
          {columnMappings.filter((m) => m.target).length} / {columnMappings.length} 列已映射
        </div>
      </div>
    )
  }

  function renderStep4() {
    const progress = stats.totalBatches > 0
      ? Math.round((stats.completedBatches / stats.totalBatches) * 100)
      : 0

    return (
      <div className="import-step">
        <div className="import-step-title">{stats.done ? '导入完成' : '正在导入...'}</div>

        <div className="import-stats-grid">
          <div className="import-stat">
            <div className="import-stat-label">总行数</div>
            <div className="import-stat-value">{totalRowCount.toLocaleString()}</div>
          </div>
          <div className="import-stat">
            <div className="import-stat-label">已处理</div>
            <div className="import-stat-value">{stats.processed.toLocaleString()}</div>
          </div>
          <div className="import-stat">
            <div className="import-stat-label">已插入</div>
            <div className="import-stat-value">{stats.inserted.toLocaleString()}</div>
          </div>
          <div className="import-stat">
            <div className="import-stat-label">错误</div>
            <div className="import-stat-value" style={{ color: stats.errors.length > 0 ? '#f87171' : undefined }}>
              {stats.errors.length}
            </div>
          </div>
        </div>

        <div className="import-progress-bar-wrap">
          <div className="import-progress-bar" style={{ width: `${progress}%` }} />
        </div>
        <div className="import-progress-text">
          {stats.done
            ? '✅ 导入完成'
            : `批次 ${stats.completedBatches} / ${stats.totalBatches}  (${progress}%)`}
        </div>

        {stats.errors.length > 0 && (
          <div className="import-error-log">
            <div className="import-step-desc">错误日志：</div>
            <div className="import-error-list">
              {stats.errors.map((err, i) => (
                <div key={i} className="import-error-item">{err}</div>
              ))}
            </div>
          </div>
        )}
      </div>
    )
  }

  /* ── Step navigation ───────────────────── */
  const canNext = (() => {
    switch (step) {
      case 0: return !!rawFile
      case 1: return sourceFields.length > 0 && totalRowCount > 0
      case 2: return !!targetSchema && !!targetTable.trim()
      case 3: return columnMappings.some((m) => m.target !== '')
      default: return false
    }
  })()

  async function handleNext() {
    if (step === 0) {
      parseFile()
      return
    }
    if (step === 1) {
      // If target was pre-selected and columns already loaded, skip to mapping
      if (initialTable && targetColumns.length > 0) {
        setColumnMappings(autoMapColumns(sourceFields, targetColumns))
        setStep(3)
        return
      }
      setStep(2)
      return
    }
    if (step === 2) {
      const nextTable = targetTable.trim()
      if (!nextTable) return
      if (nextTable !== targetTable) {
        setTargetTable(nextTable)
      }

      const targetExists = availableTables.some((t) => t.name === nextTable)
      if (!targetExists) {
        const inferredPlan = inferCreateTablePlan(sourceFields, allParsedRows)
        setCreateTablePlan(inferredPlan)
        setTargetColumns(inferredPlan.columns)
        setPrimaryKeys(inferredPlan.primaryKeys)
        setColumnMappings(autoMapColumns(sourceFields, inferredPlan.columns))
        setStep(3)
        return
      }

      const resolvedColumns = targetColumns.length > 0 && !createTablePlan
        ? targetColumns
        : await loadTargetColumns(targetSchema, nextTable)

      if (resolvedColumns.length === 0) {
        onFlash('error', `未读取到目标表 "${nextTable}" 的字段，请确认表存在且当前账号有查看列信息的权限。`)
        return
      }

      if (columnMappings.length === 0 && sourceFields.length > 0) {
        setColumnMappings(autoMapColumns(sourceFields, resolvedColumns))
      }

      setStep(3)
      return
    }
    if (step === 3) {
      setStep(4)
      void executeImport()
      return
    }
  }

  function handleBack() {
    if (step === 3 && initialTable) {
      // Skip target step when pre-selected
      setStep(1)
      return
    }
    setStep((s) => Math.max(0, s - 1))
  }

  const stepLabels = ['选择文件', '数据预览', '选择目标', '字段映射', '执行导入']

  return (
    <div className="import-wizard-overlay" onClick={onClose}>
      <div className="import-wizard-modal" onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="import-wizard-header">
          <div className="import-wizard-title">📥 导入向导</div>
          <button className="import-close-btn" onClick={onClose} disabled={importing}>✕</button>
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
          <div style={{ flex: 1 }} />
          {step < 4 && (
            <button
              className="import-btn import-btn-primary"
              disabled={!canNext}
              onClick={handleNext}
            >
              {step === 3 ? '开始导入' : '下一步 →'}
            </button>
          )}
          {step === 4 && stats.done && (
            <button className="import-btn import-btn-primary" onClick={onClose}>
              完成
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
