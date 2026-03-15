import { useState, useMemo } from 'react'

/* ════════════════════════════════════════════════
   TableDesigner – Navicat-style visual table editor
   ════════════════════════════════════════════════ */

export type ColumnDef = {
  name: string
  type: string
  length: string
  notNull: boolean
  isPK: boolean
  autoIncrement: boolean
  defaultValue: string
  comment: string
}

type Props = {
  /** Existing columns when editing a table; empty for new table */
  initialColumns?: ColumnDef[]
  tableName: string
  schemaName: string
  /** 'create' for new table, 'alter' for modifying */
  mode: 'create' | 'alter'
  onExecute: (sql: string) => void
  onCancel: () => void
}

const MYSQL_TYPES = [
  'INT', 'BIGINT', 'TINYINT', 'SMALLINT', 'MEDIUMINT',
  'VARCHAR', 'CHAR', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'TINYTEXT',
  'DATETIME', 'TIMESTAMP', 'DATE', 'TIME', 'YEAR',
  'DECIMAL', 'FLOAT', 'DOUBLE',
  'BLOB', 'MEDIUMBLOB', 'LONGBLOB', 'TINYBLOB',
  'ENUM', 'SET', 'JSON', 'BOOLEAN', 'BIT',
]

function blankCol(): ColumnDef {
  return { name: '', type: 'VARCHAR', length: '255', notNull: false, isPK: false, autoIncrement: false, defaultValue: '', comment: '' }
}

function q(name: string) { return '`' + name.replace(/`/g, '``') + '`' }

export default function TableDesigner({ initialColumns, tableName, schemaName, mode, onExecute, onCancel }: Props) {
  const [columns, setColumns] = useState<ColumnDef[]>(() =>
    initialColumns && initialColumns.length > 0 ? initialColumns : [
      { name: 'id', type: 'BIGINT', length: '20', notNull: true, isPK: true, autoIncrement: true, defaultValue: '', comment: '' },
    ]
  )
  const [activeTab, setActiveTab] = useState<'fields' | 'sql'>('fields')
  const [selectedIdx, setSelectedIdx] = useState(0)
  const [tblName, setTblName] = useState(tableName)

  // Original columns for ALTER comparison
  const originals = useMemo(() => initialColumns ?? [], [initialColumns])

  function updateCol(idx: number, patch: Partial<ColumnDef>) {
    setColumns(prev => prev.map((c, i) => i === idx ? { ...c, ...patch } : c))
  }

  function addColumn() {
    const c = blankCol()
    setColumns(prev => [...prev, c])
    setSelectedIdx(columns.length)
  }

  function removeColumn() {
    if (columns.length <= 1) return
    setColumns(prev => prev.filter((_, i) => i !== selectedIdx))
    setSelectedIdx(Math.max(0, selectedIdx - 1))
  }

  function moveUp() {
    if (selectedIdx <= 0) return
    setColumns(prev => {
      const arr = [...prev]
      ;[arr[selectedIdx - 1], arr[selectedIdx]] = [arr[selectedIdx], arr[selectedIdx - 1]]
      return arr
    })
    setSelectedIdx(selectedIdx - 1)
  }

  function moveDown() {
    if (selectedIdx >= columns.length - 1) return
    setColumns(prev => {
      const arr = [...prev]
      ;[arr[selectedIdx], arr[selectedIdx + 1]] = [arr[selectedIdx + 1], arr[selectedIdx]]
      return arr
    })
    setSelectedIdx(selectedIdx + 1)
  }

  /* ── SQL Generation ─────────────────────────── */
  const generatedSql = useMemo(() => {
    if (mode === 'create') {
      const pks = columns.filter(c => c.isPK).map(c => q(c.name))
      const lines = columns
        .filter(c => c.name.trim())
        .map(c => {
          let def = `  ${q(c.name)} ${c.type}`
          if (c.length && !['TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'TINYTEXT', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB', 'TINYBLOB', 'JSON', 'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR', 'BOOLEAN'].includes(c.type.toUpperCase())) {
            def += `(${c.length})`
          }
          if (c.notNull) def += ' NOT NULL'
          if (c.autoIncrement) def += ' AUTO_INCREMENT'
          if (c.defaultValue) {
            def += c.defaultValue.toUpperCase() === 'NULL' ? ' DEFAULT NULL'
              : c.defaultValue.toUpperCase() === 'CURRENT_TIMESTAMP' ? ' DEFAULT CURRENT_TIMESTAMP'
              : ` DEFAULT '${c.defaultValue}'`
          }
          if (c.comment) def += ` COMMENT '${c.comment.replace(/'/g, "\\'")}'`
          return def
        })
      if (pks.length) lines.push(`  PRIMARY KEY (${pks.join(', ')})`)
      return `CREATE TABLE ${q(schemaName)}.${q(tblName)} (\n${lines.join(',\n')}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
    }

    // ALTER mode: generate diff
    const stmts: string[] = []
    const prefix = `ALTER TABLE ${q(schemaName)}.${q(tblName)}`

    // Dropped columns
    for (const orig of originals) {
      if (!columns.find(c => c.name === orig.name)) {
        stmts.push(`${prefix} DROP COLUMN ${q(orig.name)};`)
      }
    }

    // Added / modified columns
    for (let i = 0; i < columns.length; i++) {
      const c = columns[i]
      if (!c.name.trim()) continue
      const orig = originals.find(o => o.name === c.name)

      let colDef = c.type
      if (c.length && !['TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'TINYTEXT', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB', 'TINYBLOB', 'JSON', 'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR', 'BOOLEAN'].includes(c.type.toUpperCase())) {
        colDef += `(${c.length})`
      }
      if (c.notNull) colDef += ' NOT NULL'
      if (c.autoIncrement) colDef += ' AUTO_INCREMENT'
      if (c.defaultValue) {
        colDef += c.defaultValue.toUpperCase() === 'NULL' ? ' DEFAULT NULL'
          : c.defaultValue.toUpperCase() === 'CURRENT_TIMESTAMP' ? ' DEFAULT CURRENT_TIMESTAMP'
          : ` DEFAULT '${c.defaultValue}'`
      }
      if (c.comment) colDef += ` COMMENT '${c.comment.replace(/'/g, "\\'")}'`

      if (!orig) {
        const after = i > 0 ? ` AFTER ${q(columns[i - 1].name)}` : ' FIRST'
        stmts.push(`${prefix} ADD COLUMN ${q(c.name)} ${colDef}${after};`)
      } else if (
        orig.type !== c.type || orig.length !== c.length || orig.notNull !== c.notNull ||
        orig.autoIncrement !== c.autoIncrement || orig.defaultValue !== c.defaultValue || orig.comment !== c.comment
      ) {
        stmts.push(`${prefix} MODIFY COLUMN ${q(c.name)} ${colDef};`)
      }
    }

    return stmts.join('\n') || '-- 无变更'
  }, [columns, tblName, schemaName, mode, originals])

  return (
    <div className="td-container">
      {/* Top bar */}
      <div className="td-topbar">
        <div className="td-topbar-left">
          <label className="td-name-label">表名</label>
          <input
            className="td-name-input"
            value={tblName}
            onChange={e => setTblName(e.target.value)}
            placeholder="table_name"
            disabled={mode === 'alter'}
          />
        </div>
        <div className="td-topbar-right">
          <button className="td-btn td-btn-primary" onClick={() => onExecute(generatedSql)} disabled={!tblName.trim()}>
            💾 保存
          </button>
          <button className="td-btn" onClick={onCancel}>取消</button>
        </div>
      </div>

      {/* Tab bar */}
      <div className="td-tab-bar">
        <button className={`td-tab${activeTab === 'fields' ? ' active' : ''}`} onClick={() => setActiveTab('fields')}>字段</button>
        <button className={`td-tab${activeTab === 'sql' ? ' active' : ''}`} onClick={() => setActiveTab('sql')}>SQL 预览</button>
      </div>

      {activeTab === 'fields' ? (
        <>
          {/* Field toolbar */}
          <div className="td-field-toolbar">
            <button className="td-btn" onClick={addColumn} title="添加字段">➕ 添加字段</button>
            <button className="td-btn" onClick={removeColumn} title="删除字段" disabled={columns.length <= 1}>➖ 删除</button>
            <div className="td-toolbar-sep" />
            <button className="td-btn" onClick={moveUp} title="上移" disabled={selectedIdx <= 0}>⬆</button>
            <button className="td-btn" onClick={moveDown} title="下移" disabled={selectedIdx >= columns.length - 1}>⬇</button>
          </div>

          {/* Fields grid */}
          <div className="td-grid-wrapper">
            <table className="td-grid">
              <thead>
                <tr>
                  <th style={{ width: 30 }}>#</th>
                  <th style={{ width: 180 }}>名称</th>
                  <th style={{ width: 130 }}>类型</th>
                  <th style={{ width: 70 }}>长度</th>
                  <th style={{ width: 50 }}>非空</th>
                  <th style={{ width: 50 }}>主键</th>
                  <th style={{ width: 50 }}>自增</th>
                  <th style={{ width: 140 }}>默认值</th>
                  <th>注释</th>
                </tr>
              </thead>
              <tbody>
                {columns.map((col, idx) => (
                  <tr
                    key={idx}
                    className={idx === selectedIdx ? 'td-row-selected' : ''}
                    onClick={() => setSelectedIdx(idx)}
                  >
                    <td className="td-cell-num">{idx + 1}</td>
                    <td>
                      <input
                        className="td-cell-input"
                        value={col.name}
                        onChange={e => updateCol(idx, { name: e.target.value })}
                        placeholder="column_name"
                      />
                    </td>
                    <td>
                      <select
                        className="td-cell-select"
                        value={col.type}
                        onChange={e => updateCol(idx, { type: e.target.value })}
                      >
                        {MYSQL_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                      </select>
                    </td>
                    <td>
                      <input
                        className="td-cell-input td-cell-short"
                        value={col.length}
                        onChange={e => updateCol(idx, { length: e.target.value })}
                        placeholder=""
                      />
                    </td>
                    <td className="td-cell-center">
                      <input type="checkbox" checked={col.notNull} onChange={e => updateCol(idx, { notNull: e.target.checked })} />
                    </td>
                    <td className="td-cell-center">
                      <input type="checkbox" checked={col.isPK} onChange={e => updateCol(idx, { isPK: e.target.checked })} />
                    </td>
                    <td className="td-cell-center">
                      <input type="checkbox" checked={col.autoIncrement} onChange={e => updateCol(idx, { autoIncrement: e.target.checked })} />
                    </td>
                    <td>
                      <input
                        className="td-cell-input"
                        value={col.defaultValue}
                        onChange={e => updateCol(idx, { defaultValue: e.target.value })}
                        placeholder="NULL"
                      />
                    </td>
                    <td>
                      <input
                        className="td-cell-input"
                        value={col.comment}
                        onChange={e => updateCol(idx, { comment: e.target.value })}
                        placeholder=""
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      ) : (
        /* SQL Preview */
        <div className="td-sql-preview">
          <pre>{generatedSql}</pre>
        </div>
      )}
    </div>
  )
}
