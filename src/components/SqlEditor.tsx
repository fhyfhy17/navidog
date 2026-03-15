import { useEffect, useRef, useMemo, useImperativeHandle, forwardRef } from 'react'
import { EditorView, keymap, placeholder as cmPlaceholder } from '@codemirror/view'
import { EditorState, Compartment, Prec } from '@codemirror/state'
import { sql, MySQL, keywordCompletionSource } from '@codemirror/lang-sql'
import { basicSetup } from 'codemirror'
import { defaultKeymap, indentWithTab } from '@codemirror/commands'
import {
  acceptCompletion,
  autocompletion,
  completionKeymap,
  completionStatus,
  type Completion,
  type CompletionContext,
} from '@codemirror/autocomplete'
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language'
import { tags } from '@lezer/highlight'
import type { SchemaNode } from '../types'

/* ═══════════════════════════════════════════════
   Dark syntax highlighting – VS Code Dark+ palette
   (matches the right panel DDL colors)
   ═══════════════════════════════════════════════ */

const darkHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword,         color: '#569cd6' },
  { tag: tags.operatorKeyword, color: '#569cd6' },
  { tag: tags.definitionKeyword, color: '#569cd6' },
  { tag: tags.typeName,        color: '#4ec9b0' },
  { tag: tags.string,          color: '#ce9178' },
  { tag: tags.number,          color: '#b5cea8' },
  { tag: tags.bool,            color: '#569cd6' },
  { tag: tags.null,            color: '#569cd6' },
  { tag: tags.comment,         color: '#6a9955' },
  { tag: tags.lineComment,     color: '#6a9955' },
  { tag: tags.blockComment,    color: '#6a9955' },
  { tag: tags.punctuation,     color: '#808080' },
  { tag: tags.paren,           color: '#808080' },
  { tag: tags.squareBracket,   color: '#808080' },
  { tag: tags.brace,           color: '#808080' },
  { tag: tags.operator,        color: '#d4d4d4' },
  { tag: tags.variableName,    color: '#9cdcfe' },
  { tag: tags.propertyName,    color: '#9cdcfe' },
])

const lightHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword,         color: '#0033b3' },
  { tag: tags.operatorKeyword, color: '#0033b3' },
  { tag: tags.definitionKeyword, color: '#0033b3' },
  { tag: tags.typeName,        color: '#067d17' },
  { tag: tags.string,          color: '#a5060e' },
  { tag: tags.number,          color: '#1750eb' },
  { tag: tags.bool,            color: '#0033b3' },
  { tag: tags.null,            color: '#0033b3' },
  { tag: tags.comment,         color: '#8c8c8c' },
  { tag: tags.lineComment,     color: '#8c8c8c' },
  { tag: tags.blockComment,    color: '#8c8c8c' },
  { tag: tags.punctuation,     color: '#333' },
  { tag: tags.paren,           color: '#333' },
  { tag: tags.squareBracket,   color: '#333' },
  { tag: tags.brace,           color: '#333' },
  { tag: tags.operator,        color: '#333' },
  { tag: tags.variableName,    color: '#871094' },
  { tag: tags.propertyName,    color: '#871094' },
])

/* ═══════════════════════════════════════════════
   Editor base theme
   ═══════════════════════════════════════════════ */

const navidogTheme = EditorView.theme({
  '&': {
    fontSize: '13px',
    height: '100%',
  },
  '.cm-scroller': {
    fontFamily: 'var(--font-mono)',
    overflow: 'auto',
  },
  '.cm-gutters': {
    backgroundColor: 'transparent',
    border: 'none',
    paddingRight: '4px',
  },
  '.cm-lineNumbers .cm-gutterElement': {
    paddingLeft: '8px',
    paddingRight: '6px',
    minWidth: '24px',
    color: '#b0b0b0',
    fontSize: '12px',
  },
  '.cm-activeLine': {
    backgroundColor: 'rgba(0,0,0,0.03)',
  },
  '.cm-activeLineGutter': {
    backgroundColor: 'transparent',
  },
  '.cm-cursor': {
    borderLeftColor: '#333',
  },
  '.cm-selectionBackground': {
    backgroundColor: 'rgba(41,104,200,0.2) !important',
  },
  '&.cm-focused .cm-selectionBackground': {
    backgroundColor: 'rgba(41,104,200,0.3) !important',
  },
  '.cm-tooltip-autocomplete': {
    maxHeight: '280px',
    fontSize: '12px',
    border: '1px solid #d0d0d0',
    borderRadius: '6px',
    boxShadow: '0 4px 14px rgba(0,0,0,0.12)',
  },
  '.cm-completionLabel': {
    fontSize: '12px',
  },
  '.cm-completionDetail': {
    fontSize: '11px',
    fontStyle: 'normal',
    color: '#999',
    marginLeft: '8px',
  },
  '.cm-completionIcon': {
    fontSize: '12px',
    opacity: '0.7',
  },
  '.cm-tooltip-autocomplete ul li[aria-selected]': {
    backgroundColor: 'rgba(41,104,200,0.12)',
    color: 'inherit',
  },
  '.cm-panels': {
    backgroundColor: 'transparent',
  },
  '.cm-search': {
    fontSize: '13px',
  },
})

/* ═══════════════════════════════════════════════
   Exported types
   ═══════════════════════════════════════════════ */

type SqlEditorProps = {
  value: string
  onChange: (value: string) => void
  onRun?: () => void
  onRunSelection?: (selectedSql: string) => void
  schemas?: SchemaNode[]
  currentSchema?: string
  tableColumns?: Record<string, string[]>
  placeholder?: string
  suppressExternalValueSync?: boolean
}

export type SqlEditorHandle = {
  getSelectedText: () => string
  getValue: () => string
  getRunnableSql: () => string
  setValue: (value: string) => void
  formatSql: () => void
  getView: () => EditorView | null
}

/* ═══════════════════════════════════════════════
   Component
   ═══════════════════════════════════════════════ */

const sqlCompartment = new Compartment()
const highlightCompartment = new Compartment()
const autocompleteCompartment = new Compartment()

function isDark() {
  return document.documentElement.getAttribute('data-theme') === 'dark'
}

async function formatSqlDocument(sqlText: string) {
  const { format } = await import('sql-formatter')
  return format(sqlText, {
    language: 'mysql',
    tabWidth: 2,
    keywordCase: 'upper',
  })
}

/** Check if the semicolon at `index` is a real statement separator (not inside a string/comment) */
function isStatementSeparator(sqlText: string, index: number) {
  let inSingle = false
  let inDouble = false
  let inBacktick = false
  let inLineComment = false
  let inBlockComment = false

  for (let i = 0; i <= index; i += 1) {
    const ch = sqlText[i]
    const next = sqlText[i + 1]
    const prev = sqlText[i - 1]

    if (inLineComment) {
      if (ch === '\n') inLineComment = false
      continue
    }

    if (inBlockComment) {
      if (prev === '*' && ch === '/') inBlockComment = false
      continue
    }

    if (inSingle) {
      if (ch === "'" && prev !== '\\') inSingle = false
      continue
    }

    if (inDouble) {
      if (ch === '"' && prev !== '\\') inDouble = false
      continue
    }

    if (inBacktick) {
      if (ch === '`') inBacktick = false
      continue
    }

    if (ch === '-' && next === '-') {
      inLineComment = true
      i += 1
      continue
    }

    if (ch === '#') {
      inLineComment = true
      continue
    }

    if (ch === '/' && next === '*') {
      inBlockComment = true
      i += 1
      continue
    }

    if (ch === "'") {
      inSingle = true
      continue
    }

    if (ch === '"') {
      inDouble = true
      continue
    }

    if (ch === '`') {
      inBacktick = true
      continue
    }

    if (ch === ';' && i === index) {
      return true
    }
  }

  return false
}

/** Find the boundaries of the SQL statement surrounding the cursor position */
function getStatementBoundaries(sqlText: string, cursor: number) {
  if (!sqlText.trim()) {
    return { from: 0, to: sqlText.length }
  }

  let from = 0
  for (let i = Math.min(cursor - 1, sqlText.length - 1); i >= 0; i -= 1) {
    if (sqlText[i] === ';' && isStatementSeparator(sqlText, i)) {
      from = i + 1
      break
    }
  }

  let to = sqlText.length
  for (let i = Math.max(0, cursor); i < sqlText.length; i += 1) {
    if (sqlText[i] === ';' && isStatementSeparator(sqlText, i)) {
      to = i + 1
      break
    }
  }

  return { from, to }
}

/** Get the SQL that should be run: selection if present, else current statement, else all */
function getRunnableSqlFromView(view: EditorView) {
  const { from, to, head } = view.state.selection.main
  if (from !== to) {
    return view.state.sliceDoc(from, to).trim()
  }

  const sqlText = view.state.doc.toString()
  const current = getStatementBoundaries(sqlText, head)
  const activeSql = sqlText.slice(current.from, current.to).trim()

  if (activeSql) {
    return activeSql
  }

  return sqlText.trim()
}

const SqlEditor = forwardRef<SqlEditorHandle, SqlEditorProps>(function SqlEditor({
  value,
  onChange,
  onRun,
  onRunSelection,
  schemas = [],
  currentSchema,
  tableColumns = {},
  placeholder = '在这里输入 SQL...',
  suppressExternalValueSync = false,
}, ref) {
  const containerRef = useRef<HTMLDivElement>(null)
  const viewRef = useRef<EditorView | null>(null)
  const onChangeRef = useRef(onChange)
  const onRunRef = useRef(onRun)
  const onRunSelectionRef = useRef(onRunSelection)
  const lastEmittedRef = useRef(value)

  onChangeRef.current = onChange
  onRunRef.current = onRun
  onRunSelectionRef.current = onRunSelection

  useImperativeHandle(ref, () => ({
    getSelectedText() {
      const view = viewRef.current
      if (!view) return ''
      const { from, to } = view.state.selection.main
      return from === to ? '' : view.state.sliceDoc(from, to)
    },
    getValue() {
      return viewRef.current?.state.doc.toString() ?? ''
    },
    getRunnableSql() {
      const view = viewRef.current
      if (!view) return ''
      return getRunnableSqlFromView(view)
    },
    setValue(nextValue: string) {
      const view = viewRef.current
      if (!view) return
      const currentDoc = view.state.doc.toString()
      if (currentDoc === nextValue) return
      lastEmittedRef.current = nextValue
      view.dispatch({
        changes: {
          from: 0,
          to: currentDoc.length,
          insert: nextValue,
        },
      })
    },
    formatSql() {
      const view = viewRef.current
      if (!view) return
      void (async () => {
        try {
          const formatted = await formatSqlDocument(view.state.doc.toString())
          view.dispatch({
            changes: { from: 0, to: view.state.doc.length, insert: formatted },
          })
          lastEmittedRef.current = formatted
          onChangeRef.current(formatted)
        } catch {
          // Ignore formatter load/parse errors and keep the editor usable.
        }
      })()
    },
    getView() {
      return viewRef.current
    },
  }))

  const identifierOptions = useMemo<Completion[]>(() => {
    const activeSchema = schemas.find((schema) => schema.name === currentSchema) ?? null
    const options: Completion[] = []
    const seen = new Set<string>()

    if (!activeSchema) {
      return options
    }

    for (const table of activeSchema.tables) {
      if (!seen.has(table.name)) {
        seen.add(table.name)
        options.push({
          label: table.name,
          type: 'class',
          detail: 'table',
        })
      }
    }

    for (const [cacheKey, columns] of Object.entries(tableColumns)) {
      if (!cacheKey.includes(`:${activeSchema.name}.`)) continue
      for (const column of columns) {
        if (seen.has(column)) continue
        seen.add(column)
        options.push({
          label: column,
          type: 'property',
          detail: 'column',
        })
      }
    }

    return options
  }, [schemas, currentSchema, tableColumns])

  const identifierCompletionSource = useMemo(() => {
    const normalizedOptions = identifierOptions.map((option) => ({
      ...option,
      boost: option.detail === 'table' ? 2 : 1,
      apply: option.label,
    }))

    return (context: CompletionContext) => {
      const word = context.matchBefore(/[`\w$]*$/)
      if (!word) return null
      if (word.from === word.to && !context.explicit) return null

      const query = word.text.replaceAll('`', '').toLowerCase()
      const filtered = query
        ? normalizedOptions.filter((option) => option.label.toLowerCase().includes(query)).slice(0, 80)
        : normalizedOptions.slice(0, 80)

      if (filtered.length === 0) {
        return null
      }

      return {
        from: word.from,
        to: word.to,
        options: filtered,
        validFor: /[`\w$]*/,
      }
    }
  }, [identifierOptions])

  useEffect(() => {
    if (!containerRef.current) return

    const runKeymap = keymap.of([
      {
        key: 'Mod-Enter',
        run: () => {
          const view = viewRef.current
          if (!view) {
            onRunRef.current?.()
            return true
          }

          const { from, to } = view.state.selection.main
          if (from !== to && onRunSelectionRef.current) {
            onRunSelectionRef.current(view.state.sliceDoc(from, to))
            return true
          }

          onRunSelectionRef.current?.(getRunnableSqlFromView(view))
          return true
        },
      },
      {
        key: 'Shift-Mod-Enter',
        run: () => {
          onRunRef.current?.()
          return true
        },
      },
    ])
    const completionAcceptKeymap = Prec.highest(keymap.of([
      {
        key: 'Enter',
        run: (view) => completionStatus(view.state) === 'active' ? acceptCompletion(view) : false,
      },
      {
        key: 'Tab',
        run: (view) => completionStatus(view.state) === 'active' ? acceptCompletion(view) : false,
      },
    ]))

    const dark = isDark()

    const startState = EditorState.create({
      doc: value,
      extensions: [
        basicSetup,
        navidogTheme,
        highlightCompartment.of(syntaxHighlighting(dark ? darkHighlightStyle : lightHighlightStyle)),
        sqlCompartment.of(sql({ dialect: MySQL })),
        autocompleteCompartment.of(autocompletion({
          activateOnTyping: true,
          activateOnTypingDelay: 180,
          maxRenderedOptions: 30,
          interactionDelay: 0,
          selectOnOpen: true,
          defaultKeymap: true,
          override: [
            identifierCompletionSource,
            keywordCompletionSource(MySQL),
          ],
        })),
        completionAcceptKeymap,
        Prec.highest(keymap.of(completionKeymap)),
        runKeymap,
        keymap.of([...defaultKeymap, indentWithTab]),
        cmPlaceholder(placeholder),
        EditorView.updateListener.of(update => {
          if (update.docChanged) {
            const newDoc = update.state.doc.toString()
            lastEmittedRef.current = newDoc
            onChangeRef.current(newDoc)
          }
        }),
        EditorView.lineWrapping,
      ],
    })

    const view = new EditorView({
      state: startState,
      parent: containerRef.current,
    })

    viewRef.current = view

    // Watch for theme changes
    const observer = new MutationObserver(() => {
      const nowDark = isDark()
      view.dispatch({
        effects: highlightCompartment.reconfigure(
          syntaxHighlighting(nowDark ? darkHighlightStyle : lightHighlightStyle)
        ),
      })
    })
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] })

    return () => {
      observer.disconnect()
      view.destroy()
      viewRef.current = null
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (suppressExternalValueSync) return
    const view = viewRef.current
    if (!view) return
    // Skip if this value came from our own onChange emission
    if (value === lastEmittedRef.current) return

    const currentDoc = view.state.doc.toString()
    if (currentDoc !== value) {
      lastEmittedRef.current = value
      view.dispatch({
        changes: {
          from: 0,
          to: currentDoc.length,
          insert: value,
        },
      })
    }
  }, [value, suppressExternalValueSync])

  useEffect(() => {
    const view = viewRef.current
    if (!view) return

    view.dispatch({
      effects: autocompleteCompartment.reconfigure(autocompletion({
        activateOnTyping: true,
        activateOnTypingDelay: 180,
        maxRenderedOptions: 30,
        interactionDelay: 0,
        selectOnOpen: true,
        defaultKeymap: true,
        override: [
          identifierCompletionSource,
          keywordCompletionSource(MySQL),
        ],
      })),
    })
  }, [identifierCompletionSource])

  return (
    <div
      ref={containerRef}
      style={{ height: '100%', overflow: 'hidden' }}
    />
  )
})

export default SqlEditor
