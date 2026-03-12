import { useEffect, useRef, useMemo } from 'react'
import { EditorView, keymap, placeholder as cmPlaceholder } from '@codemirror/view'
import { EditorState, Compartment } from '@codemirror/state'
import { sql, MySQL, type SQLConfig } from '@codemirror/lang-sql'
import { basicSetup } from 'codemirror'
import { defaultKeymap, indentWithTab } from '@codemirror/commands'
import { autocompletion } from '@codemirror/autocomplete'
import type { SchemaNode } from '../types'

/* ═══════════════════════════════════════════════
   Light theme – matches NaviDog IDE style
   ═══════════════════════════════════════════════ */

const navidogTheme = EditorView.theme({
  '&': {
    height: '100%',
    fontSize: '13px',
    fontFamily: "'SF Mono', 'Menlo', 'Monaco', 'Consolas', monospace",
    background: '#ffffff',
  },
  '.cm-content': {
    caretColor: '#2968c8',
    padding: '8px 0',
  },
  '.cm-cursor': {
    borderLeftColor: '#2968c8',
  },
  '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': {
    background: '#cce2ff !important',
  },
  '.cm-activeLine': {
    background: '#f5f8ff',
  },
  '.cm-gutters': {
    background: '#f8f8f8',
    borderRight: '1px solid #e2e2e2',
    color: '#aaa',
    fontSize: '11px',
  },
  '.cm-activeLineGutter': {
    background: '#eef3fb',
    color: '#333',
  },
  /* Autocomplete tooltip */
  '.cm-tooltip': {
    border: '1px solid rgba(0,0,0,0.12)',
    borderRadius: '6px',
    boxShadow: '0 6px 24px rgba(0,0,0,0.15)',
    background: 'rgba(255,255,255,0.97)',
    backdropFilter: 'blur(12px)',
    overflow: 'hidden',
  },
  '.cm-tooltip.cm-tooltip-autocomplete': {
    '& > ul': {
      fontFamily: "'SF Mono', 'Menlo', monospace",
      fontSize: '12px',
      maxHeight: '240px',
    },
    '& > ul > li': {
      padding: '3px 10px 3px 6px',
      lineHeight: '1.5',
    },
    '& > ul > li[aria-selected]': {
      background: '#2968c8',
      color: '#fff',
    },
  },
  '.cm-completionIcon': {
    width: '18px',
    paddingRight: '4px',
    opacity: '0.7',
  },
  '.cm-completionLabel': {
    fontWeight: '500',
  },
  '.cm-completionDetail': {
    marginLeft: '8px',
    fontStyle: 'normal',
    color: '#999',
    fontSize: '11px',
  },
  /* Search panel */
  '.cm-panels': {
    borderBottom: '1px solid #e2e2e2',
    background: '#f8f8f8',
  },
  /* Matching brackets */
  '.cm-matchingBracket': {
    background: '#d4edbc',
    outline: '1px solid #92c353',
  },
})

/* ═══════════════════════════════════════════════
   Props
   ═══════════════════════════════════════════════ */

type SqlEditorProps = {
  value: string
  onChange: (value: string) => void
  onRun?: () => void
  schemas?: SchemaNode[]
  currentSchema?: string
  placeholder?: string
}

/* ═══════════════════════════════════════════════
   Component
   ═══════════════════════════════════════════════ */

// Compartment for dynamic SQL language reconfiguration
const sqlCompartment = new Compartment()

export default function SqlEditor({
  value,
  onChange,
  onRun,
  schemas = [],
  currentSchema,
  placeholder = '在这里输入 SQL...',
}: SqlEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const viewRef = useRef<EditorView | null>(null)
  const onChangeRef = useRef(onChange)
  const onRunRef = useRef(onRun)

  // Keep refs current
  onChangeRef.current = onChange
  onRunRef.current = onRun

  // Build SQL schema config for autocomplete
  const sqlConfig = useMemo<SQLConfig>(() => {
    const schemaMap: Record<string, string[]> = {}

    for (const schema of schemas) {
      for (const table of schema.tables) {
        const qualifiedName = `${schema.name}.${table.name}`
        schemaMap[qualifiedName] = []
        if (schema.name === currentSchema) {
          schemaMap[table.name] = []
        }
      }
    }

    return {
      dialect: MySQL,
      schema: schemaMap,
      defaultSchema: currentSchema,
    }
  }, [schemas, currentSchema])

  // Create editor
  useEffect(() => {
    if (!containerRef.current) return

    const runKeymap = keymap.of([
      {
        key: 'Mod-Enter',
        run: () => {
          onRunRef.current?.()
          return true
        },
      },
    ])

    const startState = EditorState.create({
      doc: value,
      extensions: [
        basicSetup,
        navidogTheme,
        sqlCompartment.of(sql({
          dialect: MySQL,
          schema: {},
        })),
        autocompletion({
          activateOnTyping: true,
          maxRenderedOptions: 30,
        }),
        runKeymap,
        keymap.of([...defaultKeymap, indentWithTab]),
        cmPlaceholder(placeholder),
        EditorView.updateListener.of(update => {
          if (update.docChanged) {
            onChangeRef.current(update.state.doc.toString())
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

    return () => {
      view.destroy()
      viewRef.current = null
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // Only create once

  // Sync external value changes (e.g., from history clicks)
  useEffect(() => {
    const view = viewRef.current
    if (!view) return

    const currentDoc = view.state.doc.toString()
    if (currentDoc !== value) {
      view.dispatch({
        changes: {
          from: 0,
          to: currentDoc.length,
          insert: value,
        },
      })
    }
  }, [value])

  // Reconfigure SQL language when schema changes
  useEffect(() => {
    const view = viewRef.current
    if (!view) return

    view.dispatch({
      effects: sqlCompartment.reconfigure(sql(sqlConfig)),
    })
  }, [sqlConfig])

  return (
    <div
      ref={containerRef}
      style={{ height: '100%', overflow: 'hidden' }}
    />
  )
}
