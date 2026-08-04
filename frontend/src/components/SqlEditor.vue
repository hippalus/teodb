<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue';
import { EditorView, keymap, placeholder as cmPlaceholder } from '@codemirror/view';
import { EditorState } from '@codemirror/state';
import { defaultKeymap, indentWithTab } from '@codemirror/commands';
import { sql, PostgreSQL } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { autocompletion } from '@codemirror/autocomplete';

const props = withDefaults(
  defineProps<{
    modelValue?: string;
    placeholder?: string;
    tableNames?: string[];
    readonly?: boolean;
    height?: string;
  }>(),
  {
    modelValue: '',
    placeholder: 'Enter SQL query...',
    tableNames: () => [],
    readonly: false,
    height: '200px',
  }
);

const emit = defineEmits<{
  'update:modelValue': [value: string];
  execute: [];
}>();

const editorRef = ref<HTMLDivElement>();
let view: EditorView | null = null;

function buildSchema(): Record<string, string[]> {
  const schema: Record<string, string[]> = {};
  for (const name of props.tableNames) {
    schema[name] = [];
  }
  return schema;
}

function createState() {
  return EditorState.create({
    doc: props.modelValue,
    extensions: [
      keymap.of([
        ...defaultKeymap,
        indentWithTab,
        {
          key: 'Ctrl-Enter',
          mac: 'Cmd-Enter',
          run: () => {
            emit('execute');
            return true;
          },
        },
      ]),
      sql({ dialect: PostgreSQL, schema: buildSchema() }),
      autocompletion(),
      oneDark,
      cmPlaceholder(props.placeholder),
      EditorView.updateListener.of((update) => {
        if (update.docChanged) {
          emit('update:modelValue', update.state.doc.toString());
        }
      }),
      EditorView.editable.of(!props.readonly),
      EditorView.theme({
        '&': { height: props.height },
        '.cm-scroller': { overflow: 'auto' },
      }),
      EditorView.lineWrapping,
    ],
  });
}

onMounted(() => {
  if (!editorRef.value) return;
  view = new EditorView({
    state: createState(),
    parent: editorRef.value,
  });
});

onUnmounted(() => {
  view?.destroy();
});

watch(
  () => props.modelValue,
  (newVal) => {
    if (view && view.state.doc.toString() !== newVal) {
      view.dispatch({
        changes: {
          from: 0,
          to: view.state.doc.length,
          insert: newVal,
        },
      });
    }
  }
);

function focus() {
  view?.focus();
}

defineExpose({ focus });
</script>

<template>
  <div ref="editorRef" class="border border-surface-300 rounded-lg overflow-hidden"></div>
</template>
