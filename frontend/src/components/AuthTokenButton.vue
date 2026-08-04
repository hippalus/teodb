<script setup lang="ts">
import { ref, watch } from 'vue';
import { useToast } from 'primevue/usetoast';
import Button from 'primevue/button';
import Dialog from 'primevue/dialog';
import Password from 'primevue/password';
import { useAuth } from '@/composables/useAuth';

const { token, hasToken, setToken, clearToken } = useAuth();
const toast = useToast();

const visible = ref(false);
const draft = ref('');

// Seed the input with the current token each time the dialog opens.
watch(visible, (open) => {
  if (open) draft.value = token.value;
});

function save() {
  setToken(draft.value);
  visible.value = false;
  toast.add({
    severity: hasToken.value ? 'success' : 'info',
    summary: hasToken.value ? 'Token saved' : 'Token cleared',
    detail: hasToken.value
      ? 'Bearer token will be sent with admin requests.'
      : 'Requests will be sent without authentication.',
    life: 3000,
  });
}

function clear() {
  clearToken();
  draft.value = '';
  visible.value = false;
  toast.add({ severity: 'info', summary: 'Token cleared', life: 3000 });
}
</script>

<template>
  <Button
    :icon="hasToken ? 'pi pi-lock' : 'pi pi-lock-open'"
    text
    rounded
    :severity="hasToken ? 'success' : 'secondary'"
    :aria-label="hasToken ? 'Auth token set' : 'Set auth token'"
    v-tooltip.bottom="hasToken ? 'Auth token set — click to change' : 'Set auth token'"
    @click="visible = true"
  />

  <Dialog
    v-model:visible="visible"
    modal
    header="Admin Auth Token"
    :style="{ width: '28rem' }"
  >
    <p class="text-sm text-surface-500 mb-3">
      Bearer token sent on admin endpoints and <code>/metrics</code>. Stored in
      this browser's local storage only.
    </p>
    <Password
      v-model="draft"
      :feedback="false"
      toggleMask
      fluid
      placeholder="Paste bearer token"
      inputClass="w-full"
      @keyup.enter="save"
    />
    <template #footer>
      <Button
        label="Clear"
        text
        severity="danger"
        :disabled="!hasToken"
        @click="clear"
      />
      <Button label="Save" icon="pi pi-check" @click="save" />
    </template>
  </Dialog>
</template>
