import { computed, ref } from 'vue';

/**
 * Bearer-token state for the admin UI.
 *
 * sessionStorage (not localStorage) is the source of truth — the axios request
 * interceptor in `api/client.ts` reads `teodb_token` straight from it on every
 * request. sessionStorage is per-tab and cleared when the tab closes, so the
 * admin token does not persist on disk across browser sessions, shrinking the
 * window an XSS could exfiltrate it (FE-2). It is still readable by JavaScript
 * on the origin — pair with a strict CSP at deployment for defense in depth.
 *
 * Module-level state makes it a singleton: every caller shares one ref.
 */
export const TOKEN_KEY = 'teodb_token';

const token = ref<string>(sessionStorage.getItem(TOKEN_KEY) ?? '');

function setToken(value: string): void {
  const trimmed = value.trim();
  token.value = trimmed;
  if (trimmed) {
    sessionStorage.setItem(TOKEN_KEY, trimmed);
  } else {
    sessionStorage.removeItem(TOKEN_KEY);
  }
}

function clearToken(): void {
  setToken('');
}

export function useAuth() {
  return {
    token,
    hasToken: computed(() => token.value.length > 0),
    setToken,
    clearToken,
  };
}
