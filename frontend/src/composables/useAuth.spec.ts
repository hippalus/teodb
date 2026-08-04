import { beforeEach, describe, expect, it } from 'vitest';
import { TOKEN_KEY, useAuth } from './useAuth';

describe('useAuth', () => {
  beforeEach(() => {
    // The composable is a singleton over module state — reset through its own API.
    useAuth().clearToken();
    sessionStorage.clear();
    useAuth().clearToken();
  });

  it('persists a token to sessionStorage and reflects hasToken', () => {
    const { token, hasToken, setToken } = useAuth();
    expect(hasToken.value).toBe(false);

    setToken('secret-123');
    expect(token.value).toBe('secret-123');
    expect(hasToken.value).toBe(true);
    expect(sessionStorage.getItem(TOKEN_KEY)).toBe('secret-123');
  });

  it('trims whitespace', () => {
    const { token, setToken } = useAuth();
    setToken('  padded  ');
    expect(token.value).toBe('padded');
  });

  it('clearing removes the token from sessionStorage', () => {
    const { hasToken, setToken, clearToken } = useAuth();
    setToken('x');
    clearToken();
    expect(hasToken.value).toBe(false);
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
  });

  it('setting an empty/whitespace token clears it', () => {
    const { hasToken, setToken } = useAuth();
    setToken('x');
    setToken('   ');
    expect(hasToken.value).toBe(false);
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
  });

  it('shares state across callers (singleton)', () => {
    useAuth().setToken('shared');
    expect(useAuth().token.value).toBe('shared');
    expect(useAuth().hasToken.value).toBe(true);
  });
});
