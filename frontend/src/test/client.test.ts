import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, clearToken, getToken, setToken, TOKEN_KEY } from '../api/client';

afterEach(() => {
  clearToken();
  vi.restoreAllMocks();
});

describe('API client', () => {
  it('attaches a session-scoped bearer token', async () => {
    setToken('test-token');
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } }));
    await expect(api<{ ok: boolean }>('/health')).resolves.toEqual({ ok: true });
    const request = fetchMock.mock.calls[0][1] as RequestInit;
    expect(new Headers(request.headers).get('Authorization')).toBe('Bearer test-token');
    expect(getToken()).toBe('test-token');
  });

  it('converts structured API errors to ApiError', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ error: { code: 'rate_limited', message: 'Wait before retrying.', retry_after: 12 } }), { status: 429, headers: { 'Content-Type': 'application/json' } }));
    await expect(api('/somewhere')).rejects.toMatchObject({ status: 429, code: 'rate_limited', retryAfter: 12, message: 'Wait before retrying.' });
  });

  it('clears a token from sessionStorage', () => {
    setToken('another-token');
    clearToken();
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
  });
});
