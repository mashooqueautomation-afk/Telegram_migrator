import { fetchEventSource } from '@microsoft/fetch-event-source';

export const TOKEN_KEY = 'tg_migrator_access_token';
export const getToken = () => sessionStorage.getItem(TOKEN_KEY);
export const setToken = (token: string) => sessionStorage.setItem(TOKEN_KEY, token);
export const clearToken = () => sessionStorage.removeItem(TOKEN_KEY);

const PUBLIC_AUTH_PATHS = new Set(['/auth/login', '/auth/register']);

export class ApiError extends Error {
  status: number;
  code?: string;
  retryAfter?: number;

  constructor(message: string, status: number, code?: string, retryAfter?: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.retryAfter = retryAfter;
  }
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('Accept', 'application/json');
  if (init.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  // Login/register are public endpoints: never send a stale session token to them.
  const normalizedPath = path.split('?')[0];
  if (PUBLIC_AUTH_PATHS.has(normalizedPath)) {
    headers.delete('Authorization');
  } else {
    const token = getToken();
    if (token) headers.set('Authorization', `Bearer ${token}`);
    else headers.delete('Authorization');
  }

  const res = await fetch(`/api${path}`, { ...init, headers, credentials: 'same-origin' });
  if (res.status === 204) return undefined as T;

  const payload = await res.json().catch(() => null);
  if (!res.ok) {
    const err = payload?.error ?? payload?.detail ?? {};
    const message = typeof err === 'string'
      ? err
      : (err.message || err.msg || `Request failed (${res.status})`);
    const retryAfter = Number(err.retry_after || res.headers.get('Retry-After') || 0) || undefined;
    throw new ApiError(message, res.status, err.code, retryAfter);
  }
  return payload as T;
}

export function openMigrationStream(
  migrationId: string,
  onEvent: (type: string, data: any, id?: string) => void,
  onError: (error: unknown) => void,
  signal?: AbortSignal,
) {
  const token = getToken();
  if (!token) {
    onError(new ApiError('Your session has expired. Sign in again.', 401));
    return;
  }

  let lastId = 0;
  const storedLast = sessionStorage.getItem(`last_event_${migrationId}`);
  if (storedLast && /^\d+$/.test(storedLast)) lastId = Number(storedLast);

  fetchEventSource(`/api/migrations/${migrationId}/events/stream?after_seq=${lastId}`, {
    method: 'GET',
    headers: { Authorization: `Bearer ${token}`, Accept: 'text/event-stream' },
    signal,
    openWhenHidden: true,
    async onopen(response) {
      if (!response.ok) {
        const body = await response.json().catch(() => null);
        throw new ApiError(body?.error?.message || `Event stream failed (${response.status})`, response.status, body?.error?.code);
      }
      if (!response.headers.get('content-type')?.includes('text/event-stream')) {
        throw new ApiError('Live stream returned an unexpected content type.', response.status);
      }
    },
    onmessage(message) {
      if (message.id && /^\d+$/.test(message.id)) {
        lastId = Number(message.id);
        sessionStorage.setItem(`last_event_${migrationId}`, message.id);
      }
      try {
        onEvent(message.event || 'message', JSON.parse(message.data), message.id);
      } catch {
        // The next persisted snapshot keeps the UI accurate if an event frame is malformed.
      }
    },
    onclose() {
      // Normal close is expected when a job reaches a terminal state.
    },
    onerror(error) {
      onError(error);
      if (signal?.aborted) return;
      return 3000;
    },
  });
}
