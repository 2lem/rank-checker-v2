const baseUrl = process.env.BASE_URL;

if (!baseUrl) {
  console.error('FAIL: BASE_URL environment variable is required');
  process.exit(1);
}

const TIMEOUT_MS = 10_000;
const SCAN_TIMEOUT_MS = Number(process.env.SCAN_TIMEOUT_MS || 45_000);
const DEBUG_LOG_PREFIX = '[smoke]';
const trackedPlaylistId = process.env.TEST_TRACKED_PLAYLIST_ID;
const testPlaylistUrl = process.env.TEST_PLAYLIST_URL_OR_ID;

const withTimeout = async (promise, label, timeoutMs = TIMEOUT_MS) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await promise(controller.signal);
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`${label} timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
};

const request = async (path, options = {}, timeoutMs = TIMEOUT_MS) => {
  const url = new URL(path, baseUrl).toString();
  return withTimeout(
    (signal) =>
      fetch(url, {
        ...options,
        signal,
      }),
    `${options.method || 'GET'} ${path}`,
    timeoutMs
  );
};

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

const parseJsonSafely = async (response) => {
  const text = await response.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    parsed = null;
  }
  return { text, parsed };
};

const waitForSseCompletion = async (eventsUrl, label, timeoutMs = SCAN_TIMEOUT_MS) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  let lastEvent = null;
  try {
    const response = await fetch(eventsUrl, {
      headers: { Accept: 'text/event-stream' },
      signal: controller.signal,
    });
    if (!response.ok) {
      const { text } = await parseJsonSafely(response);
      throw new Error(`${label} SSE failed with ${response.status}: ${text}`);
    }
    if (!response.body) {
      throw new Error(`${label} SSE response had no body`);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (!line.startsWith('data:')) {
          continue;
        }
        const payloadRaw = line.replace(/^data:\s*/, '').trim();
        if (!payloadRaw) {
          continue;
        }
        lastEvent = payloadRaw;
        let payload = null;
        try {
          payload = JSON.parse(payloadRaw);
        } catch (error) {
          payload = null;
        }
        const type = payload?.type || '';
        const status = `${payload?.status || ''}`.toLowerCase();
        if (type === 'done' || type === 'error' || status === 'completed' || status === 'failed') {
          return { type, payloadRaw };
        }
      }
    }
    throw new Error(`${label} SSE closed without completion; lastEvent=${lastEvent || 'none'}`);
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(
        `${label} SSE timed out after ${timeoutMs}ms; lastEvent=${lastEvent || 'none'}`
      );
    }
    if (lastEvent) {
      throw new Error(`${label} SSE error; lastEvent=${lastEvent}; ${error.message}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fetchPlaylists = async () => {
  const response = await request('/api/playlists');
  const { parsed, text } = await parseJsonSafely(response);
  assert(response.status === 200, `Expected 200 for /api/playlists, got ${response.status} ${text}`);
  if (!Array.isArray(parsed)) {
    throw new Error(`Expected array payload for /api/playlists, got ${text}`);
  }
  return parsed;
};

const findTrackedPlaylist = (playlists, trackedId) =>
  playlists.find((playlist) => String(playlist.id) === String(trackedId)) || null;

const waitForPlaylistRefresh = async (trackedId, previousTimestamp, timeoutMs = 60_000) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const playlists = await fetchPlaylists();
    const tracked = findTrackedPlaylist(playlists, trackedId);
    const current = tracked?.last_meta_refresh_at;
    if (current && current !== previousTimestamp) {
      return current;
    }
    await sleep(5000);
  }
  throw new Error('Refresh job did not update last_meta_refresh_at within timeout.');
};

const run = async () => {
  const results = [];

  const healthResponse = await request('/health');
  assert(healthResponse.status === 200, `Expected 200 for /health, got ${healthResponse.status}`);
  results.push('PASS: GET /health');

  const openApiResponse = await request('/openapi.json');
  assert(openApiResponse.status === 200, `Expected 200 for /openapi.json, got ${openApiResponse.status}`);
  results.push('PASS: GET /openapi.json');

  if (testPlaylistUrl) {
    const addResponse = await request(
      '/api/playlists',
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          playlist_url: testPlaylistUrl,
          target_countries: ['US'],
          target_keywords: ['test'],
        }),
      },
      30_000
    );
    const { text: addText } = await parseJsonSafely(addResponse);
    assert(
      addResponse.status === 201 || addResponse.status === 409,
      `Expected 201/409 for add playlist, got ${addResponse.status} ${addText}`
    );
    results.push(`PASS: POST /api/playlists (status ${addResponse.status})`);
  } else {
    console.warn(`${DEBUG_LOG_PREFIX} skipping add playlist: TEST_PLAYLIST_URL_OR_ID not set`);
  }

  if (trackedPlaylistId) {
    const playlistsBefore = await fetchPlaylists();
    const trackedBefore = findTrackedPlaylist(playlistsBefore, trackedPlaylistId);
    const previousRefreshAt = trackedBefore?.last_meta_refresh_at || null;
    const refreshStart = Date.now();
    const refreshResponse = await request(
      `/api/playlists/${trackedPlaylistId}/refresh-stats`,
      { method: 'POST' },
      10_000
    );
    const refreshElapsed = Date.now() - refreshStart;
    const { parsed: refreshParsed, text: refreshText } = await parseJsonSafely(refreshResponse);
    assert(
      refreshResponse.status === 200,
      `Expected 200 for refresh stats, got ${refreshResponse.status} ${refreshText}`
    );
    assert(
      refreshElapsed <= 2000,
      `Expected refresh stats to respond in <=2s, got ${refreshElapsed}ms`
    );
    assert(
      refreshParsed?.ok,
      `Expected ok=true for refresh stats, got ${refreshText}`
    );
    assert(
      refreshParsed?.job_id,
      `Expected job_id for refresh stats, got ${refreshText}`
    );
    if (refreshParsed?.status !== 'already_running') {
      await waitForPlaylistRefresh(trackedPlaylistId, previousRefreshAt);
    }
    results.push('PASS: POST /api/playlists/{id}/refresh-stats');
  } else {
    console.warn(`${DEBUG_LOG_PREFIX} skipping refresh stats: TEST_TRACKED_PLAYLIST_ID not set`);
  }

  if (trackedPlaylistId) {
    const scanResponse = await request(
      '/api/basic-rank-checker/scans',
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tracked_playlist_id: trackedPlaylistId }),
      },
      30_000
    );
    const { parsed: scanData, text: scanText } = await parseJsonSafely(scanResponse);
    assert(
      scanResponse.status === 200,
      `Expected 200 for /api/basic-rank-checker/scans, got ${scanResponse.status} ${scanText}`
    );
    assert(
      scanData?.scan_id,
      `Expected scan_id for /api/basic-rank-checker/scans, got ${scanText}`
    );
    const eventsUrl = new URL(
      `/api/basic-rank-checker/scans/${scanData.scan_id}/events`,
      baseUrl
    ).toString();
    await waitForSseCompletion(eventsUrl, 'basic scan');
    results.push('PASS: basic scan SSE completed');
  } else {
    console.warn(`${DEBUG_LOG_PREFIX} skipping basic scan: TEST_TRACKED_PLAYLIST_ID not set`);
  }

  if (testPlaylistUrl) {
    const manualResponse = await request(
      '/api/scans/manual',
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          playlist_url: testPlaylistUrl,
          target_keywords: ['test'],
          target_countries: ['US'],
        }),
      },
      30_000
    );
    const { parsed: manualData, text: manualText } = await parseJsonSafely(manualResponse);
    assert(
      manualResponse.status === 200,
      `Expected 200 for /api/scans/manual, got ${manualResponse.status} ${manualText}`
    );
    assert(
      manualData?.scan_id,
      `Expected scan_id for /api/scans/manual, got ${manualText}`
    );
    const manualEventsUrl = new URL(
      `/api/basic-rank-checker/scans/${manualData.scan_id}/events`,
      baseUrl
    ).toString();
    await waitForSseCompletion(manualEventsUrl, 'manual scan');
    results.push('PASS: manual scan SSE completed');
  } else {
    console.warn(`${DEBUG_LOG_PREFIX} skipping manual scan: TEST_PLAYLIST_URL_OR_ID not set`);
  }

  results.forEach((line) => console.log(line));
};

run().catch((error) => {
  console.error(`FAIL: ${error.message}`);
  process.exit(1);
});
