const baseUrl = process.env.BASE_URL;

if (!baseUrl) {
  console.error('FAIL: BASE_URL environment variable is required');
  process.exit(1);
}

const TIMEOUT_MS = 10_000;

const withTimeout = async (promise, label) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    return await promise(controller.signal);
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`${label} timed out after ${TIMEOUT_MS}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
};

const request = async (path, options = {}) => {
  const url = new URL(path, baseUrl).toString();
  return withTimeout(
    (signal) =>
      fetch(url, {
        ...options,
        signal,
      }),
    `${options.method || 'GET'} ${path}`
  );
};

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

const run = async () => {
  const results = [];

  const healthResponse = await request('/health');
  assert(healthResponse.status === 200, `Expected 200 for /health, got ${healthResponse.status}`);
  results.push('PASS: GET /health');

  const openApiResponse = await request('/openapi.json');
  assert(openApiResponse.status === 200, `Expected 200 for /openapi.json, got ${openApiResponse.status}`);
  results.push('PASS: GET /openapi.json');

  const scanResponse = await request('/api/basic-rank-checker/scans', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  });
  const scanBody = await scanResponse.text();
  assert(scanResponse.status === 400, `Expected 400 for /api/basic-rank-checker/scans, got ${scanResponse.status}`);
  assert(
    scanBody.includes('tracked_playlist_id is required'),
    'Expected validation message to include "tracked_playlist_id is required"'
  );
  results.push('PASS: POST /api/basic-rank-checker/scans validation');

  results.forEach((line) => console.log(line));
};

run().catch((error) => {
  console.error(`FAIL: ${error.message}`);
  process.exit(1);
});
