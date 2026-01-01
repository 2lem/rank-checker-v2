import { expect, test } from '@playwright/test';
import fs from 'fs';
import path from 'path';

const BASE_URL =
  process.env.BASE_URL || 'https://rank-checker-v2-production.up.railway.app';
const BASE_URL_CLEAN = BASE_URL.replace(/\/$/, '');

const KEYWORDS = [
  'alpha jog',
  'bravo run',
  'charlie workout',
  'delta chill',
  'echo focus',
  'foxtrot lofi',
  'golf cardio',
  'hotel stretch',
  'india energy',
  'juliet motivation',
];

const ARTIFACT_PATH = path.join(
  'artifacts',
  'prod_manual_scan_1x10_no_playlist.json'
);

const RESULTS_TIMEOUT_MS = 20 * 60 * 1000;

const resolveCountryValue = async (page: import('@playwright/test').Page) => {
  return page.evaluate(() => {
    const select = document.querySelector(
      '[data-manual-scan-country-select]'
    ) as HTMLSelectElement | null;
    if (!select) {
      return null;
    }
    const options = Array.from(select.options);
    const match = options.find((option) => {
      const value = option.value.toLowerCase();
      const label = (option.textContent || '').toLowerCase();
      return value === 'us' || label.includes('united states');
    });
    return match?.value || null;
  });
};

const classifyLimiterVerdict = (metrics: {
  peak_rps: number | null;
  avg_rps: number | null;
  min_inter_start_s: number | null;
  any_429_count: number;
}) => {
  const overRps =
    (metrics.peak_rps !== null && metrics.peak_rps > 2.0) ||
    (metrics.avg_rps !== null && metrics.avg_rps > 2.0) ||
    metrics.any_429_count > 0;
  if (overRps) {
    return 'NOT ACTIVE';
  }
  if (metrics.min_inter_start_s !== null && metrics.min_inter_start_s >= 0.45) {
    return 'ACTIVE';
  }
  return 'PARTIAL';
};

const classifySafetyVerdict = (metrics: {
  peak_rps: number | null;
  avg_rps: number | null;
  min_inter_start_s: number | null;
  any_429_count: number;
}) => {
  const overRps =
    (metrics.peak_rps !== null && metrics.peak_rps > 2.0) ||
    (metrics.avg_rps !== null && metrics.avg_rps > 2.0) ||
    metrics.any_429_count > 0;
  if (overRps) {
    return 'NOT SAFE';
  }
  if (metrics.min_inter_start_s !== null && metrics.min_inter_start_s >= 0.45) {
    return 'SAFE';
  }
  return 'BORDERLINE';
};

const parseMaybeNumber = (value: unknown) => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  return null;
};

const parseMaybeInt = (value: unknown) => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  return null;
};

test('Prod manual scan (1 country, 10 keywords, no playlist)', async ({
  page,
  request,
}) => {
  test.setTimeout(RESULTS_TIMEOUT_MS + 60_000);

  let scanId: string | null = null;
  let scanStartedCount = 0;
  let postDurationMs: number | null = null;
  let postStartedAt: number | null = null;

  page.on('request', (req) => {
    if (req.method() === 'POST' && req.url().includes('/api/scans/manual')) {
      scanStartedCount += 1;
      if (postStartedAt === null) {
        postStartedAt = Date.now();
      }
    }
  });

  page.on('response', async (res) => {
    if (
      res.request().method() === 'POST' &&
      res.url().includes('/api/scans/manual')
    ) {
      if (postStartedAt !== null && postDurationMs === null) {
        postDurationMs = Date.now() - postStartedAt;
      }
      try {
        const data = await res.json();
        if (data && typeof data === 'object' && 'scan_id' in data) {
          scanId = String((data as { scan_id: string }).scan_id);
        }
      } catch (error) {
        // Ignore JSON parse errors; scanId will be validated later.
      }
    }
  });

  await page.goto('/');

  const manualToggle = page.locator('#manual-scan-toggle');
  await expect(manualToggle).toBeVisible();
  await manualToggle.click();

  const playlistInput = page.locator('[data-manual-scan-playlist]');
  await expect(playlistInput).toBeVisible();

  const countrySelect = page.locator('[data-manual-scan-country-select]');
  await expect(countrySelect).toBeVisible();
  await page.waitForFunction(() => {
    const select = document.querySelector(
      '[data-manual-scan-country-select]'
    ) as HTMLSelectElement | null;
    return select ? select.options.length > 1 : false;
  });

  const countryValue = await resolveCountryValue(page);
  if (!countryValue) {
    throw new Error('Could not resolve United States option in country list.');
  }
  await countrySelect.selectOption({ value: countryValue });
  await page.locator('[data-manual-scan-country-add]').click();

  const countryChips = page.locator('[data-manual-scan-country-pills] .chip');
  await expect(countryChips).toHaveCount(1);

  const keywordInput = page.locator('[data-manual-scan-keyword-input]');
  const keywordAdd = page.locator('[data-manual-scan-keyword-add]');
  const keywordChips = page.locator('[data-manual-scan-keyword-pills] .chip');
  await expect(keywordInput).toBeVisible();
  await expect(keywordAdd).toBeVisible();

  for (const [index, keyword] of KEYWORDS.entries()) {
    await keywordInput.fill(keyword);
    await keywordAdd.click();
    await expect(keywordChips).toHaveCount(index + 1);
    await expect(keywordChips.nth(index)).toContainText(keyword);
  }

  await expect(playlistInput).toHaveValue('');

  const runButton = page.locator('[data-manual-scan-run]');
  await expect(runButton).toBeVisible();
  await runButton.click();

  const progressWrap = page.locator('[data-manual-scan-progress]');
  const progressStatus = page.locator('[data-manual-scan-progress-status]');
  await expect(progressWrap).toBeVisible({ timeout: 10_000 });
  await expect(progressStatus).toContainText(/starting|scan|progress|running/i, {
    timeout: 10_000,
  });

  await page.waitForFunction(
    () => {
      const results = document.querySelector(
        '[data-manual-scan-results]'
      ) as HTMLElement | null;
      return Boolean(results && !results.hidden);
    },
    { timeout: RESULTS_TIMEOUT_MS }
  );

  const summaryLead = page.locator('[data-manual-scan-summary-lead]');
  await expect(summaryLead).toHaveText(/.+/);

  const connectionStatus = (await progressStatus.textContent()) || '';
  if (connectionStatus.toLowerCase().includes('connection lost')) {
    throw new Error('Manual scan completed with a connection lost warning.');
  }

  if (scanStartedCount !== 1) {
    throw new Error(`Expected 1 manual scan start, saw ${scanStartedCount}.`);
  }

  if (!scanId) {
    throw new Error('Manual scan did not return a scan_id.');
  }

  const scanPayload = (await (async () => {
    const deadline = Date.now() + RESULTS_TIMEOUT_MS;
    let lastPayload: unknown = null;
    while (Date.now() < deadline) {
      const response = await request.get(
        `${BASE_URL_CLEAN}/api/basic-rank-checker/scans/${scanId}`
      );
      if (response.ok()) {
        const data = (await response.json()) as { status?: string };
        lastPayload = data;
        const statusValue = data?.status || '';
        if (['completed', 'completed_partial'].includes(statusValue)) {
          return data;
        }
        if (['failed', 'cancelled'].includes(statusValue)) {
          throw new Error(
            `Scan did not complete successfully (status=${statusValue}).`
          );
        }
      }
      await page.waitForTimeout(10_000);
    }
    throw new Error(
      `Timed out waiting for scan completion. Last payload: ${JSON.stringify(
        lastPayload
      )}`
    );
  })()) as {
    status?: string;
    started_at?: string;
    finished_at?: string;
    spotify_total_calls?: number;
    peak_rps?: number;
    avg_rps?: number;
    min_inter_start_s?: number;
    any_429_count?: number;
  };

  const status = scanPayload.status || '';
  if (!['completed', 'completed_partial'].includes(status)) {
    throw new Error(`Scan did not complete successfully (status=${status}).`);
  }

  const startedAt = scanPayload.started_at
    ? new Date(scanPayload.started_at)
    : null;
  const finishedAt = scanPayload.finished_at
    ? new Date(scanPayload.finished_at)
    : null;
  const scanTotalDurationS =
    startedAt && finishedAt
      ? Number(((finishedAt.getTime() - startedAt.getTime()) / 1000).toFixed(2))
      : null;

  const spotifyTotalCalls = parseMaybeInt(scanPayload.spotify_total_calls);
  const peakRps = parseMaybeNumber(scanPayload.peak_rps);
  const avgRps = parseMaybeNumber(scanPayload.avg_rps);
  const minInterStartS = parseMaybeNumber(scanPayload.min_inter_start_s);
  const any429Count = parseMaybeInt(scanPayload.any_429_count) ?? 0;

  if (any429Count > 0) {
    throw new Error(`Spotify 429s detected: ${any429Count}.`);
  }
  if (peakRps !== null && peakRps > 2.0) {
    throw new Error(`peak_rps too high: ${peakRps}.`);
  }
  if (avgRps !== null && avgRps > 2.0) {
    throw new Error(`avg_rps too high: ${avgRps}.`);
  }

  const limiterVerdict = classifyLimiterVerdict({
    peak_rps: peakRps,
    avg_rps: avgRps,
    min_inter_start_s: minInterStartS,
    any_429_count: any429Count,
  });

  const safetyVerdict = classifySafetyVerdict({
    peak_rps: peakRps,
    avg_rps: avgRps,
    min_inter_start_s: minInterStartS,
    any_429_count: any429Count,
  });

  const timeoutVerdict =
    postDurationMs !== null && postDurationMs < 1000
      ? 'CONFIRMED'
      : 'UNCONFIRMED';

  const artifact = {
    base_url: BASE_URL,
    country: 'us',
    keywords: KEYWORDS,
    playlist_input_used: false,
    scan_started_count: scanStartedCount,
    scan_id: scanId,
    post_duration_ms: postDurationMs,
    scan_total_duration_s: scanTotalDurationS,
    spotify_total_calls: spotifyTotalCalls,
    peak_rps: peakRps,
    avg_rps: avgRps,
    min_inter_start_s: minInterStartS,
    any_429_count: any429Count,
    timeout_verdict: timeoutVerdict,
    limiter_verdict: limiterVerdict,
    safety_verdict: safetyVerdict,
  };

  fs.mkdirSync(path.dirname(ARTIFACT_PATH), { recursive: true });
  fs.writeFileSync(ARTIFACT_PATH, JSON.stringify(artifact, null, 2));
});
