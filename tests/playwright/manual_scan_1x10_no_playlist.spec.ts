import { expect, test } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';

test('manual scan 1x10 no playlist', async ({ page }) => {
  test.setTimeout(20 * 60 * 1000);

  const keywords = [
    'signal',
    'horizon',
    'drift',
    'summit',
    'pulse',
    'ember',
    'tides',
    'halo',
    'vector',
    'ascent',
  ];

  let scanStartedCount = 0;
  let scanId: string | null = null;
  await page.goto('/');
  const baseUrl = process.env.BASE_URL ?? new URL(page.url()).origin;

  page.on('response', (response) => {
    const request = response.request();
    if (request.method() !== 'POST' || !response.url().includes('/api/scans/manual')) {
      return;
    }
    scanStartedCount += 1;
  });

  const manualScanTab = page.getByTestId('manual-scan-tab');
  await expect(manualScanTab).toBeVisible();
  await manualScanTab.click();

  const playlistInput = page.getByTestId('playlist-input');
  await expect(playlistInput).toBeVisible();

  const countrySelect = page.getByTestId('country-input');
  await expect(countrySelect).toBeVisible();
  await page.waitForFunction(() => {
    const select = document.querySelector('[data-testid="country-input"]') as HTMLSelectElement | null;
    return Boolean(select && select.options.length > 1);
  });
  const countryValue = await countrySelect.evaluate((select) => {
    const option = Array.from(select.options).find((item) => item.value.toLowerCase() === 'us');
    return option?.value ?? null;
  });
  if (!countryValue) {
    throw new Error('Unable to locate US market option.');
  }
  await countrySelect.selectOption({ value: countryValue });
  await page.getByTestId('country-add-button').click();

  const keywordInput = page.getByTestId('keyword-input');
  const keywordAddButton = page.getByTestId('keyword-add-button');
  await expect(keywordInput).toBeVisible();
  await expect(keywordAddButton).toBeVisible();

  for (const keyword of keywords) {
    await keywordInput.fill(keyword);
    await keywordAddButton.click();
  }

  await expect(playlistInput).toHaveValue('');

  const scanStartAt = Date.now();
  const runManualScanButton = page.getByTestId('run-manual-scan-button');
  await expect(runManualScanButton).toBeVisible();
  const responsePromise = page.waitForResponse(
    (response) => response.request().method() === 'POST' && response.url().includes('/api/scans/manual')
  );
  const postStartedAt = Date.now();
  await runManualScanButton.click();
  const response = await responsePromise;
  const postEndedAt = Date.now();
  const postDurationMs = postEndedAt - postStartedAt;
  const responseData = await response.json();
  if (responseData && typeof responseData === 'object' && 'scan_id' in responseData) {
    scanId = typeof responseData.scan_id === 'string' ? responseData.scan_id : String(responseData.scan_id);
  }

  const scanStatus = page.getByTestId('scan-status');
  await expect(scanStatus).toBeVisible({ timeout: 10_000 });

  const resultsContainer = page.getByTestId('results-container');
  await expect(resultsContainer).toBeVisible({ timeout: 20 * 60 * 1000 });

  const summaryLead = page.getByTestId('empty-state');
  const summaryLeadText = (await summaryLead.textContent())?.trim() ?? '';
  if (summaryLeadText.length > 0) {
    await expect(summaryLead).toContainText(/no results/i);
  }

  const connectionLost = page.locator('text=/Connection lost/i');
  if (await connectionLost.count()) {
    await expect(connectionLost.first()).toBeHidden();
  }

  if (!scanId) {
    throw new Error('Manual scan did not return a scan_id.');
  }

  const statusUrl = new URL(`/api/basic-rank-checker/scans/${scanId}`, baseUrl).toString();
  let pollDelayMs = 2000;
  const maxPollDelayMs = 10000;
  const pollStart = Date.now();
  const maxPollDurationMs = 20 * 60 * 1000;
  let statusPayload: Record<string, unknown> | null = null;

  while (Date.now() - pollStart < maxPollDurationMs) {
    const statusResponse = await page.request.get(statusUrl);
    if (!statusResponse.ok()) {
      throw new Error(`Scan status request failed with ${statusResponse.status()}.`);
    }
    statusPayload = (await statusResponse.json()) as Record<string, unknown>;
    const status = statusPayload.status;
    if (status === 'completed' || status === 'failed' || status === 'cancelled') {
      break;
    }
    await page.waitForTimeout(pollDelayMs);
    pollDelayMs = Math.min(pollDelayMs + 1000, maxPollDelayMs);
  }

  if (!statusPayload) {
    throw new Error('Scan status polling never returned a payload.');
  }

  const statusValue = statusPayload.status;
  if (statusValue !== 'completed' && statusValue !== 'failed' && statusValue !== 'cancelled') {
    throw new Error('Scan status did not reach a terminal state within timeout.');
  }
  if (statusValue === 'failed' || statusValue === 'cancelled') {
    throw new Error(`Manual scan ended with status: ${statusValue}.`);
  }

  const scanEndAt = Date.now();

  expect(scanStartedCount).toBe(1);

  const scanTotalDurationS = Math.round((scanEndAt - scanStartAt) / 1000);

  const spotifyTotalCalls = Number(statusPayload.spotify_total_calls);
  const peakRps = Number(statusPayload.peak_rps);
  const avgRps = Number(statusPayload.avg_rps);
  const minInterStartS = Number(statusPayload.min_inter_start_s);
  const any429Count = Number(statusPayload.any_429_count);

  if (
    Number.isNaN(spotifyTotalCalls) ||
    Number.isNaN(peakRps) ||
    Number.isNaN(avgRps) ||
    Number.isNaN(minInterStartS) ||
    Number.isNaN(any429Count)
  ) {
    throw new Error('Scan status did not include required Spotify metrics.');
  }

  const timeoutVerdict = postDurationMs < 1000 ? 'CONFIRMED' : 'NOT CONFIRMED';
  let limiterVerdict: 'ACTIVE' | 'PARTIAL' | 'NOT ACTIVE' = 'NOT ACTIVE';
  if (peakRps <= 2.0 && minInterStartS >= 0.5 && any429Count === 0) {
    limiterVerdict = 'ACTIVE';
  } else if (avgRps <= 2 && peakRps > 2 && any429Count === 0) {
    limiterVerdict = 'PARTIAL';
  }

  let safetyVerdict: 'SAFE' | 'BORDERLINE' | 'NOT SAFE' = 'NOT SAFE';
  if (peakRps <= 2 && avgRps <= 2 && any429Count === 0) {
    safetyVerdict = 'SAFE';
  } else if (
    any429Count === 0 &&
    peakRps <= 2.5 &&
    avgRps <= 2.5 &&
    (peakRps > 2 || avgRps > 2)
  ) {
    safetyVerdict = 'BORDERLINE';
  }

  if (scanStartedCount !== 1) {
    throw new Error(`Expected exactly one manual scan, got ${scanStartedCount}.`);
  }
  if (any429Count > 0) {
    throw new Error(`Unsafe scan: any_429_count=${any429Count}.`);
  }
  if (peakRps > 2.0 || avgRps > 2.0) {
    throw new Error(`Unsafe scan: peak_rps=${peakRps} avg_rps=${avgRps}.`);
  }

  const artifactPayload = {
    base_url: baseUrl,
    country: 'us',
    keywords,
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

  const artifactsDir = path.join(process.cwd(), 'artifacts');
  await fs.mkdir(artifactsDir, { recursive: true });
  await fs.writeFile(
    path.join(artifactsDir, 'prod_manual_scan_1x10_no_playlist.json'),
    JSON.stringify(artifactPayload, null, 2),
    'utf8'
  );
});
