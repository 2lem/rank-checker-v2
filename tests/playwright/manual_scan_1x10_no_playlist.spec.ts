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
  let postStartedAt: number | null = null;
  let postEndedAt: number | null = null;

  page.on('request', (request) => {
    if (request.method() === 'POST' && request.url().includes('/api/scans/manual')) {
      postStartedAt = Date.now();
    }
  });

  page.on('response', async (response) => {
    const request = response.request();
    if (request.method() !== 'POST' || !response.url().includes('/api/scans/manual')) {
      return;
    }
    scanStartedCount += 1;
    postEndedAt = Date.now();
    try {
      const data = await response.json();
      if (data && typeof data === 'object' && 'scan_id' in data) {
        scanId = typeof data.scan_id === 'string' ? data.scan_id : String(data.scan_id);
      }
    } catch (error) {
      if (error) {
        // Ignore JSON parse failures for non-JSON error responses.
      }
    }
  });

  await page.goto('/');

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
  await runManualScanButton.click();

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

  const scanEndAt = Date.now();

  expect(scanStartedCount).toBe(1);

  const postDurationMs = postStartedAt && postEndedAt ? postEndedAt - postStartedAt : null;
  const scanTotalDurationS = Math.round((scanEndAt - scanStartAt) / 1000);

  const artifactPayload = {
    base_url: process.env.BASE_URL ?? null,
    country: 'us',
    keywords,
    playlist_input_used: false,
    scan_started_count: scanStartedCount,
    scan_id: scanId,
    post_duration_ms: postDurationMs,
    scan_total_duration_s: scanTotalDurationS,
  };

  const artifactsDir = path.join(process.cwd(), 'artifacts');
  await fs.mkdir(artifactsDir, { recursive: true });
  await fs.writeFile(
    path.join(artifactsDir, 'prod_manual_scan_1x10_no_playlist.json'),
    JSON.stringify(artifactPayload, null, 2),
    'utf8'
  );
});
