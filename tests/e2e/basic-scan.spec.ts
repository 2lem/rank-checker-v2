import { expect, test } from '@playwright/test';
import { attachSseStateOnFailure, installSseObserver, waitForSseCompletion } from './sse-helpers';

test('Basic scan completes without hanging', async ({ page }) => {
  test.setTimeout(90_000);
  const playlistId = process.env.TEST_TRACKED_PLAYLIST_ID;
  const scanPath = process.env.BASIC_SCAN_PATH || (playlistId ? `/playlists/${playlistId}` : null);
  test.skip(!scanPath, 'BASIC_SCAN_PATH or TEST_TRACKED_PLAYLIST_ID is not set');

  await installSseObserver(page);

  await page.goto(scanPath);

  const toggle = page.locator('[data-basic-scan-toggle]');
  if (await toggle.isVisible()) {
    await toggle.click();
  }

  const runButton = page.locator('[data-basic-scan-run]');
  await expect(runButton).toBeVisible();
  await runButton.click();

  const progress = page.locator('[data-basic-scan-progress]');
  await expect(progress).toBeVisible({ timeout: 10000 });

  await waitForSseCompletion(page, 45_000);

  const statusArea = page.locator('#playlist-status');
  await expect(statusArea).toContainText('Basic Scan completed', { timeout: 30000 });
  await expect(statusArea).not.toContainText('connection lost');

  await page.waitForFunction(
    () => {
      const progressEl = document.querySelector('[data-basic-scan-progress]');
      const resultsEl = document.querySelector('[data-basic-scan-results]');
      const statusEl = document.querySelector('#playlist-status');
      const progressHidden = progressEl ? progressEl.hidden : true;
      const resultsVisible = resultsEl ? !resultsEl.hidden : false;
      const statusError = statusEl ? statusEl.classList.contains('status-error') : false;
      return progressHidden || resultsVisible || statusError;
    },
    { timeout: 30000 }
  );

  const results = page.locator('[data-basic-scan-results]');
  await expect(results).toBeVisible({ timeout: 10000 });
});

test.afterEach(async ({ page }, testInfo) => {
  await attachSseStateOnFailure(page, testInfo);
});
