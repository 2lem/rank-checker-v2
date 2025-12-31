import { expect, test } from '@playwright/test';
import { attachSseStateOnFailure, installSseObserver, waitForSseCompletion } from './sse-helpers';

test('Manual scan completes without hanging', async ({ page }) => {
  test.setTimeout(90_000);
  const playlistId = process.env.TEST_TRACKED_PLAYLIST_ID;
  const playlistUrl = process.env.TEST_PLAYLIST_URL_OR_ID;
  test.skip(!playlistUrl, 'TEST_PLAYLIST_URL_OR_ID is not set');

  await installSseObserver(page);

  const primaryPath = playlistId ? `/playlists/${playlistId}` : '/';
  await page.goto(primaryPath);

  let manualToggle = page.locator('#manual-scan-toggle');
  if (!(await manualToggle.isVisible())) {
    await page.goto('/');
    manualToggle = page.locator('#manual-scan-toggle');
  }

  await expect(manualToggle).toBeVisible();
  await manualToggle.click();

  const playlistInput = page.locator('[data-manual-scan-playlist]');
  await expect(playlistInput).toBeVisible();
  await playlistInput.fill(playlistUrl);

  const countrySelect = page.locator('[data-manual-scan-country-select]');
  await expect(countrySelect).toBeVisible();
  await page.waitForFunction(() => {
    const select = document.querySelector('[data-manual-scan-country-select]');
    return select && select.options.length > 1;
  });
  await countrySelect.selectOption({ index: 1 });
  await page.locator('[data-manual-scan-country-add]').click();

  const keywordInput = page.locator('[data-manual-scan-keyword-input]');
  await expect(keywordInput).toBeVisible();
  await keywordInput.fill('entrenar');
  await page.locator('[data-manual-scan-keyword-add]').click();

  const runButton = page.locator('[data-manual-scan-run]');
  await expect(runButton).toBeVisible();
  await runButton.click();

  const progress = page.locator('[data-manual-scan-progress]');
  await expect(progress).toBeVisible({ timeout: 10000 });

  const progressStatus = page.locator('[data-manual-scan-progress-status]');
  await expect(progressStatus).toContainText(/scan|starting/i, { timeout: 10000 });

  await waitForSseCompletion(page, 45_000);

  await page.waitForFunction(
    () => {
      const progressEl = document.querySelector('[data-manual-scan-progress]');
      const resultsEl = document.querySelector('[data-manual-scan-results]');
      const statusEl = document.querySelector('#playlist-status');
      const progressHidden = progressEl ? progressEl.hidden : true;
      const resultsVisible = resultsEl ? !resultsEl.hidden : false;
      const statusError = statusEl ? statusEl.classList.contains('status-error') : false;
      return progressHidden || resultsVisible || statusError;
    },
    { timeout: 30000 }
  );

  const status = page.locator('#playlist-status');
  await expect(status).not.toHaveClass(/status-loading/);
});

test.afterEach(async ({ page }, testInfo) => {
  await attachSseStateOnFailure(page, testInfo);
});
