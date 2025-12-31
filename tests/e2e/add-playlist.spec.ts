import { expect, test } from '@playwright/test';
import { attachSseStateOnFailure, installSseObserver } from './sse-helpers';

test('Add playlist submission completes', async ({ page }) => {
  test.setTimeout(60_000);
  const playlistInput = process.env.TEST_PLAYLIST_URL_OR_ID;
  const addPath = process.env.ADD_PLAYLIST_PATH || '/';
  test.skip(!playlistInput, 'TEST_PLAYLIST_URL_OR_ID is not set');

  await installSseObserver(page);

  await page.goto(addPath);

  const addButton = page.locator('[data-panel-target="add-playlist"]');
  if (await addButton.isVisible()) {
    await addButton.click();
  }

  const input = page.locator('[data-playlist-input]');
  await expect(input).toBeVisible();
  await input.fill(playlistInput);

  const form = page.locator('form[data-form="add-playlist"]');
  await form.locator('button[type="submit"]').click();

  const status = page.locator('#playlist-status');
  await expect(status).toContainText('Adding playlist', { timeout: 5000 });

  await Promise.race([
    page.waitForNavigation({ waitUntil: 'load', timeout: 15000 }).catch(() => null),
    page.waitForFunction(
      () => {
        const statusEl = document.querySelector('#playlist-status');
        if (!statusEl) {
          return false;
        }
        const loadingCleared = !statusEl.classList.contains('status-loading');
        const hasSuccess = statusEl.classList.contains('status-success');
        const hasError = statusEl.classList.contains('status-error');
        return loadingCleared && (hasSuccess || hasError);
      },
      { timeout: 30000 }
    ),
  ]);

  await expect(status).not.toHaveClass(/status-loading/);
  await expect(status).toHaveClass(/status-(success|error)/);
});

test.afterEach(async ({ page }, testInfo) => {
  await attachSseStateOnFailure(page, testInfo);
});
