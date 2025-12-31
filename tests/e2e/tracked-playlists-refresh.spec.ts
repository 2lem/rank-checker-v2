import { expect, test } from '@playwright/test';
import { attachSseStateOnFailure, installSseObserver } from './sse-helpers';

test('Tracked playlist refresh completes', async ({ page }) => {
  test.setTimeout(60_000);
  const playlistId = process.env.TEST_TRACKED_PLAYLIST_ID;
  test.skip(!playlistId, 'TEST_TRACKED_PLAYLIST_ID is not set');

  await installSseObserver(page);

  await page.goto(`/playlists/${playlistId}`);

  const status = page.locator('#playlist-status');
  const refreshButton = page.locator('[data-refresh-stats]');

  await expect(refreshButton).toBeVisible();
  await refreshButton.click();
  await expect(status).toContainText('Fetching data', { timeout: 5000 });

  await Promise.race([
    page
      .waitForNavigation({ waitUntil: 'load', timeout: 15000 })
      .catch(() => null),
    page.waitForFunction(
      () => {
        const statusEl = document.querySelector('#playlist-status');
        return statusEl ? !statusEl.classList.contains('status-loading') : false;
      },
      { timeout: 30000 }
    ),
  ]);

  await expect(status).not.toHaveClass(/status-loading/);
});

test.afterEach(async ({ page }, testInfo) => {
  await attachSseStateOnFailure(page, testInfo);
});
