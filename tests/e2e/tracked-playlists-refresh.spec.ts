import { expect, test } from '@playwright/test';
import { attachSseStateOnFailure, installSseObserver } from './sse-helpers';

const waitForPlaylistRefresh = async (page, playlistId, previousTimestamp) => {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const response = await page.request.get('/api/playlists');
    const payload = await response.json();
    const tracked = Array.isArray(payload)
      ? payload.find((item) => String(item.id) === String(playlistId))
      : null;
    const currentTimestamp = tracked?.last_meta_refresh_at;
    if (currentTimestamp && currentTimestamp !== previousTimestamp) {
      return currentTimestamp;
    }
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
  throw new Error('Refresh job did not update last_meta_refresh_at within timeout.');
};

test('Tracked playlist refresh completes', async ({ page }) => {
  test.setTimeout(60_000);
  const playlistId = process.env.TEST_TRACKED_PLAYLIST_ID;
  test.skip(!playlistId, 'TEST_TRACKED_PLAYLIST_ID is not set');

  await installSseObserver(page);

  await page.goto(`/playlists/${playlistId}`);

  const status = page.locator('#playlist-status');
  const refreshButton = page.locator('[data-refresh-stats]');

  await expect(refreshButton).toBeVisible();
  const playlistsResponse = await page.request.get('/api/playlists');
  const playlistsPayload = await playlistsResponse.json();
  const trackedBefore = Array.isArray(playlistsPayload)
    ? playlistsPayload.find((item) => String(item.id) === String(playlistId))
    : null;
  const previousRefreshAt = trackedBefore?.last_meta_refresh_at || null;

  const refreshStart = Date.now();
  const [refreshResponse] = await Promise.all([
    page.waitForResponse((response) =>
      response.url().includes(`/api/playlists/${playlistId}/refresh-stats`)
    ),
    refreshButton.click(),
  ]);
  const refreshElapsed = Date.now() - refreshStart;
  expect(refreshElapsed).toBeLessThanOrEqual(2000);
  expect(refreshResponse.ok()).toBeTruthy();
  const refreshPayload = await refreshResponse.json();
  expect(refreshPayload?.ok).toBeTruthy();
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
  if (refreshPayload?.status !== 'already_running') {
    await waitForPlaylistRefresh(page, playlistId, previousRefreshAt);
  }
});

test.afterEach(async ({ page }, testInfo) => {
  await attachSseStateOnFailure(page, testInfo);
});
