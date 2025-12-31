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
  const followersValue = page.locator('[data-playlist-followers]');
  const scannedValue = page.locator('[data-playlist-scanned]');
  const updatedValue = page.locator('[data-playlist-updated]');

  await expect(refreshButton).toBeVisible();
  await expect(followersValue).toBeVisible();

  const initialFollowers = (await followersValue.textContent())?.trim() ?? '';
  const initialScanned = (await scannedValue.textContent())?.trim() ?? '';
  const initialUpdated = (await updatedValue.textContent())?.trim() ?? '';

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
  await page.waitForFunction(
    ({ followers, scanned, updated }) => {
      const followersEl = document.querySelector('[data-playlist-followers]');
      const scannedEl = document.querySelector('[data-playlist-scanned]');
      const updatedEl = document.querySelector('[data-playlist-updated]');
      const currentFollowers = followersEl?.textContent?.trim() || '';
      const currentScanned = scannedEl?.textContent?.trim() || '';
      const currentUpdated = updatedEl?.textContent?.trim() || '';
      return (
        currentFollowers !== followers ||
        currentScanned !== scanned ||
        currentUpdated !== updated
      );
    },
    { followers: initialFollowers, scanned: initialScanned, updated: initialUpdated },
    { timeout: 60000 }
  );
});

test.afterEach(async ({ page }, testInfo) => {
  await attachSseStateOnFailure(page, testInfo);
});
