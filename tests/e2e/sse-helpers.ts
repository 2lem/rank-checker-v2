import type { Page, TestInfo } from '@playwright/test';

type SseLogEntry = {
  url: string;
  readyState: number;
  lastEvent: string | null;
  lastEventAt: number | null;
  errorCount: number;
};

type SseLogState = {
  instances: SseLogEntry[];
};

declare global {
  interface Window {
    __e2eSseLog?: SseLogState;
  }
}

export const installSseObserver = async (page: Page) => {
  await page.addInitScript(() => {
    const OriginalEventSource = window.EventSource;
    if (!OriginalEventSource) {
      return;
    }
    window.__e2eSseLog = { instances: [] };

    window.EventSource = function EventSourceProxy(url, config) {
      const instance = new OriginalEventSource(url, config);
      const entry = {
        url: typeof url === 'string' ? url : String(url),
        readyState: instance.readyState,
        lastEvent: null,
        lastEventAt: null,
        errorCount: 0,
      };
      window.__e2eSseLog.instances.push(entry);

      const refreshState = () => {
        entry.readyState = instance.readyState;
      };

      instance.addEventListener('open', () => {
        refreshState();
      });
      instance.addEventListener('message', (event) => {
        entry.lastEvent = event.data || null;
        entry.lastEventAt = Date.now();
        refreshState();
      });
      instance.addEventListener('error', () => {
        entry.errorCount += 1;
        refreshState();
      });
      return instance;
    };

    window.EventSource.prototype = OriginalEventSource.prototype;
  });
};

export const waitForSseCompletion = async (page: Page, timeoutMs = 45_000) => {
  await page.waitForFunction(
    () => {
      const log = window.__e2eSseLog;
      if (!log || !log.instances.length) {
        return false;
      }
      const lastEntry = log.instances[log.instances.length - 1];
      const data = lastEntry.lastEvent || '';
      if (!data) {
        return false;
      }
      const lowered = data.toLowerCase();
      return (
        lowered.includes('"type":"done"') ||
        lowered.includes('"type":"error"') ||
        lowered.includes('completed') ||
        lowered.includes('failed')
      );
    },
    { timeout: timeoutMs }
  );
};

export const attachSseStateOnFailure = async (page: Page, testInfo: TestInfo) => {
  if (testInfo.status === testInfo.expectedStatus) {
    return;
  }
  try {
    const state = await page.evaluate(() => window.__e2eSseLog || null);
    await testInfo.attach('sse-state', {
      body: JSON.stringify(state, null, 2),
      contentType: 'application/json',
    });
  } catch (error) {
    await testInfo.attach('sse-state', {
      body: JSON.stringify({ error: String(error) }, null, 2),
      contentType: 'application/json',
    });
  }
};
