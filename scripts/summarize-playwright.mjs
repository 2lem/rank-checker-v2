import fs from 'node:fs';
import path from 'node:path';

const resultsPath = process.env.PLAYWRIGHT_RESULTS_PATH || 'playwright-report/results.json';
const resolvedPath = path.resolve(resultsPath);

const emojiForStatus = (status) => {
  switch (status) {
    case 'passed':
      return '✅';
    case 'skipped':
      return '⚠️';
    case 'failed':
    case 'timedOut':
    case 'interrupted':
      return '❌';
    default:
      return '⚠️';
  }
};

const collectTests = (suite, entries = []) => {
  if (!suite) {
    return entries;
  }
  if (Array.isArray(suite.specs)) {
    for (const spec of suite.specs) {
      for (const testCase of spec.tests || []) {
        const lastResult = testCase.results?.[testCase.results.length - 1];
        const status = lastResult?.status || testCase.expectedStatus || 'unknown';
        entries.push({
          title: spec.title || testCase.title || 'Unknown test',
          status,
        });
      }
    }
  }
  for (const child of suite.suites || []) {
    collectTests(child, entries);
  }
  return entries;
};

let summaryLines = ['- ⚠️ Playwright results not found'];
let overallStatus = 'failure';

if (fs.existsSync(resolvedPath)) {
  const data = JSON.parse(fs.readFileSync(resolvedPath, 'utf8'));
  const tests = collectTests(data);

  if (tests.length) {
    summaryLines = tests.map((testCase) => `${emojiForStatus(testCase.status)} ${testCase.title}`);
    overallStatus = tests.some((testCase) =>
      ['failed', 'timedOut', 'interrupted'].includes(testCase.status)
    )
      ? 'failure'
      : 'success';
  } else {
    summaryLines = ['- ⚠️ No Playwright tests were recorded'];
    overallStatus = 'failure';
  }
}

const summary = summaryLines.join('\n');

if (process.env.GITHUB_OUTPUT) {
  fs.appendFileSync(process.env.GITHUB_OUTPUT, `e2e_summary<<EOF\n${summary}\nEOF\n`);
  fs.appendFileSync(process.env.GITHUB_OUTPUT, `e2e_status=${overallStatus}\n`);
}

console.log(summary);
console.log(`Overall status: ${overallStatus}`);
