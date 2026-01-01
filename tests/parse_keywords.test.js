import assert from "node:assert/strict";
import { test } from "node:test";

import { parseKeywords } from "../app/web/static/keyword_utils.js";

test("parseKeywords splits newline-separated keywords into tokens", () => {
  const input = [
    "musik zum joggen",
    "musik beim joggen",
    "musik zum laufen",
    "musik beim laufen",
    "lieder zum joggen",
    "playlist zum laufen",
  ].join("\n");

  assert.deepEqual(parseKeywords(input), [
    "musik zum joggen",
    "musik beim joggen",
    "musik zum laufen",
    "musik beim laufen",
    "lieder zum joggen",
    "playlist zum laufen",
  ]);
});

test("parseKeywords splits comma-separated keywords", () => {
  const input = "lofi, focus, chill";
  assert.deepEqual(parseKeywords(input), ["lofi", "focus", "chill"]);
});

test("parseKeywords splits mixed comma and newline inputs", () => {
  const input = "lofi, focus\nchill,\nsleep";
  assert.deepEqual(parseKeywords(input), ["lofi", "focus", "chill", "sleep"]);
});

test("parseKeywords preserves internal spaces and de-duplicates case-insensitively", () => {
  const input = "musik zum joggen\nMusik zum joggen\nmusik beim laufen";
  assert.deepEqual(parseKeywords(input), ["musik zum joggen", "musik beim laufen"]);
});
