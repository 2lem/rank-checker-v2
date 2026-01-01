export const parseKeywords = (raw = "") => {
  const normalized = String(raw).replace(/\r\n?/g, "\n");
  const entries = normalized
    .split(/[,\n]+/g)
    .map((value) => value.trim())
    .filter(Boolean);
  const unique = [];
  const seen = new Set();
  entries.forEach((entry) => {
    const key = entry.toLowerCase();
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    unique.push(entry);
  });
  return unique;
};

if (typeof window !== "undefined") {
  window.parseKeywords = parseKeywords;
}
