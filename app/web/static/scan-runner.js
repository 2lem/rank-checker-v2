(() => {
  const normalizePlaylistUrl = (value) => (value || "").trim().split("?", 1)[0];
  const extractPlaylistId = (text) => {
    const match = (text || "").match(
      /(?:open\.spotify\.com\/playlist\/|spotify:playlist:)([A-Za-z0-9]+)/
    );
    return match ? match[1] : null;
  };

  const defaultFormatTimestamp = (value) => {
    if (!value) {
      return "—";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return value;
    }
    return new Intl.DateTimeFormat("sv-SE", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    }).format(date);
  };

  const defaultFormatSummaryDateParts = (value) => {
    if (!value) {
      return { dateLabel: "—", timeLabel: "—" };
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return { dateLabel: value, timeLabel: value };
    }
    const dateLabel = date.toLocaleDateString("en-GB", {
      day: "numeric",
      month: "long",
      year: "numeric",
    });
    const timeLabel = date.toLocaleTimeString("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
    return { dateLabel, timeLabel };
  };

  const createBasicScanController = (options) => {
    if (!window.BasicScan?.init) {
      return null;
    }
    const mergedOptions = {
      formatTimestamp: options?.formatTimestamp || defaultFormatTimestamp,
      formatSummaryDateParts: options?.formatSummaryDateParts || defaultFormatSummaryDateParts,
      ...options,
    };
    return window.BasicScan.init(mergedOptions);
  };

  window.ScanRunner = {
    normalizePlaylistUrl,
    extractPlaylistId,
    defaultFormatTimestamp,
    defaultFormatSummaryDateParts,
    createBasicScanController,
  };
})();
