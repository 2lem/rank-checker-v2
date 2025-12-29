(() => {
  const clampScrollTarget = (target) => {
    const documentHeight = Math.max(
      document.body.scrollHeight,
      document.documentElement.scrollHeight
    );
    const maxScroll = Math.max(0, documentHeight - window.innerHeight);
    return Math.min(Math.max(0, target), maxScroll);
  };

  const scrollSectionToCenter = (section) => {
    if (!section) {
      return;
    }
    const rect = section.getBoundingClientRect();
    const sectionTop = window.scrollY + rect.top;
    const target = sectionTop - window.innerHeight / 2 + rect.height / 2;
    window.scrollTo({ top: clampScrollTarget(target), behavior: "smooth" });
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

  const defaultFormatFollowersValue = (value) => {
    if (value === undefined || value === null || value === "") {
      return "—";
    }
    if (typeof value === "object") {
      if ("total" in value) {
        return defaultFormatFollowersValue(value.total);
      }
      return "—";
    }
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      return numeric.toLocaleString();
    }
    return value;
  };

  const flagEmoji = (code) => {
    const upper = (code || "").toUpperCase();
    if (upper.length !== 2) {
      return "";
    }
    return upper.replace(/./g, (char) => String.fromCodePoint(127397 + char.charCodeAt()));
  };

  const formatFileTimestamp = (value) => {
    if (!value) {
      return "";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return "";
    }
    const pad = (part) => String(part).padStart(2, "0");
    const year = date.getFullYear();
    const month = pad(date.getMonth() + 1);
    const day = pad(date.getDate());
    const hours = pad(date.getHours());
    const minutes = pad(date.getMinutes());
    return `${day}-${month}-${year}_${hours}-${minutes}`;
  };

  const resolveFollowers = (result, formatFollowersValueFn) => {
    const fallback = formatFollowersValueFn || defaultFormatFollowersValue;
    const nestedFollowers =
      result.followers && typeof result.followers === "object"
        ? result.followers.total
        : undefined;
    const nestedPlaylistFollowers =
      result.playlist && typeof result.playlist === "object"
        ? result.playlist.followers || result.playlist.followers_total || result.playlist.follower_count
        : undefined;
    const nestedPlaylistFollowersTotal =
      nestedPlaylistFollowers && typeof nestedPlaylistFollowers === "object"
        ? nestedPlaylistFollowers.total
        : undefined;
    const candidate =
      result.playlist_followers ??
      result.followers_total ??
      result.followers_count ??
      result.follower_count ??
      nestedFollowers ??
      nestedPlaylistFollowersTotal ??
      nestedPlaylistFollowers ??
      result.followers;
    return fallback(candidate);
  };

  const showSection = (section) => {
    if (!section) {
      return;
    }
    section.hidden = false;
    section.style.removeProperty("display");
  };

  const hideSection = (section) => {
    if (!section) {
      return;
    }
    section.hidden = true;
    section.style.display = "none";
  };

  const initBasicScan = (options) => {
    const {
      root,
      resolveMarketLabel = (value) => value,
      formatTimestamp = defaultFormatTimestamp,
      formatSummaryDateParts = defaultFormatSummaryDateParts,
      formatFollowersValue = defaultFormatFollowersValue,
      onStatus,
      getStartPayload,
      getTargetPlaylistId,
      onStageChange,
    } = options || {};

    const scanRoot = typeof root === "string" ? document.querySelector(root) : root;
    if (!scanRoot) {
      return null;
    }

    const scanParameters = scanRoot.querySelector("[data-basic-scan-parameters]");
    const runControls = scanRoot.querySelector("[data-basic-scan-run-controls]");
    const runButton = scanRoot.querySelector("[data-basic-scan-run]");
    const progressWrap = scanRoot.querySelector("[data-basic-scan-progress]");
    const progressFill = scanRoot.querySelector("[data-basic-scan-progress-fill]");
    const progressStatus = scanRoot.querySelector("[data-basic-scan-progress-status]");
    const progressLog = scanRoot.querySelector("[data-basic-scan-progress-log]");
    const resultsWrap = scanRoot.querySelector("[data-basic-scan-results]");
    const summaryLead = scanRoot.querySelector("[data-basic-scan-summary-lead]");
    const summaryList = scanRoot.querySelector("[data-basic-scan-summary-list]");
    const detailedWrap = scanRoot.querySelector("[data-basic-scan-detailed]");
    const exportSummary = scanRoot.querySelector("[data-basic-scan-export-summary]");
    const exportDetailed = scanRoot.querySelector("[data-basic-scan-export-detailed]");
    const summarySection = scanRoot.querySelector("[data-basic-scan-summary]") || document.getElementById("basic-scan-summary");
    const csvControls = scanRoot.querySelector("[data-basic-scan-csv-controls]");
    const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;

    let activeScanId = null;
    let eventSource = null;
    let currentStage = 1;
    let activeTargetPlaylistId = getTargetPlaylistId ? getTargetPlaylistId() : null;
    const closeEventSource = () => {
      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }
    };

    const setStage = (stage, scrollTarget) => {
      currentStage = stage;
      const showStage1 = stage === 1;
      const showStage2 = stage === 2;
      const showStage3 = stage === 3;

      if (showStage1) {
        showSection(scanParameters);
        showSection(runControls);
        hideSection(progressWrap);
        hideSection(resultsWrap);
        hideSection(csvControls);
      }

      if (showStage2) {
        hideSection(scanParameters);
        hideSection(runControls);
        showSection(progressWrap);
        hideSection(resultsWrap);
        hideSection(csvControls);
      }

      if (showStage3) {
        hideSection(scanParameters);
        hideSection(runControls);
        hideSection(progressWrap);
        showSection(resultsWrap);
        showSection(csvControls);
      }

      const target =
        scrollTarget ||
        (showStage1 ? scanParameters || scanRoot : showStage2 ? progressWrap : summarySection);
      if (scanRoot && scanRoot.offsetParent !== null) {
        scrollSectionToCenter(target);
      }
      if (typeof onStageChange === "function") {
        onStageChange(stage);
      }
    };

    const openStage1 = () => setStage(1, scanParameters || scanRoot);
    const openStage2 = () => setStage(2, progressWrap || scanRoot);

    const setProgress = (current, total, message) => {
      if (!progressWrap) {
        return;
      }
      const percent = total ? Math.min((current / total) * 100, 100) : 0;
      if (progressFill) {
        progressFill.style.width = `${percent}%`;
      }
      if (progressStatus) {
        progressStatus.textContent = message || "Scanning...";
      }
    };

    const appendLog = (message) => {
      if (!progressLog || !message) {
        return;
      }
      const entry = document.createElement("div");
      entry.className = "progress-log-entry";
      entry.textContent = message;
      progressLog.appendChild(entry);
      progressLog.scrollTop = progressLog.scrollHeight;
    };

    const renderSummary = (scan) => {
      if (!summaryLead || !summaryList) {
        return;
      }
      const follower = scan.follower_snapshot ?? "—";
      summaryLead.textContent = `At ${formatTimestamp(scan.started_at)}, your playlist follower count was ${follower}.`;
      summaryList.innerHTML = "";
      (scan.summary || []).forEach((item) => {
        const row = document.createElement("div");
        row.className = `summary-row ${item.tracked_found_in_top20 ? "" : "summary-row-missing"}`;
        const countryLabel = resolveMarketLabel(item.country);
        const { dateLabel, timeLabel } = formatSummaryDateParts(item.searched_at);
        const rankText = item.tracked_found_in_top20
          ? `your playlist rank was #${item.tracked_rank}.`
          : "your playlist was not found in the top 20.";
        row.textContent = `On ${dateLabel} at ${timeLabel} in ${countryLabel} for ‘${item.keyword}’, ${rankText}`;
        summaryList.appendChild(row);
      });
    };

    const renderDetailed = (scan) => {
      if (!detailedWrap) {
        return;
      }
      detailedWrap.innerHTML = "";
      const detailed = scan.detailed || {};
      const countryAccordions = [];
      const highlightId = activeTargetPlaylistId || scan.playlist_id;
      Object.values(detailed).forEach((countryData) => {
        const countryCode = countryData.country;
        const countryDetails = document.createElement("details");
        countryDetails.className = "accordion";
        const summary = document.createElement("summary");
        summary.textContent = `${flagEmoji(countryCode)} ${resolveMarketLabel(countryCode)}`;
        countryDetails.addEventListener("toggle", () => {
          if (!countryDetails.open) {
            return;
          }
          countryAccordions.forEach((openDetails) => {
            if (openDetails !== countryDetails) {
              openDetails.open = false;
            }
          });
          requestAnimationFrame(() => {
            scrollSectionToCenter(countryDetails);
          });
        });
        countryDetails.appendChild(summary);
        countryAccordions.push(countryDetails);

        const keywordContainer = document.createElement("div");
        keywordContainer.className = "accordion-body";
        const keywordAccordions = [];

        Object.entries(countryData.keywords || {}).forEach(([keyword, keywordData]) => {
          const keywordDetails = document.createElement("details");
          keywordDetails.className = "accordion nested";
          const keywordSummary = document.createElement("summary");
          keywordSummary.textContent = `${keyword} · ${formatTimestamp(keywordData.searched_at)}`;
          keywordDetails.appendChild(keywordSummary);
          keywordDetails.addEventListener("toggle", () => {
            if (!keywordDetails.open) {
              return;
            }
            keywordAccordions.forEach((openKeyword) => {
              if (openKeyword !== keywordDetails) {
                openKeyword.open = false;
              }
            });
            requestAnimationFrame(() => {
              scrollSectionToCenter(keywordDetails);
            });
          });

          const table = document.createElement("table");
          table.className = "results-table";
          table.innerHTML = `
            <thead>
              <tr>
                <th>Rank</th>
                <th>Playlist</th>
                <th>Owner</th>
                <th>Followers</th>
                <th>Songs</th>
              </tr>
            </thead>
            <tbody></tbody>
          `;
          const tbody = table.querySelector("tbody");
          (keywordData.results || []).forEach((result) => {
            const followersValue = resolveFollowers(result, formatFollowersValue);
            const row = document.createElement("tr");
            const isTarget = !!(highlightId && result.playlist_id && result.playlist_id === highlightId);
            if (isTarget) {
              row.classList.add("tracked-highlight");
            }
            row.innerHTML = `
              <td>${result.rank ?? "—"}</td>
              <td>
                ${
                  result.playlist_url
                    ? `<a href="${result.playlist_url}" target="_blank" rel="noopener noreferrer">${result.playlist_name || "—"}</a>`
                    : result.playlist_name || "—"
                }
              </td>
              <td>${result.playlist_owner || "—"}</td>
              <td>${followersValue}</td>
              <td>${result.songs_count ?? "—"}</td>
            `;
            tbody.appendChild(row);
          });

          const tableScroll = document.createElement("div");
          tableScroll.className = "table-scroll";
          tableScroll.appendChild(table);

          const keywordBody = document.createElement("div");
          keywordBody.className = "accordion-body";
          keywordBody.appendChild(tableScroll);
          keywordDetails.appendChild(keywordBody);
          keywordContainer.appendChild(keywordDetails);
          keywordAccordions.push(keywordDetails);
        });

        countryDetails.appendChild(keywordContainer);
        detailedWrap.appendChild(countryDetails);
      });
    };

    const buildExportUrl = (path) => {
      const url = new URL(path, window.location.origin);
      if (timeZone) {
        url.searchParams.set("timezone", timeZone);
      }
      return `${url.pathname}${url.search}`;
    };

    const loadResults = async (scanId) => {
      const response = await fetch(`/api/basic-rank-checker/scans/${scanId}`);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "Failed to load scan results.");
      }
      renderSummary(data);
      renderDetailed(data);
      const startedAt = data.started_at || data.searched_at || data.summary?.[0]?.searched_at;
      const timestamp = formatFileTimestamp(startedAt);
      const effectiveScanId = data.scan_id || scanId;
      const filenameBase = timestamp ? `${timestamp}_${effectiveScanId}` : `${effectiveScanId}`;
      if (exportSummary) {
        exportSummary.href = buildExportUrl(
          `/api/basic-rank-checker/scans/${scanId}/export/summary.csv`
        );
        exportSummary.download = `${filenameBase}_summary.csv`;
      }
      if (exportDetailed) {
        exportDetailed.href = buildExportUrl(
          `/api/basic-rank-checker/scans/${scanId}/export/detailed.csv`
        );
        exportDetailed.download = `${filenameBase}_detailed.csv`;
      }
    };

    const startScan = async () => {
      runButton.disabled = true;
      progressLog.innerHTML = "";
      setProgress(0, 0, "Starting scan…");
      activeTargetPlaylistId = getTargetPlaylistId ? getTargetPlaylistId() : activeTargetPlaylistId;

      try {
        const payload = (typeof getStartPayload === "function" ? getStartPayload() : null) || {};
        const response = await fetch("/api/basic-rank-checker/scans", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.detail || "Failed to start scan.");
        }
        activeScanId = data.scan_id;
        closeEventSource();
        eventSource = new EventSource(`/api/basic-rank-checker/scans/${activeScanId}/events`);
        eventSource.onmessage = async (event) => {
          const payload = JSON.parse(event.data);
          if (payload.type === "progress") {
            setProgress(payload.step, payload.total, payload.message);
            appendLog(payload.message);
          }
          if (payload.type === "done") {
            closeEventSource();
            setProgress(payload.total || 1, payload.total || 1, "Scan completed.");
            await loadResults(activeScanId);
            setStage(3, summarySection);
            runButton.disabled = false;
          }
          if (payload.type === "error") {
            closeEventSource();
            appendLog(payload.message || "Scan failed.");
            setProgress(0, 1, payload.message || "Scan failed.");
            setStage(1);
            runButton.disabled = false;
            if (typeof onStatus === "function") {
              onStatus(payload.message || "Scan failed.", "status-error");
            }
          }
        };
      } catch (error) {
        setStage(1);
        runButton.disabled = false;
        if (typeof onStatus === "function") {
          onStatus(error.message || "Failed to start scan.", "status-error");
        }
      }
    };

    runButton?.addEventListener("click", () => {
      openStage2();
      startScan();
    });

    setStage(1, scanParameters || scanRoot);

    return {
      dispose: closeEventSource,
      setStage,
      getStage: () => currentStage,
      openStage1,
      openStage2,
      startScan,
    };
  };

  window.BasicScan = {
    init: initBasicScan,
    scrollSectionToCenter,
  };
})();
