/**
 * LogScope — Web Server Log Analytics
 * Frontend Controller (Vanilla JavaScript)
 */

document.addEventListener("DOMContentLoaded", () => {
  // DOM Elements
  const logFileInput = document.getElementById("logFileInput");
  const fileSelectBtn = document.getElementById("fileSelectBtn");
  const selectedFileName = document.getElementById("selectedFileName");
  const selectedFileSize = document.getElementById("selectedFileSize");
  const useSampleBtn = document.getElementById("useSampleBtn");
  const analyzeBtn = document.getElementById("analyzeBtn");
  const analyzeBtnText = document.getElementById("analyzeBtnText");
  const analyzeBtnSpinner = document.getElementById("analyzeBtnSpinner");
  const executionBanner = document.getElementById("executionBanner");
  const bannerMessage = document.getElementById("bannerMessage");
  const bannerMeta = document.getElementById("bannerMeta");

  // Health Pill Elements
  const engineStatusPill = document.getElementById("engineStatusPill");
  const engineStatusText = document.getElementById("engineStatusText");
  const engineMetaChip = document.getElementById("engineMetaChip");

  // KPI Value Elements
  const kpiTotalRequests = document.getElementById("kpiTotalRequests");
  const kpiTotalRequestsSub = document.getElementById("kpiTotalRequestsSub");
  const kpiUniqueUrls = document.getElementById("kpiUniqueUrls");
  const kpiUniqueIps = document.getElementById("kpiUniqueIps");
  const kpiStatusCategories = document.getElementById("kpiStatusCategories");
  const kpiStatusCategoriesSub = document.getElementById("kpiStatusCategoriesSub");

  // Analytics Containers
  const urlListContainer = document.getElementById("urlListContainer");
  const statusListContainer = document.getElementById("statusListContainer");
  const ipTableBody = document.getElementById("ipTableBody");

  // Modal Elements
  const errorModal = document.getElementById("errorModal");
  const modalCloseBtn = document.getElementById("modalCloseBtn");
  const modalDismissBtn = document.getElementById("modalDismissBtn");
  const modalErrorMsg = document.getElementById("modalErrorMsg");
  const modalTraceBox = document.getElementById("modalTraceBox");

  // State
  let currentFile = null;
  let isUsingSample = true;
  let urlChartInstance = null;
  let statusChartInstance = null;

  // Formatting Helpers
  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  }

  function escapeHtml(str) {
    if (!str) return "";
    return String(str).replace(/[&<>"']/g, (m) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[m]));
  }

  // Update Input UI Selection
  function setSampleMode() {
    isUsingSample = true;
    currentFile = null;
    logFileInput.value = "";
    selectedFileName.textContent = "input/access.log (Default Sample)";
    selectedFileSize.textContent = "Original dataset";
    useSampleBtn.style.display = "none";
  }

  function setUploadMode(file) {
    isUsingSample = false;
    currentFile = file;
    selectedFileName.textContent = file.name;
    selectedFileSize.textContent = formatBytes(file.size);
    useSampleBtn.style.display = "inline-flex";
  }

  logFileInput.addEventListener("change", (e) => {
    if (logFileInput.files && logFileInput.files.length > 0) {
      const file = logFileInput.files[0];
      const ext = file.name.slice(((file.name.lastIndexOf(".") - 1) >>> 0) + 2).toLowerCase();
      if (ext !== "log" && ext !== "txt") {
        showError("Invalid File Extension", `Only .log and .txt files are supported. Selected: .${ext}`);
        setSampleMode();
        return;
      }
      if (file.size > 16 * 1024 * 1024) {
        showError("File Too Large", `Selected file (${formatBytes(file.size)}) exceeds maximum limit of 16 MB.`);
        setSampleMode();
        return;
      }
      setUploadMode(file);
    } else {
      setSampleMode();
    }
  });

  useSampleBtn.addEventListener("click", () => {
    setSampleMode();
  });

  // Modal Handlers
  function showError(title, detail) {
    modalErrorMsg.textContent = title || "Execution Error";
    modalTraceBox.textContent = detail || "No additional trace available.";
    errorModal.classList.remove("hidden");
  }

  function hideError() {
    errorModal.classList.add("hidden");
  }

  modalCloseBtn.addEventListener("click", hideError);
  modalDismissBtn.addEventListener("click", hideError);
  errorModal.addEventListener("click", (e) => {
    if (e.target === errorModal) hideError();
  });

  // Render Analytics Data
  function renderAnalytics(results, meta) {
    if (!results) return;

    // 1. KPI Cards
    kpiTotalRequests.textContent = results.total_requests.toLocaleString();
    if (results.raw_lines > 0) {
      kpiTotalRequestsSub.textContent = `${results.total_requests} of ${results.raw_lines} log entries matched (${results.match_percentage}%)`;
    } else {
      kpiTotalRequestsSub.textContent = "Processed MapReduce records";
    }

    kpiUniqueUrls.textContent = results.unique_urls.toLocaleString();
    kpiUniqueIps.textContent = results.unique_ips.toLocaleString();
    
    kpiStatusCategories.textContent = results.status_categories_count.toLocaleString();
    kpiStatusCategoriesSub.textContent = `HTTP Errors (4xx/5xx): ${results.error_count} (${results.error_rate}%)`;

    // 2. URLs Chart / List
    renderUrlAnalytics(results.urls, results.total_requests);

    // 3. Status Distribution Chart / List
    renderStatusAnalytics(results.statuses, results.status_categories, results.total_requests);

    // 4. IP Table
    renderIpTable(results.ips, results.total_requests);
  }

  function renderUrlAnalytics(urls, total) {
    const entries = Object.entries(urls || {});
    if (entries.length === 0) {
      urlListContainer.innerHTML = '<div class="empty-state">No URL records found in Hadoop output.</div>';
      return;
    }

    // Try Chart.js if available and canvas exists
    const canvas = document.getElementById("urlChart");
    if (window.Chart && canvas) {
      try {
        if (urlChartInstance) {
          urlChartInstance.destroy();
        }

        const labels = entries.slice(0, 8).map(e => e[0]);
        const data = entries.slice(0, 8).map(e => e[1]);

        canvas.style.display = "block";
        urlListContainer.style.display = "none";

        urlChartInstance = new Chart(canvas, {
          type: "bar",
          data: {
            labels: labels,
            datasets: [{
              label: "Requests",
              data: data,
              backgroundColor: "rgba(6, 182, 212, 0.7)",
              borderColor: "#06b6d4",
              borderWidth: 1,
              borderRadius: 6
            }]
          },
          options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (ctx) => ` Requests: ${ctx.parsed.x} (${total > 0 ? Math.round((ctx.parsed.x / total) * 100) : 0}%)`
                }
              }
            },
            scales: {
              x: {
                grid: { color: "#1f2d44" },
                ticks: { color: "#94a3b8", precision: 0 }
              },
              y: {
                grid: { display: false },
                ticks: { color: "#f8fafc", font: { family: "monospace", size: 12 } }
              }
            }
          }
        });
        return;
      } catch (err) {
        console.warn("Chart.js URL render failed, using CSS fallback:", err);
      }
    }

    // CSS Fallback rendering
    const maxVal = Math.max(1, ...entries.map(e => e[1]));
    let html = '<div class="css-chart-list">';
    entries.forEach(([url, count]) => {
      const pct = Math.round((count / maxVal) * 100);
      const sharePct = total > 0 ? ((count / total) * 100).toFixed(1) : 0;
      html += `
        <div class="bar-row">
          <div class="bar-info">
            <code>${escapeHtml(url)}</code>
            <b>${count.toLocaleString()} <span style="font-size:11px;color:var(--text-muted)">(${sharePct}%)</span></b>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width: ${pct}%"></div>
          </div>
        </div>
      `;
    });
    html += '</div>';

    if (canvas) canvas.style.display = "none";
    urlListContainer.style.display = "block";
    urlListContainer.innerHTML = html;
  }

  function renderStatusAnalytics(statuses, categories, total) {
    const entries = Object.entries(statuses || {});
    if (entries.length === 0) {
      statusListContainer.innerHTML = '<div class="empty-state">No HTTP status records found in Hadoop output.</div>';
      return;
    }

    const canvas = document.getElementById("statusChart");
    if (window.Chart && canvas) {
      try {
        if (statusChartInstance) {
          statusChartInstance.destroy();
        }

        const labels = entries.map(e => `HTTP ${e[0]}`);
        const data = entries.map(e => e[1]);
        const bgColors = entries.map(([code]) => {
          if (code.startsWith("2")) return "rgba(16, 185, 129, 0.75)";
          if (code.startsWith("3")) return "rgba(59, 130, 246, 0.75)";
          if (code.startsWith("4")) return "rgba(245, 158, 11, 0.75)";
          if (code.startsWith("5")) return "rgba(239, 68, 68, 0.75)";
          return "rgba(148, 163, 184, 0.75)";
        });

        canvas.style.display = "block";
        statusListContainer.style.display = "none";

        statusChartInstance = new Chart(canvas, {
          type: "doughnut",
          data: {
            labels: labels,
            datasets: [{
              data: data,
              backgroundColor: bgColors,
              borderColor: "#162032",
              borderWidth: 2,
              hoverOffset: 4
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: {
                position: "bottom",
                labels: { color: "#94a3b8", font: { size: 12 }, padding: 12 }
              },
              tooltip: {
                callbacks: {
                  label: (ctx) => ` Count: ${ctx.parsed} (${total > 0 ? Math.round((ctx.parsed / total) * 100) : 0}%)`
                }
              }
            },
            cutout: "65%"
          }
        });
        return;
      } catch (err) {
        console.warn("Chart.js status render failed, using CSS fallback:", err);
      }
    }

    // CSS Fallback rendering
    const statusDescriptions = {
      "200": "OK (Success)",
      "201": "Created",
      "301": "Moved Permanently",
      "304": "Not Modified",
      "400": "Bad Request",
      "401": "Unauthorized",
      "403": "Forbidden",
      "404": "Not Found",
      "500": "Internal Server Error",
      "502": "Bad Gateway",
      "503": "Service Unavailable"
    };

    const maxVal = Math.max(1, ...entries.map(e => e[1]));
    let html = '<div class="status-list">';
    entries.forEach(([code, count]) => {
      const cls = code.startsWith("2") ? "status-200" : (code.startsWith("4") ? "status-404" : (code.startsWith("5") ? "status-500" : "status-3xx"));
      const desc = statusDescriptions[code] || "HTTP Response";
      const pct = Math.round((count / maxVal) * 100);

      html += `
        <div class="status-item">
          <span class="status-badge ${cls}">${escapeHtml(code)}</span>
          <div class="status-meta">
            <div class="status-name">${escapeHtml(desc)}</div>
            <div class="status-bar-track">
              <div class="status-bar-fill ${cls}" style="width: ${pct}%"></div>
            </div>
          </div>
          <span class="status-count">${count.toLocaleString()}</span>
        </div>
      `;
    });
    html += '</div>';

    if (canvas) canvas.style.display = "none";
    statusListContainer.style.display = "block";
    statusListContainer.innerHTML = html;
  }

  function renderIpTable(ips, total) {
    if (!ips || ips.length === 0) {
      ipTableBody.innerHTML = '<tr><td colspan="3" class="empty-state">No client IP records found in Hadoop output.</td></tr>';
      return;
    }

    let rowsHtml = "";
    ips.forEach((item) => {
      rowsHtml += `
        <tr>
          <td>
            <div class="ip-pill">
              <span class="ip-tag">CLIENT</span>
              <code>${escapeHtml(item.ip)}</code>
            </div>
          </td>
          <td><b>${item.count.toLocaleString()}</b></td>
          <td>
            <div class="share-cell">
              <div class="share-track">
                <div class="share-fill" style="width: ${item.percentage}%"></div>
              </div>
              <span class="share-text">${item.percentage}%</span>
            </div>
          </td>
        </tr>
      `;
    });

    ipTableBody.innerHTML = rowsHtml;
  }

  // Fetch Engine Health
  async function checkHealth() {
    try {
      const res = await fetch("/api/health");
      const health = await res.json();

      if (health.is_ready) {
        engineStatusPill.className = "status-pill";
        engineStatusText.textContent = "Hadoop MapReduce Engine: Ready";
        engineMetaChip.textContent = health.hadoop_version ? health.hadoop_version.split(" ")[0] + " " + health.hadoop_version.split(" ")[1] : "Hadoop Local Mode";
      } else if (health.status === "degraded") {
        engineStatusPill.className = "status-pill degraded";
        engineStatusText.textContent = "Hadoop: Degraded";
        engineMetaChip.textContent = health.message;
      } else {
        engineStatusPill.className = "status-pill unavailable";
        engineStatusText.textContent = "Hadoop: Unavailable";
        engineMetaChip.textContent = "Check Hadoop in Ubuntu/WSL";
      }
    } catch (err) {
      engineStatusPill.className = "status-pill unavailable";
      engineStatusText.textContent = "Backend Offline";
      engineMetaChip.textContent = "Flask server not responding";
    }
  }

  // Handle Log Analysis Execution
  analyzeBtn.addEventListener("click", async () => {
    analyzeBtn.disabled = true;
    analyzeBtnSpinner.style.display = "inline-block";
    analyzeBtnText.textContent = "Invoking Hadoop...";
    
    executionBanner.className = "execution-banner running";
    bannerMessage.innerHTML = '<span class="spinner"></span> Hadoop MapReduce job is processing. Spawning JVM and parsing logs...';
    bannerMeta.textContent = "Please wait (local mode)";

    const formData = new FormData();
    if (!isUsingSample && currentFile) {
      formData.append("logfile", currentFile);
      formData.append("use_sample", "false");
    } else {
      formData.append("use_sample", "true");
    }

    try {
      const response = await fetch("/api/analyze", {
        method: "POST",
        body: formData
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        const errorMsg = data.error || `MapReduce Job Failed (HTTP ${response.status})`;
        const errorDetail = data.detail || "Check terminal logs or Hadoop configuration.";

        executionBanner.className = "execution-banner failed";
        bannerMessage.innerHTML = `⚠️ <b>Analysis Failed:</b> ${escapeHtml(errorMsg)}`;
        bannerMeta.textContent = data.duration_seconds ? `Halted after ${data.duration_seconds}s` : "Error";

        showError(errorMsg, errorDetail);
        return;
      }

      // Success: render fresh analytics
      renderAnalytics(data.results, data);

      executionBanner.className = "execution-banner success";
      bannerMessage.innerHTML = `✓ <b>MapReduce Job Completed Successfully:</b> Analyzed <b>${escapeHtml(data.source_name)}</b> (${data.raw_lines || data.results.total_requests} records processed).`;
      bannerMeta.textContent = `Duration: ${data.duration_seconds}s · ${data.timestamp}`;

      if (data.notice) {
        showError("Notice: No Log Matches", data.notice + "\n\nPlease ensure your log follows the standard Apache Combined / Common format with HTTP/1.1.");
      }

      // Refresh health pill
      checkHealth();

    } catch (err) {
      console.error("Analysis request failed:", err);
      executionBanner.className = "execution-banner failed";
      bannerMessage.innerHTML = "⚠️ <b>Connection Error:</b> Could not reach Flask backend.";
      bannerMeta.textContent = "Check WSL terminal";
      showError("Connection Error", `Could not connect to backend server:\n${err.message}\n\nMake sure Flask is running in WSL on http://127.0.0.1:5000.`);
    } finally {
      analyzeBtn.disabled = false;
      analyzeBtnSpinner.style.display = "none";
      analyzeBtnText.textContent = "▶ Analyze Logs";
    }
  });

  // Initial Load: Check Health and load any existing results
  checkHealth();

  // If initial results were injected by Flask Jinja, initialize charts
  if (window.__INITIAL_RESULTS__) {
    renderAnalytics(window.__INITIAL_RESULTS__, {});
  }
});
