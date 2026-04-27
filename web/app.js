const drilldownConfig = {
  sourceIP: {
    countLabel: "设备",
    primaryTitle: "设备排行",
    secondaryColumn: "访问主机",
    buildSecondaryTitle: (primary) => (primary ? `${primary} 访问的主机` : "访问主机"),
    buildDetailTitle: (primary, secondary) =>
      primary && secondary ? `${primary} / ${secondary} 的连接明细` : "连接明细",
  },
  host: {
    countLabel: "主机",
    primaryTitle: "主机排行",
    secondaryColumn: "访问设备",
    buildSecondaryTitle: (primary) => (primary ? `${primary} 的访问设备` : "访问设备"),
    buildDetailTitle: (primary, secondary) =>
      primary && secondary ? `${primary} / ${secondary} 的连接明细` : "连接明细",
  },
  outbound: {
    countLabel: "代理",
    primaryTitle: "代理排行",
    secondaryColumn: "目标主机",
    buildSecondaryTitle: (primary) => (primary ? `${primary} 命中的目标主机` : "目标主机"),
    buildDetailTitle: (primary, secondary) =>
      primary && secondary ? `${primary} / ${secondary} 的连接明细` : "连接明细",
  },
}

const DIMENSION_STORAGE_KEY = "traffic-monitor:selected-dimension"

const elements = {
  dimension: document.getElementById("dimension"),
  dimensionTabs: Array.from(document.querySelectorAll(".dimension-tab")),
  range: document.getElementById("range"),
  start: document.getElementById("start"),
  end: document.getElementById("end"),
  statusBanner: document.getElementById("statusBanner"),
  runtimeSummary: document.getElementById("runtimeSummary"),
  selectionPath: document.getElementById("selectionPath"),
  runtimeConnectionState: document.getElementById("runtimeConnectionState"),
  runtimeDimension: document.getElementById("runtimeDimension"),
  runtimeRangeLabel: document.getElementById("runtimeRangeLabel"),
  settingsPanel: document.getElementById("settingsPanel"),
  settingsTitle: document.getElementById("settingsTitle"),
  settingsDescription: document.getElementById("settingsDescription"),
  settingsForm: document.getElementById("settingsForm"),
  settingsUrl: document.getElementById("settingsUrl"),
  settingsSecret: document.getElementById("settingsSecret"),
  settingsSaveBtn: document.getElementById("settingsSaveBtn"),
  settingsCancelBtn: document.getElementById("settingsCancelBtn"),
  settingsBtn: document.getElementById("settingsBtn"),
  autoSwitchBtn: document.getElementById("autoSwitchBtn"),
  autoSwitchPanel: document.getElementById("autoSwitchPanel"),
  autoSwitchForm: document.getElementById("autoSwitchForm"),
  autoSwitchEnabled: document.getElementById("autoSwitchEnabled"),
  autoSwitchThreshold: document.getElementById("autoSwitchThreshold"),
  autoSwitchCooldown: document.getElementById("autoSwitchCooldown"),
  autoSwitchRefreshBtn: document.getElementById("autoSwitchRefreshBtn"),
  autoSwitchSaveBtn: document.getElementById("autoSwitchSaveBtn"),
  autoSwitchCancelBtn: document.getElementById("autoSwitchCancelBtn"),
  autoSwitchGroupsBody: document.getElementById("autoSwitchGroupsBody"),
  autoSwitchEventsBody: document.getElementById("autoSwitchEventsBody"),
  dashboardShell: document.getElementById("dashboardShell"),
  refreshBtn: document.getElementById("refreshBtn"),
  clearBtn: document.getElementById("clearBtn"),
  countLabel: document.getElementById("countLabel"),
  primaryTitle: document.getElementById("primaryTitle"),
  countValue: document.getElementById("countValue"),
  uploadValue: document.getElementById("uploadValue"),
  downloadValue: document.getElementById("downloadValue"),
  totalValue: document.getElementById("totalValue"),
  tableBody: document.getElementById("tableBody"),
  trendCanvas: document.getElementById("trendCanvas"),
  trendTooltip: document.getElementById("trendTooltip"),
  trendAxis: document.getElementById("trendAxis"),
  secondaryTitle: document.getElementById("secondaryTitle"),
  secondaryHeader: document.getElementById("secondaryHeader"),
  detailTitle: document.getElementById("detailTitle"),
  detailSearch: document.getElementById("detailSearch"),
  secondaryBody: document.getElementById("secondaryBody"),
  detailCards: document.getElementById("detailCards"),
}

const state = {
  lastTrendPoints: [],
  trendPlotPoints: [],
  trendHoverIndex: -1,
  primaryRows: [],
  secondaryRows: [],
  detailRows: [],
  selectedPrimary: null,
  selectedSecondary: null,
  detailSearchQuery: "",
  loadSeq: 0,
  detailSeq: 0,
  mihomoSettings: {
    url: "",
    secret: "",
  },
  settingsOpen: false,
  settingsRequired: false,
  autoSwitchOpen: false,
  autoSwitch: {
    enabled: false,
    thresholdBytesPerMinute: 0,
    cooldownSeconds: 0,
    groupTargets: [],
    groups: [],
    events: [],
  },
}

function isValidDimension(value) {
  return Object.prototype.hasOwnProperty.call(drilldownConfig, value)
}

function loadStoredDimension() {
  try {
    const value = window.localStorage.getItem(DIMENSION_STORAGE_KEY)
    return isValidDimension(value) ? value : null
  } catch (error) {
    console.warn("Failed to load stored dimension", error)
    return null
  }
}

function persistSelectedDimension(value) {
  if (!isValidDimension(value)) return

  try {
    window.localStorage.setItem(DIMENSION_STORAGE_KEY, value)
  } catch (error) {
    console.warn("Failed to persist selected dimension", error)
  }
}

function nowLocalInputValue(offsetMs) {
  const date = new Date(Date.now() + offsetMs)
  const pad = (value) => String(value).padStart(2, "0")
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`
}

function formatDateTime(timestamp) {
  if (!Number.isFinite(timestamp)) return "--"
  const date = new Date(timestamp)
  const pad = (value) => String(value).padStart(2, "0")
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`
}

function formatTrendAxisTick(timestamp, spanMs) {
  if (!Number.isFinite(timestamp)) return "--"
  const date = new Date(timestamp)
  const pad = (value) => String(value).padStart(2, "0")
  const monthDay = `${pad(date.getMonth() + 1)}-${pad(date.getDate())}`
  const hourMinute = `${pad(date.getHours())}:${pad(date.getMinutes())}`

  if (spanMs <= 86400000) return hourMinute
  if (spanMs <= 604800000) return `${monthDay} ${hourMinute}`
  return monthDay
}

function getTrendTickIndexes(pointCount, targetCount = 5) {
  if (pointCount <= 0) return []
  if (pointCount === 1) return [0]

  const ticks = Math.min(pointCount, targetCount)
  return Array.from(
    new Set(
      Array.from({ length: ticks }, (_, index) =>
        Math.round(((pointCount - 1) * index) / Math.max(ticks - 1, 1)),
      ),
    ),
  )
}

function updateTrendAxisLabels(points) {
  if (!points.length) {
    elements.trendAxis.innerHTML = '<span>--</span><span>--</span><span>--</span><span>--</span><span>--</span>'
    return
  }

  const startPoint = points[0]
  const endPoint = points[points.length - 1]
  const spanMs = Math.max(endPoint.timestamp - startPoint.timestamp, 0)
  const tickMarkup = getTrendTickIndexes(points.length)
    .map((index) => `<span>${escapeHTML(formatTrendAxisTick(points[index].timestamp, spanMs))}</span>`)
    .join("")

  elements.trendAxis.innerHTML = tickMarkup
}

function updateCustomInputs() {
  const range = Number(elements.range.value)
  const end = Date.now()
  const start = range === -1 ? end - 86400000 : end - range
  elements.start.value = nowLocalInputValue(start - end)
  elements.end.value = nowLocalInputValue(0)
}

function getTimeRange() {
  if (Number(elements.range.value) === -1) {
    return {
      start: new Date(elements.start.value).getTime(),
      end: new Date(elements.end.value).getTime(),
    }
  }

  const end = Date.now()
  return {
    end,
    start: end - Number(elements.range.value),
  }
}

function currentRangeLabel() {
  if (Number(elements.range.value) === -1) {
    const { start, end } = getTimeRange()
    return `${formatDateTime(start)} - ${formatDateTime(end)}`
  }

  const option = elements.range.options[elements.range.selectedIndex]
  const label = option?.textContent?.trim() || "7 天"
  return `最近 ${label}`
}

function bucketSize(start, end) {
  const range = end - start
  if (range <= 3600000) return 60000
  if (range <= 86400000) return 300000
  if (range <= 604800000) return 3600000
  return 86400000
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B"
  const units = ["B", "KB", "MB", "GB", "TB"]
  let value = bytes
  let idx = 0
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024
    idx += 1
  }
  return `${value.toFixed(value >= 10 || idx === 0 ? 0 : 1)} ${units[idx]}`
}

function bytesToMegabytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return ""
  return String(Math.round(bytes / (1024 * 1024)))
}

function megabytesToBytes(value) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) return 0
  return Math.round(parsed * 1024 * 1024)
}

function secondsToMinutes(value) {
  if (!Number.isFinite(value) || value <= 0) return ""
  return String(Math.round(value / 60))
}

function escapeHTML(text) {
  return String(text ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;")
}

function renderTruncatedText(text, className = "", fallback = "Unknown") {
  const value = String(text || fallback)
  const safe = escapeHTML(value)
  const cls = className ? `truncate ${className}` : "truncate"
  return `<span class="${cls}" title="${safe}">${safe}</span>`
}

function setStatus(text, isError = false) {
  if (!text) {
    elements.statusBanner.textContent = ""
    elements.statusBanner.classList.add("hidden")
    elements.statusBanner.classList.remove("error")
    return
  }

  elements.statusBanner.textContent = text
  elements.statusBanner.classList.toggle("error", Boolean(isError))
  elements.statusBanner.classList.remove("hidden")
}

function syncDimensionTabs() {
  elements.dimensionTabs.forEach((button) => {
    button.classList.toggle("active", button.dataset.dimension === elements.dimension.value)
  })
}

function syncContextSummary() {
  const config = drilldownConfig[elements.dimension.value] || drilldownConfig.sourceIP
  elements.runtimeConnectionState.textContent = state.mihomoSettings.url ? "已配置" : "待配置"
  elements.runtimeDimension.textContent = config.primaryTitle
  elements.runtimeRangeLabel.textContent = currentRangeLabel()

  if (!state.selectedPrimary) {
    elements.selectionPath.textContent = `当前维度为${config.countLabel}，等待选择主分组。`
    return
  }

  if (!state.selectedSecondary) {
    elements.selectionPath.textContent = `${config.countLabel} / ${state.selectedPrimary}`
    return
  }

  elements.selectionPath.textContent = `${config.countLabel} / ${state.selectedPrimary} / ${state.selectedSecondary}`
}

function updateViewHints() {
  const config = drilldownConfig[elements.dimension.value] || drilldownConfig.sourceIP
  const secondaryTitle = config.buildSecondaryTitle(state.selectedPrimary)
  const detailTitle = config.buildDetailTitle(state.selectedPrimary, state.selectedSecondary)

  elements.countLabel.textContent = config.countLabel
  elements.primaryTitle.textContent = config.primaryTitle
  elements.secondaryHeader.textContent = config.secondaryColumn
  elements.secondaryTitle.textContent = secondaryTitle
  elements.secondaryTitle.title = secondaryTitle
  elements.detailTitle.textContent = detailTitle
  elements.detailTitle.title = detailTitle

  syncDimensionTabs()
  syncContextSummary()
}

async function fetchJSON(path, params) {
  const url = new URL(path, window.location.origin)
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value))
    }
  })

  const response = await fetch(url)
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}))
    throw new Error(payload.error || `Request failed: ${response.status}`)
  }
  return response.json()
}

async function sendJSON(path, method, payload) {
  const response = await fetch(path, {
    method,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload || {}),
  })
  if (!response.ok) {
    const errorPayload = await response.json().catch(() => ({}))
    throw new Error(errorPayload.error || `Request failed: ${response.status}`)
  }
  return response.json()
}

function syncSettingsForm() {
  elements.settingsUrl.value = state.mihomoSettings.url || ""
  elements.settingsSecret.value = state.mihomoSettings.secret || ""
}

function syncSettingsUI() {
  const panelVisible = state.settingsOpen || state.settingsRequired
  elements.settingsPanel.classList.toggle("hidden", !panelVisible)
  elements.dashboardShell.classList.toggle("hidden", state.settingsRequired)
  elements.runtimeSummary.classList.toggle("hidden", state.settingsRequired)
  elements.settingsCancelBtn.classList.toggle("hidden", state.settingsRequired)

  if (state.settingsRequired) {
    elements.settingsTitle.textContent = "连接 Mihomo"
    elements.settingsDescription.textContent =
      "当前还没有可用的 Mihomo 连接设置。先填写 Mihomo Controller 地址和 Secret，保存后再开始采集。"
    elements.settingsSaveBtn.textContent = "保存并连接"
  } else {
    elements.settingsTitle.textContent = "更新连接设置"
    elements.settingsDescription.textContent =
      "修改后会立刻用于后续采集。下次服务重启时，如果环境变量里仍然有值，会继续以环境变量为准。"
    elements.settingsSaveBtn.textContent = "保存修改"
  }

  syncContextSummary()
}

function syncAutoSwitchUI() {
  elements.autoSwitchPanel.classList.toggle("hidden", !state.autoSwitchOpen)
}

function mergeAutoSwitchGroups(groups, groupTargets) {
  const savedTargets = new Map(
    (groupTargets || []).map((item) => [
      item.groupName,
      {
        enabled: Boolean(item.enabled),
        targetProxy: item.targetProxy || "",
      },
    ]),
  )

  return (groups || []).map((group) => {
    const saved = savedTargets.get(group.name) || {}
    return {
      ...group,
      enabled: Boolean(saved.enabled),
      targetProxy: saved.targetProxy && group.all.includes(saved.targetProxy)
        ? saved.targetProxy
        : group.now || group.all[0] || "",
    }
  })
}

function syncAutoSwitchForm() {
  elements.autoSwitchEnabled.checked = Boolean(state.autoSwitch.enabled)
  elements.autoSwitchThreshold.value = bytesToMegabytes(state.autoSwitch.thresholdBytesPerMinute)
  elements.autoSwitchCooldown.value = secondsToMinutes(state.autoSwitch.cooldownSeconds)
  renderAutoSwitchGroups()
  renderAutoSwitchEvents()
}

function renderAutoSwitchGroups() {
  if (!state.mihomoSettings.url) {
    elements.autoSwitchGroupsBody.innerHTML =
      '<tr><td colspan="5" class="empty">连接 Mihomo 后加载可控策略组</td></tr>'
    return
  }

  if (!state.autoSwitch.groups.length) {
    elements.autoSwitchGroupsBody.innerHTML =
      '<tr><td colspan="5" class="empty">当前没有可控策略组，仅展示 select 和 fallback</td></tr>'
    return
  }

  elements.autoSwitchGroupsBody.innerHTML = state.autoSwitch.groups
    .map((group) => {
      const options = (group.all || [])
        .map((proxy) => {
          const selected = proxy === group.targetProxy ? " selected" : ""
          return `<option value="${escapeHTML(proxy)}"${selected}>${escapeHTML(proxy)}</option>`
        })
        .join("")

      return `
        <tr data-group-name="${escapeHTML(group.name)}">
          <td>
            <input class="auto-switch-group-enabled" type="checkbox" ${group.enabled ? "checked" : ""} />
          </td>
          <td><div class="mono">${renderTruncatedText(group.name, "host", "-")}</div></td>
          <td><span class="chip route">${escapeHTML(group.type)}</span></td>
          <td><div class="mono">${renderTruncatedText(group.now || "-", "host", "-")}</div></td>
          <td>
            <select class="auto-switch-target-select">
              ${options}
            </select>
          </td>
        </tr>
      `
    })
    .join("")
}

function renderAutoSwitchEvents() {
  if (!state.autoSwitch.events.length) {
    elements.autoSwitchEventsBody.innerHTML =
      '<div class="detail-empty">当前还没有自动切换记录</div>'
    return
  }

  elements.autoSwitchEventsBody.innerHTML = state.autoSwitch.events
    .map((event) => {
      const results = (event.results || [])
        .map((result) => {
          const extra = result.message ? ` · ${escapeHTML(result.message)}` : ""
          return `<div class="auto-switch-result">${escapeHTML(result.groupName)} -> ${escapeHTML(result.targetProxy)} · ${escapeHTML(result.status)}${extra}</div>`
        })
        .join("")

      const errorLine = event.error
        ? `<div class="auto-switch-event-error">${escapeHTML(event.error)}</div>`
        : ""

      return `
        <article class="auto-switch-event-card">
          <div class="auto-switch-event-top">
            <strong>${escapeHTML(event.host || "Unknown")}</strong>
            <span>${escapeHTML(formatDateTime(event.triggeredAt))}</span>
          </div>
          <div class="auto-switch-event-meta">
            <span>分钟累计 ${escapeHTML(formatBytes(event.totalBytes || 0))}</span>
            <span>窗口 ${escapeHTML(formatDateTime(event.windowStart))} - ${escapeHTML(formatDateTime(event.windowEnd))}</span>
          </div>
          <div class="auto-switch-result-list">${results || '<div class="auto-switch-result">没有执行记录</div>'}</div>
          ${errorLine}
        </article>
      `
    })
    .join("")
}

async function loadAutoSwitchSettings() {
  const settings = await fetchJSON("/api/auto-switch/settings")
  state.autoSwitch.enabled = Boolean(settings.enabled)
  state.autoSwitch.thresholdBytesPerMinute = Number(settings.thresholdBytesPerMinute || 0)
  state.autoSwitch.cooldownSeconds = Number(settings.cooldownSeconds || 0)
  state.autoSwitch.groupTargets = Array.isArray(settings.groupTargets) ? settings.groupTargets : []
}

async function loadAutoSwitchGroups() {
  if (!state.mihomoSettings.url) {
    state.autoSwitch.groups = []
    renderAutoSwitchGroups()
    return
  }

  const groups = await fetchJSON("/api/auto-switch/groups")
  state.autoSwitch.groups = mergeAutoSwitchGroups(groups, state.autoSwitch.groupTargets)
  renderAutoSwitchGroups()
}

async function loadAutoSwitchEvents() {
  const events = await fetchJSON("/api/auto-switch/events")
  state.autoSwitch.events = Array.isArray(events) ? events : []
  renderAutoSwitchEvents()
}

async function refreshAutoSwitchData() {
  await loadAutoSwitchSettings()
  await Promise.all([loadAutoSwitchGroups(), loadAutoSwitchEvents()])
  syncAutoSwitchForm()
}

function collectAutoSwitchGroupTargets() {
  const rows = Array.from(elements.autoSwitchGroupsBody.querySelectorAll("tr[data-group-name]"))
  return rows.map((row) => {
    const groupName = row.dataset.groupName || ""
    const enabled = row.querySelector(".auto-switch-group-enabled")?.checked || false
    const targetProxy = row.querySelector(".auto-switch-target-select")?.value || ""
    return { groupName, enabled, targetProxy }
  })
}

async function loadSettings() {
  const settings = await fetchJSON("/api/settings/mihomo")
  state.mihomoSettings = {
    url: settings.url || "",
    secret: settings.secret || "",
  }
  state.settingsRequired = !state.mihomoSettings.url
  state.settingsOpen = state.settingsRequired
  syncSettingsForm()
  syncSettingsUI()
}

function openSettingsPanel() {
  state.settingsOpen = true
  state.autoSwitchOpen = false
  syncSettingsForm()
  syncSettingsUI()
  syncAutoSwitchUI()
  elements.settingsUrl.focus()
}

function closeSettingsPanel() {
  if (state.settingsRequired) return
  state.settingsOpen = false
  syncSettingsForm()
  syncSettingsUI()
}

function openAutoSwitchPanel() {
  state.autoSwitchOpen = true
  state.settingsOpen = false
  syncAutoSwitchForm()
  syncSettingsUI()
  syncAutoSwitchUI()
  elements.autoSwitchThreshold.focus()
}

function closeAutoSwitchPanel() {
  state.autoSwitchOpen = false
  syncAutoSwitchForm()
  syncAutoSwitchUI()
}

async function saveSettings(event) {
  event.preventDefault()

  const payload = {
    url: elements.settingsUrl.value.trim(),
    secret: elements.settingsSecret.value.trim(),
  }

  elements.settingsSaveBtn.disabled = true
  setStatus("正在保存 Mihomo 设置...")

  try {
    const saved = await sendJSON("/api/settings/mihomo", "PUT", payload)
    state.mihomoSettings = {
      url: saved.url || "",
      secret: saved.secret || "",
    }
    state.settingsRequired = !state.mihomoSettings.url
    state.settingsOpen = false
    syncSettingsForm()
    syncSettingsUI()
    setStatus("Mihomo 设置已保存")
    await refreshAutoSwitchData()
    await loadData()
  } catch (error) {
    console.error(error)
    setStatus(error.message || "保存 Mihomo 设置失败", true)
  } finally {
    elements.settingsSaveBtn.disabled = false
  }
}

async function saveAutoSwitchSettings(event) {
  event.preventDefault()

  const payload = {
    enabled: elements.autoSwitchEnabled.checked,
    thresholdBytesPerMinute: megabytesToBytes(elements.autoSwitchThreshold.value),
    cooldownSeconds: Math.max(0, Number(elements.autoSwitchCooldown.value || 0)) * 60,
    groupTargets: collectAutoSwitchGroupTargets(),
  }

  elements.autoSwitchSaveBtn.disabled = true
  setStatus("正在保存自动切换配置...")

  try {
    const saved = await sendJSON("/api/auto-switch/settings", "PUT", payload)
    state.autoSwitch.enabled = Boolean(saved.enabled)
    state.autoSwitch.thresholdBytesPerMinute = Number(saved.thresholdBytesPerMinute || 0)
    state.autoSwitch.cooldownSeconds = Number(saved.cooldownSeconds || 0)
    state.autoSwitch.groupTargets = Array.isArray(saved.groupTargets) ? saved.groupTargets : []
    state.autoSwitch.groups = mergeAutoSwitchGroups(state.autoSwitch.groups, state.autoSwitch.groupTargets)
    state.autoSwitchOpen = false
    syncAutoSwitchForm()
    syncAutoSwitchUI()
    setStatus("自动切换配置已保存")
  } catch (error) {
    console.error(error)
    setStatus(error.message || "保存自动切换配置失败", true)
  } finally {
    elements.autoSwitchSaveBtn.disabled = false
  }
}

function renderCards(rows) {
  const upload = rows.reduce((sum, row) => sum + row.upload, 0)
  const download = rows.reduce((sum, row) => sum + row.download, 0)
  elements.countValue.textContent = String(rows.length)
  elements.uploadValue.textContent = formatBytes(upload)
  elements.downloadValue.textContent = formatBytes(download)
  elements.totalValue.textContent = formatBytes(upload + download)
}

function renderPrimaryTable(rows) {
  if (!rows.length) {
    elements.tableBody.innerHTML = '<div class="empty">当前时间范围内没有数据</div>'
    return
  }

  elements.tableBody.innerHTML = rows
    .slice(0, 120)
    .map((row, index) => {
      const active = state.selectedPrimary === row.label ? " active" : ""
      return `
        <div class="ranking-item primary-row${active}" tabindex="0" data-primary="${escapeHTML(row.label)}">
          <div class="ranking-main">
            <div class="ranking-title">
              <span class="rank">${index + 1}</span>
              <div class="mono">${renderTruncatedText(row.label, "host", "-")}</div>
            </div>
            <div class="ranking-total mono">${formatBytes(row.total)}</div>
          </div>
          <div class="ranking-metrics">
            <span>↑ ${formatBytes(row.upload)}</span>
            <span>↓ ${formatBytes(row.download)}</span>
          </div>
        </div>
      `
    })
    .join("")
}

function renderSecondaryTable(rows) {
  const filteredRows = rows.filter((row) =>
    row.label.toLowerCase().includes(state.detailSearchQuery.toLowerCase()),
  )

  if (!filteredRows.length) {
    const emptyText = state.selectedPrimary ? "当前分组下没有二级数据" : "选择左侧分组后加载"
    elements.secondaryBody.innerHTML = `<tr><td colspan="5" class="empty">${emptyText}</td></tr>`
    return
  }

  elements.secondaryBody.innerHTML = filteredRows
    .slice(0, 120)
    .map((row, index) => {
      const active = state.selectedSecondary === row.label ? " active" : ""
      return `
        <tr class="secondary-row${active}" tabindex="0" data-secondary="${escapeHTML(row.label)}">
          <td><span class="rank">${index + 1}</span></td>
          <td><div class="mono">${renderTruncatedText(row.label, "host", "-")}</div></td>
          <td>${formatBytes(row.upload)}</td>
          <td>${formatBytes(row.download)}</td>
          <td class="mono">${formatBytes(row.total)}</td>
        </tr>
      `
    })
    .join("")
}

function renderDetails(rows) {
  if (!state.selectedPrimary || !state.selectedSecondary) {
    elements.detailCards.innerHTML = '<div class="detail-empty">选择中间分组后查看链路明细</div>'
    return
  }

  if (!rows.length) {
    elements.detailCards.innerHTML = '<div class="detail-empty">当前选择下没有链路明细</div>'
    return
  }

  const cards = rows
    .slice(0, 120)
    .map((row) => {
      const chips = (row.chains || [])
        .map((item) => `<span class="chip route">${escapeHTML(item)}</span>`)
        .join("")

      return `
        <article class="detail-card">
          <div class="detail-card-head">
            <div class="detail-card-title mono">
              ${renderTruncatedText(row.destinationIP || row.outbound || "Unknown", "card-title")}
            </div>
            <div class="detail-card-total">${formatBytes(row.total)}</div>
          </div>
          <div class="detail-card-meta">
            <span title="${escapeHTML(row.sourceIP || "Inner")}">${escapeHTML(row.sourceIP || "Inner")}</span>
            <span title="${escapeHTML(row.outbound || "DIRECT")}">${escapeHTML(row.outbound || "DIRECT")}</span>
          </div>
          <div class="detail-card-meta">
            <span>↑ ${formatBytes(row.upload)}</span>
            <span>↓ ${formatBytes(row.download)}</span>
          </div>
          <div class="chips">${chips || '<span class="chip route">DIRECT</span>'}</div>
        </article>
      `
    })
    .join("")

  elements.detailCards.innerHTML = `<div class="detail-card-grid">${cards}</div>`
}

function hideTrendTooltip() {
  elements.trendTooltip.classList.add("hidden")
  elements.trendTooltip.setAttribute("aria-hidden", "true")
  elements.trendTooltip.innerHTML = ""

  if (state.trendHoverIndex === -1 || !state.lastTrendPoints.length) return

  state.trendHoverIndex = -1
  renderTrend(state.lastTrendPoints)
}

function showTrendTooltip(event) {
  if (!state.trendPlotPoints.length) return

  const rect = elements.trendCanvas.getBoundingClientRect()
  const pointerX = event.clientX - rect.left
  let nearestIndex = 0
  let nearestDistance = Number.POSITIVE_INFINITY

  state.trendPlotPoints.forEach((plotPoint, index) => {
    const distance = Math.abs(plotPoint.x - pointerX)
    if (distance < nearestDistance) {
      nearestDistance = distance
      nearestIndex = index
    }
  })

  if (nearestIndex !== state.trendHoverIndex) {
    state.trendHoverIndex = nearestIndex
    renderTrend(state.lastTrendPoints)
  }

  const activePoint = state.trendPlotPoints[nearestIndex]
  const total = activePoint.point.upload + activePoint.point.download
  elements.trendTooltip.innerHTML = `
    <div class="trend-tooltip-time">${escapeHTML(formatDateTime(activePoint.point.timestamp))}</div>
    <div class="trend-tooltip-metric total"><span>总流量</span><strong>${formatBytes(total)}</strong></div>
    <div class="trend-tooltip-metric upload"><span>上传</span><strong>${formatBytes(activePoint.point.upload)}</strong></div>
    <div class="trend-tooltip-metric download"><span>下载</span><strong>${formatBytes(activePoint.point.download)}</strong></div>
  `
  elements.trendTooltip.classList.remove("hidden")
  elements.trendTooltip.setAttribute("aria-hidden", "false")

  const tooltipWidth = elements.trendTooltip.offsetWidth
  const tooltipHeight = elements.trendTooltip.offsetHeight
  const anchorX = rect.left + activePoint.x
  const anchorY = rect.top + activePoint.totalY
  let left = anchorX + 14
  let top = anchorY - tooltipHeight - 14

  if (left + tooltipWidth > window.innerWidth - 12) {
    left = window.innerWidth - tooltipWidth - 12
  }
  if (left < 12) {
    left = 12
  }
  if (top < 12) {
    top = Math.min(anchorY + 18, window.innerHeight - tooltipHeight - 12)
  }

  elements.trendTooltip.style.left = `${left}px`
  elements.trendTooltip.style.top = `${top}px`
}

function drawTrendSeries(ctx, points, color, lineWidth) {
  if (!points.length) return

  ctx.beginPath()
  points.forEach(([x, y], index) => {
    if (index === 0) ctx.moveTo(x, y)
    else ctx.lineTo(x, y)
  })
  ctx.strokeStyle = color
  ctx.lineWidth = lineWidth
  ctx.stroke()
}

function renderTrend(points) {
  state.lastTrendPoints = points
  const canvas = elements.trendCanvas
  const dpr = window.devicePixelRatio || 1
  const rect = canvas.getBoundingClientRect()
  const width = Math.max(320, Math.floor(rect.width || 860))
  const height = Math.max(220, Math.floor(rect.height || 260))
  canvas.width = width * dpr
  canvas.height = height * dpr

  const ctx = canvas.getContext("2d")
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
  ctx.clearRect(0, 0, width, height)

  updateTrendAxisLabels(points)

  const values = points.map((point) => point.upload + point.download)
  const max = Math.max(...values, 1)
  const left = 56
  const right = width - 18
  const top = 18
  const bottom = height - 18

  ctx.strokeStyle = "rgba(167, 181, 198, 0.45)"
  ctx.lineWidth = 1
  for (let i = 0; i <= 4; i += 1) {
    const y = top + ((bottom - top) / 4) * i
    ctx.beginPath()
    ctx.moveTo(left, y)
    ctx.lineTo(right, y)
    ctx.stroke()
  }

  ctx.fillStyle = "#748399"
  ctx.font = '12px "Segoe UI", sans-serif'
  ctx.fillText(formatBytes(max), 8, top + 4)
  ctx.fillText("0 B", 18, bottom)

  state.trendPlotPoints = []
  if (!points.length) {
    state.trendHoverIndex = -1
    elements.trendTooltip.classList.add("hidden")
    elements.trendTooltip.setAttribute("aria-hidden", "true")
    elements.trendTooltip.innerHTML = ""
    return
  }

  const tickIndexes = getTrendTickIndexes(points.length)
  ctx.save()
  ctx.setLineDash([4, 6])
  ctx.strokeStyle = "rgba(167, 181, 198, 0.28)"
  tickIndexes.forEach((index) => {
    const x = left + ((right - left) * index) / Math.max(points.length - 1, 1)
    ctx.beginPath()
    ctx.moveTo(x, top)
    ctx.lineTo(x, bottom)
    ctx.stroke()
  })
  ctx.restore()

  const totalLinePoints = points.map((point, index) => {
    const x = left + ((right - left) * index) / Math.max(points.length - 1, 1)
    const totalY = bottom - ((bottom - top) * (point.upload + point.download)) / max
    const uploadY = bottom - ((bottom - top) * point.upload) / max
    const downloadY = bottom - ((bottom - top) * point.download) / max
    state.trendPlotPoints.push({
      x,
      totalY,
      uploadY,
      downloadY,
      point,
    })
    return [x, totalY]
  })
  const uploadLinePoints = state.trendPlotPoints.map(({ x, uploadY }) => [x, uploadY])
  const downloadLinePoints = state.trendPlotPoints.map(({ x, downloadY }) => [x, downloadY])

  const areaGradient = ctx.createLinearGradient(0, top, 0, bottom)
  areaGradient.addColorStop(0, "rgba(95, 70, 255, 0.18)")
  areaGradient.addColorStop(1, "rgba(95, 70, 255, 0.02)")

  ctx.beginPath()
  totalLinePoints.forEach(([x, y], index) => {
    if (index === 0) ctx.moveTo(x, y)
    else ctx.lineTo(x, y)
  })
  ctx.lineTo(right, bottom)
  ctx.lineTo(left, bottom)
  ctx.closePath()
  ctx.fillStyle = areaGradient
  ctx.fill()

  drawTrendSeries(ctx, totalLinePoints, "#5f46ff", 2.5)
  drawTrendSeries(ctx, uploadLinePoints, "#3db8ff", 2)
  drawTrendSeries(ctx, downloadLinePoints, "#31d184", 2)

  if (state.trendHoverIndex >= 0 && state.trendHoverIndex < state.trendPlotPoints.length) {
    const activePoint = state.trendPlotPoints[state.trendHoverIndex]
    ctx.save()
    ctx.setLineDash([4, 4])
    ctx.strokeStyle = "rgba(95, 70, 255, 0.42)"
    ctx.beginPath()
    ctx.moveTo(activePoint.x, top)
    ctx.lineTo(activePoint.x, bottom)
    ctx.stroke()
    ctx.restore()

    ;[
      { y: activePoint.totalY, color: "#5f46ff" },
      { y: activePoint.uploadY, color: "#3db8ff" },
      { y: activePoint.downloadY, color: "#31d184" },
    ].forEach(({ y, color }) => {
      ctx.beginPath()
      ctx.arc(activePoint.x, y, 4, 0, Math.PI * 2)
      ctx.fillStyle = "#fff"
      ctx.fill()
      ctx.beginPath()
      ctx.arc(activePoint.x, y, 2.5, 0, Math.PI * 2)
      ctx.fillStyle = color
      ctx.fill()
    })
  }
}

async function loadSecondaryRows(primaryLabel) {
  const { start, end } = getTimeRange()
  const dimension = elements.dimension.value

  if (!primaryLabel) {
    state.secondaryRows = []
    state.detailRows = []
    renderSecondaryTable([])
    renderDetails([])
    updateViewHints()
    return
  }

  let path = "/api/traffic/substats"
  let params
  if (dimension === "host") {
    path = "/api/traffic/devices-by-host"
    params = { host: primaryLabel, start, end }
  } else {
    params = { dimension, label: primaryLabel, start, end }
  }

  const rows = await fetchJSON(path, params)
  state.secondaryRows = rows
  state.selectedSecondary = rows[0]?.label || null
  renderSecondaryTable(rows)
  updateViewHints()

  if (state.selectedSecondary) {
    await loadDetails(primaryLabel, state.selectedSecondary)
  } else {
    state.detailRows = []
    renderDetails([])
    updateViewHints()
  }
}

async function loadDetails(primaryLabel, secondaryLabel) {
  const { start, end } = getTimeRange()
  const seq = ++state.detailSeq

  const rows = await fetchJSON("/api/traffic/details", {
    dimension: elements.dimension.value,
    primary: primaryLabel,
    secondary: secondaryLabel,
    start,
    end,
  })

  if (seq !== state.detailSeq) return

  state.detailRows = rows
  renderSecondaryTable(state.secondaryRows)
  renderDetails(rows)
  updateViewHints()
}

function resetDetailPanels() {
  state.selectedPrimary = null
  state.selectedSecondary = null
  state.secondaryRows = []
  state.detailRows = []
  state.detailSearchQuery = ""
  if (elements.detailSearch) elements.detailSearch.value = ""
  renderSecondaryTable([])
  renderDetails([])
  updateViewHints()
}

async function loadData() {
  if (!state.mihomoSettings.url) {
    state.settingsRequired = true
    state.settingsOpen = true
    syncSettingsUI()
    setStatus("请先填写 Mihomo URL 和 Secret", true)
    return
  }

  const { start, end } = getTimeRange()
  if (!Number.isFinite(start) || !Number.isFinite(end) || start <= 0 || end <= 0 || end < start) {
    setStatus("时间范围无效", true)
    return
  }

  const seq = ++state.loadSeq
  setStatus("加载中...")
  elements.refreshBtn.disabled = true

  try {
    resetDetailPanels()

    const [rows, trend] = await Promise.all([
      fetchJSON("/api/traffic/aggregate", {
        dimension: elements.dimension.value,
        start,
        end,
      }),
      fetchJSON("/api/traffic/trend", {
        start,
        end,
        bucket: bucketSize(start, end),
      }),
    ])

    if (seq !== state.loadSeq) return

    state.primaryRows = rows
    state.selectedPrimary = rows[0]?.label || null

    renderCards(rows)
    renderPrimaryTable(rows)
    renderTrend(trend)
    updateViewHints()

    if (state.selectedPrimary) {
      await loadSecondaryRows(state.selectedPrimary)
      renderPrimaryTable(state.primaryRows)
    }

    setStatus("")
  } catch (error) {
    console.error(error)
    setStatus(error.message || "加载失败", true)
  } finally {
    elements.refreshBtn.disabled = false
  }
}

async function clearLogs() {
  if (!window.confirm("确认清空所有历史流量记录吗？")) return

  setStatus("正在清空...")
  elements.clearBtn.disabled = true

  try {
    const response = await fetch("/api/traffic/logs", { method: "DELETE" })
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}))
      throw new Error(payload.error || "清空失败")
    }
    await loadData()
  } catch (error) {
    console.error(error)
    setStatus(error.message || "清空失败", true)
  } finally {
    elements.clearBtn.disabled = false
  }
}

elements.range.addEventListener("change", () => {
  if (Number(elements.range.value) !== -1) updateCustomInputs()
  syncContextSummary()
  loadData()
})

elements.start.addEventListener("change", () => {
  elements.range.value = "-1"
  syncContextSummary()
  loadData()
})

elements.end.addEventListener("change", () => {
  elements.range.value = "-1"
  syncContextSummary()
  loadData()
})

elements.dimensionTabs.forEach((button) => {
  button.addEventListener("click", () => {
    const nextDimension = button.dataset.dimension
    if (!nextDimension || nextDimension === elements.dimension.value) return
    elements.dimension.value = nextDimension
    persistSelectedDimension(nextDimension)
    updateViewHints()
    loadData()
  })
})

elements.settingsBtn.addEventListener("click", openSettingsPanel)
elements.settingsForm.addEventListener("submit", saveSettings)
elements.settingsCancelBtn.addEventListener("click", closeSettingsPanel)
elements.autoSwitchBtn.addEventListener("click", openAutoSwitchPanel)
elements.autoSwitchForm.addEventListener("submit", saveAutoSwitchSettings)
elements.autoSwitchCancelBtn.addEventListener("click", closeAutoSwitchPanel)
elements.autoSwitchRefreshBtn.addEventListener("click", async () => {
  elements.autoSwitchRefreshBtn.disabled = true
  setStatus("正在刷新可控策略组...")
  try {
    await loadAutoSwitchGroups()
    setStatus("可控策略组已刷新")
  } catch (error) {
    console.error(error)
    setStatus(error.message || "刷新可控策略组失败", true)
  } finally {
    elements.autoSwitchRefreshBtn.disabled = false
  }
})
elements.refreshBtn.addEventListener("click", loadData)
elements.clearBtn.addEventListener("click", clearLogs)
elements.detailSearch.addEventListener("input", (event) => {
  state.detailSearchQuery = event.target.value || ""
  renderSecondaryTable(state.secondaryRows)
})
elements.trendCanvas.addEventListener("mousemove", showTrendTooltip)
elements.trendCanvas.addEventListener("mouseleave", hideTrendTooltip)

elements.tableBody.addEventListener("click", async (event) => {
  const row = event.target.closest("[data-primary]")
  if (!row) return
  state.selectedPrimary = row.dataset.primary
  state.selectedSecondary = null
  state.detailRows = []
  renderPrimaryTable(state.primaryRows)
  renderDetails([])
  updateViewHints()
  try {
    await loadSecondaryRows(state.selectedPrimary)
  } catch (error) {
    console.error(error)
    setStatus(error.message || "加载二级明细失败", true)
  }
})

elements.tableBody.addEventListener("keydown", (event) => {
  if (event.key !== "Enter" && event.key !== " ") return
  const row = event.target.closest("[data-primary]")
  if (!row) return
  event.preventDefault()
  row.click()
})

elements.secondaryBody.addEventListener("click", async (event) => {
  const row = event.target.closest("[data-secondary]")
  if (!row || !state.selectedPrimary) return
  state.selectedSecondary = row.dataset.secondary
  renderSecondaryTable(state.secondaryRows)
  renderDetails([])
  updateViewHints()
  try {
    await loadDetails(state.selectedPrimary, state.selectedSecondary)
  } catch (error) {
    console.error(error)
    setStatus(error.message || "加载链路明细失败", true)
  }
})

elements.secondaryBody.addEventListener("keydown", (event) => {
  if (event.key !== "Enter" && event.key !== " ") return
  const row = event.target.closest("[data-secondary]")
  if (!row) return
  event.preventDefault()
  row.click()
})

window.addEventListener("resize", () => {
  elements.trendTooltip.classList.add("hidden")
  state.trendHoverIndex = -1
  renderTrend(state.lastTrendPoints)
})

async function initializeApp() {
  const storedDimension = loadStoredDimension()
  if (storedDimension) {
    elements.dimension.value = storedDimension
  } else {
    persistSelectedDimension(elements.dimension.value)
  }

  updateCustomInputs()
  updateViewHints()
  renderCards([])
  renderPrimaryTable([])
  renderTrend([])
  resetDetailPanels()
  syncAutoSwitchUI()

  try {
    await loadSettings()
    if (state.settingsRequired) {
      setStatus("请先填写 Mihomo URL 和 Secret")
      return
    }
    await refreshAutoSwitchData()
    await loadData()
  } catch (error) {
    console.error(error)
    state.settingsRequired = true
    state.settingsOpen = true
    syncSettingsUI()
    setStatus(error.message || "加载 Mihomo 设置失败", true)
  }
}

initializeApp()
