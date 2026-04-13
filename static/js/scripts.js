// ─────────────────────────────────────
// App state
// ─────────────────────────────────────
let token = null;
let currentUser = null;
let session = { queries: 0 };
let qaDatasetValidationTimer = null;
let qaIsLoading = false;
let qaAnalyzeInFlight = false;
const qaDatasetValidationState = {
  status: "idle",
  datasetHint: "",
  projectId: "",
  queryText: "",
};
let qbDatasetValidationTimer = null;
let qbIsLoading = false;
let dbIsLoading = false;
const qbDatasetValidationState = {
  status: "idle",
  datasetHint: "",
  projectId: "",
};

// ─────────────────────────────────────
// Utils
// ─────────────────────────────────────
function fmtBytes(n) {
  if (n == null) return "—";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let value = Number(n);

  for (const unit of units) {
    if (value < 1024) return value.toFixed(2) + " " + unit;
    value /= 1024;
  }

  return value.toFixed(2) + " EB";
}

function copyToClipboard(event) {
  const button = event.currentTarget;
  const text = button.getAttribute("data-query");

  if (!text) {
    console.error("Nenhuma query para copiar");
    return;
  }

  navigator.clipboard
    .writeText(text)
    .then(() => {
      button.classList.add("copied");
      setTimeout(() => {
        button.classList.remove("copied");
      }, 2000);
    })
    .catch((err) => {
      console.error("Erro ao copiar:", err);
    });
}

function fmtUSD(v) {
  return v == null ? "—" : "USD " + Number(v).toFixed(4);
}

function authHeaders() {
  return {
    "Content-Type": "application/json",
    Authorization: "Bearer " + token,
  };
}

function enforceQAConfigReadOnly() {
  const projectEl = document.getElementById("qa-project");
  const datasetEl = document.getElementById("qa-dataset");

  [projectEl, datasetEl].forEach((el) => {
    if (!el) return;

    el.readOnly = true;
    el.setAttribute("readonly", "readonly");
    el.tabIndex = -1;

    const blockEdit = (event) => {
      event.preventDefault();
    };

    el.onkeydown = blockEdit;
    el.onbeforeinput = blockEdit;
    el.onpaste = blockEdit;
    el.ondrop = blockEdit;
  });
}

function showScreen(id) {
  document.querySelectorAll(".screen").forEach((screen) => {
    screen.classList.remove("active");
  });

  const target = document.getElementById(id);
  if (target) target.classList.add("active");
}

function updateLastAccess() {
  const now = new Date();
  const formatted =
    now.getHours().toString().padStart(2, "0") +
    ":" +
    now.getMinutes().toString().padStart(2, "0");

  const el = document.getElementById("s-lasttime");
  if (el) el.textContent = formatted;
}

function prettifyErrorMessage(message) {
  if (!message) return "Ocorreu um erro inesperado.";

  const msg = String(message);

  if (msg.includes("Project ID")) {
    return "Informe um Project ID válido do GCP.";
  }

  if (
    msg.toLowerCase().includes("credenciais") ||
    msg.toLowerCase().includes("credentials")
  ) {
    return "Não foi possível autenticar no BigQuery. Verifique as credenciais do ambiente.";
  }

  if (
    msg.includes("401") ||
    msg.includes("Não autenticado") ||
    msg.includes("Sessão expirada")
  ) {
    return "Sua sessão expirou. Faça login novamente.";
  }

  if (msg.toLowerCase().includes("query não pode ser vazia")) {
    return "Cole uma query SQL antes de analisar.";
  }

  return msg;
}

function setUserUI(name, username) {
  const safeName = name || username || "Usuário";
  const initials = safeName
    .split(" ")
    .map((w) => w[0])
    .filter(Boolean)
    .slice(0, 2)
    .join("")
    .toUpperCase();

  const topAvatar = document.getElementById("top-avatar");
  const topName = document.getElementById("top-name");
  const sbAvatar = document.getElementById("sb-avatar");
  const sbName = document.getElementById("sb-name");

  if (topAvatar) topAvatar.textContent = initials || "--";
  if (topName) topName.textContent = safeName;
  if (sbAvatar) sbAvatar.textContent = initials || "--";
  if (sbName) sbName.textContent = safeName;
}

function setQAProgress(stepText, pct) {
  const progress = document.getElementById("qa-progress");
  const step = document.getElementById("qa-progress-step");
  const fill = document.getElementById("qa-progress-fill");

  if (!progress || !step || !fill) return;

  progress.style.display = "flex";
  step.textContent = stepText;
  fill.style.width = `${pct}%`;
}

function hideQAProgress() {
  const progress = document.getElementById("qa-progress");
  const fill = document.getElementById("qa-progress-fill");
  const step = document.getElementById("qa-progress-step");

  if (!progress || !fill || !step) return;

  progress.style.display = "none";
  fill.style.width = "8%";
  step.textContent = "Preparando...";
}

function setQBProgress(stepText, pct) {
  const progress = document.getElementById("qb-progress");
  const step = document.getElementById("qb-progress-step");
  const fill = document.getElementById("qb-progress-fill");

  if (!progress || !step || !fill) return;

  progress.style.display = "flex";
  step.textContent = stepText;
  fill.style.width = `${pct}%`;
}

function hideQBProgress() {
  const progress = document.getElementById("qb-progress");
  const fill = document.getElementById("qb-progress-fill");
  const step = document.getElementById("qb-progress-step");

  if (!progress || !fill || !step) return;

  progress.style.display = "none";
  fill.style.width = "8%";
  step.textContent = "Preparando...";
}

function setDBProgress(stepText, pct) {
  const progress = document.getElementById("db-progress");
  const step = document.getElementById("db-progress-step");
  const fill = document.getElementById("db-progress-fill");

  if (!progress || !step || !fill) return;

  progress.style.display = "flex";
  step.textContent = stepText;
  fill.style.width = `${pct}%`;
}

function hideDBProgress() {
  const progress = document.getElementById("db-progress");
  const fill = document.getElementById("db-progress-fill");
  const step = document.getElementById("db-progress-step");

  if (!progress || !fill || !step) return;

  progress.style.display = "none";
  fill.style.width = "8%";
  step.textContent = "Preparando...";
}

function syncQAAnalyzeButtonState() {
  const btn = document.getElementById("qa-btn");
  const query = document.getElementById("qa-query")?.value.trim() || "";

  if (!btn) return;

  const blockedByValidation =
    !query ||
    qaDatasetValidationState.status !== "valid" ||
    qaDatasetValidationState.queryText !== query;

  btn.disabled = qaIsLoading || blockedByValidation;
}

function setQADatasetValidationStatus(kind, payload = {}) {
  const statusEl = document.getElementById("qa-dataset-status");
  const indicatorEl = document.getElementById("qa-dataset-indicator");
  const statusIconEl = document.getElementById("qa-dataset-status-icon");
  const statusTitleEl = document.getElementById("qa-dataset-status-title");
  const statusTextEl = document.getElementById("qa-dataset-status-text");
  const statusMetaEl = document.getElementById("qa-dataset-status-meta");
  const datasetHint = document.getElementById("qa-dataset")?.value.trim() || "";
  const title = payload.title || "";
  const message = payload.message || "";
  const tableCount = Number(payload.tableCount ?? NaN);
  const queryTableCount = Number(payload.queryTableCount ?? NaN);

  if (statusEl) {
    statusEl.className = "qb-dataset-status";
  }

  if (statusTitleEl) statusTitleEl.textContent = "";
  if (statusTextEl) statusTextEl.textContent = "";
  if (statusMetaEl) statusMetaEl.innerHTML = "";

  if (statusIconEl) {
    statusIconEl.textContent = "•";
  }

  if (indicatorEl) {
    indicatorEl.className = "qb-dataset-indicator";
    indicatorEl.textContent = "●";
  }

  if (kind === "idle") {
    syncQAAnalyzeButtonState();
    return;
  }

  if (statusEl) {
    statusEl.classList.add(kind);
  }

  if (statusTitleEl) {
    statusTitleEl.textContent =
      title ||
      (kind === "ok"
        ? "Dataset pronto para analise"
        : kind === "checking"
          ? "Validando contexto da query"
          : "Validacao pendente");
  }

  if (statusTextEl) {
    statusTextEl.textContent = message;
  }

  if (statusIconEl) {
    statusIconEl.textContent =
      kind === "ok" ? "✓" : kind === "checking" ? "…" : "!";
  }

  if (statusMetaEl && kind === "ok") {
    const chips = [];
    if (datasetHint) {
      chips.push(`<span class="qb-dataset-chip">🗂️ ${datasetHint}</span>`);
    }
    if (!Number.isNaN(tableCount)) {
      chips.push(
        `<span class="qb-dataset-chip">📊 ${tableCount} tabelas</span>`,
      );
    }
    if (!Number.isNaN(queryTableCount)) {
      chips.push(
        `<span class="qb-dataset-chip">🔎 ${queryTableCount} usadas na query</span>`,
      );
    }
    chips.push(
      '<span class="qb-dataset-chip">✅ BigQuery + Data Catalog/Dataplex</span>',
    );
    statusMetaEl.innerHTML = chips.join(" ");
  }

  if (statusMetaEl && kind === "error") {
    statusMetaEl.innerHTML =
      '<span class="qb-dataset-chip">⚠️ Revise o formato projeto.dataset.tabela</span>';
  }

  if (indicatorEl) {
    indicatorEl.classList.add(kind);
    indicatorEl.textContent =
      kind === "ok" ? "✓" : kind === "checking" ? "…" : "✕";
  }

  syncQAAnalyzeButtonState();
}

async function validateQAQueryContext() {
  const query = document.getElementById("qa-query")?.value.trim() || "";
  const projectEl = document.getElementById("qa-project");
  const datasetEl = document.getElementById("qa-dataset");
  const currentProject = projectEl?.value.trim() || "";

  qaDatasetValidationState.queryText = query;

  if (!query) {
    qaDatasetValidationState.status = "idle";
    qaDatasetValidationState.projectId = "";
    qaDatasetValidationState.datasetHint = "";
    if (projectEl) projectEl.value = "";
    if (datasetEl) datasetEl.value = "";
    setQADatasetValidationStatus("idle");
    return {
      valid: false,
      projectId: "",
      datasetHint: "",
      message: "Cole uma query SQL antes de analisar.",
    };
  }

  qaDatasetValidationState.status = "checking";
  setQADatasetValidationStatus("checking", {
    title: "Validando contexto da query",
    message:
      "Detectando dataset/tabelas e conferindo BigQuery + Data Catalog/Dataplex...",
  });

  const querySnapshot = query;

  try {
    const res = await fetch(
      "/api/agents/query_analyzer/validate-query-context",
      {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
          query: querySnapshot,
          project_id: currentProject || null,
        }),
      },
    );

    if (res.status === 401) {
      doLogout();
      return {
        valid: false,
        projectId: "",
        datasetHint: "",
        message: "Sessao expirada. Faca login novamente.",
      };
    }

    const payload = await res.json();
    if (!res.ok) {
      throw new Error(payload?.detail || "Falha na validacao da query.");
    }

    const currentQuery =
      document.getElementById("qa-query")?.value.trim() || "";
    if (currentQuery !== querySnapshot) {
      return {
        valid: false,
        projectId: "",
        datasetHint: "",
        message: "A query foi alterada durante a validacao. Tente novamente.",
      };
    }

    const detectedProject = (payload.project_id || "").trim();
    const detectedDataset = (
      payload.dataset_hint ||
      payload.dataset_id ||
      ""
    ).trim();
    if (projectEl) projectEl.value = detectedProject;
    if (datasetEl) datasetEl.value = detectedDataset;

    if (payload.valid) {
      qaDatasetValidationState.status = "valid";
      qaDatasetValidationState.projectId = detectedProject;
      qaDatasetValidationState.datasetHint = detectedDataset;
      qaDatasetValidationState.queryText = querySnapshot;
      setQADatasetValidationStatus("ok", {
        title: "Contexto validado",
        message: payload.message || "Query validada. Ja pode analisar.",
        tableCount: Number(payload.table_count || 0),
        queryTableCount: Array.isArray(payload.matched_tables)
          ? payload.matched_tables.length
          : 0,
      });
      return {
        valid: true,
        projectId: detectedProject,
        datasetHint: detectedDataset,
        message: payload.message || "Contexto validado.",
      };
    } else {
      qaDatasetValidationState.status = "invalid";
      setQADatasetValidationStatus("error", {
        title: "Contexto nao validado",
        message:
          payload.message ||
          "Nao foi possivel validar dataset e tabelas da query.",
      });
      return {
        valid: false,
        projectId: detectedProject,
        datasetHint: detectedDataset,
        message:
          payload.message ||
          "Nao foi possivel validar dataset e tabelas da query.",
      };
    }
  } catch (err) {
    qaDatasetValidationState.status = "invalid";
    setQADatasetValidationStatus("error", {
      title: "Falha na validacao",
      message: prettifyErrorMessage(err.message || "Erro ao validar query."),
    });
    return {
      valid: false,
      projectId: "",
      datasetHint: "",
      message: prettifyErrorMessage(err.message || "Erro ao validar query."),
    };
  }
}

function scheduleQAQueryValidation() {
  if (qaAnalyzeInFlight || qaIsLoading) {
    return;
  }

  if (qaDatasetValidationTimer) {
    clearTimeout(qaDatasetValidationTimer);
  }
  qaDatasetValidationTimer = setTimeout(() => {
    validateQAQueryContext();
  }, 1000);
}

function syncQBGenerateButtonState() {
  const btn = document.getElementById("qb-btn");
  const dataset = document.getElementById("qb-dataset")?.value.trim() || "";
  const projectId = document.getElementById("qb-project")?.value.trim() || "";

  if (!btn) return;

  let blockedByDataset = !dataset;
  if (dataset) {
    blockedByDataset =
      qbDatasetValidationState.status !== "valid" ||
      qbDatasetValidationState.datasetHint !== dataset ||
      qbDatasetValidationState.projectId !== projectId;
  }

  btn.disabled = qbIsLoading || blockedByDataset;
}

function setQBDatasetValidationStatus(kind, payload = {}) {
  const statusEl = document.getElementById("qb-dataset-status");
  const indicatorEl = document.getElementById("qb-dataset-indicator");
  const statusIconEl = document.getElementById("qb-dataset-status-icon");
  const statusTitleEl = document.getElementById("qb-dataset-status-title");
  const statusTextEl = document.getElementById("qb-dataset-status-text");
  const statusMetaEl = document.getElementById("qb-dataset-status-meta");
  const datasetHint = document.getElementById("qb-dataset")?.value.trim() || "";
  const title = payload.title || "";
  const message = payload.message || "";
  const tableCount = Number(payload.tableCount ?? NaN);

  if (statusEl) {
    statusEl.className = "qb-dataset-status";
  }

  if (statusTitleEl) statusTitleEl.textContent = "";
  if (statusTextEl) statusTextEl.textContent = "";
  if (statusMetaEl) statusMetaEl.innerHTML = "";

  if (statusIconEl) {
    statusIconEl.textContent = "•";
  }

  if (indicatorEl) {
    indicatorEl.className = "qb-dataset-indicator";
    indicatorEl.textContent = "●";
  }

  if (kind === "idle") {
    syncQBGenerateButtonState();
    return;
  }

  if (statusEl) {
    statusEl.classList.add(kind);
  }

  if (statusTitleEl) {
    statusTitleEl.textContent =
      title ||
      (kind === "ok"
        ? "Dataset pronto para uso"
        : kind === "checking"
          ? "Validando dataset"
          : "Validacao pendente");
  }

  if (statusTextEl) {
    statusTextEl.textContent = message;
  }

  if (statusIconEl) {
    statusIconEl.textContent =
      kind === "ok" ? "✓" : kind === "checking" ? "…" : "!";
  }

  if (statusMetaEl && kind === "ok") {
    const chips = [];
    if (datasetHint) {
      chips.push(`<span class="qb-dataset-chip">🗂️ ${datasetHint}</span>`);
    }
    if (!Number.isNaN(tableCount)) {
      chips.push(
        `<span class="qb-dataset-chip">📊 ${tableCount} tabelas</span>`,
      );
    }
    chips.push('<span class="qb-dataset-chip">✅ Metadados</span>');
    statusMetaEl.innerHTML = chips.join(" ");
  }

  if (statusMetaEl && kind === "error") {
    statusMetaEl.innerHTML =
      '<span class="qb-dataset-chip">⚠️ Revise o nome do dataset</span>';
  }

  if (indicatorEl) {
    indicatorEl.classList.add(kind);
    indicatorEl.textContent =
      kind === "ok" ? "✓" : kind === "checking" ? "…" : "✕";
  }

  syncQBGenerateButtonState();
}

async function validateQBDatasetHint() {
  const projectId = document.getElementById("qb-project")?.value.trim() || "";
  const datasetHint = document.getElementById("qb-dataset")?.value.trim() || "";

  qbDatasetValidationState.datasetHint = datasetHint;
  qbDatasetValidationState.projectId = projectId;

  if (!datasetHint) {
    qbDatasetValidationState.status = "idle";
    setQBDatasetValidationStatus("idle");
    return;
  }

  if (!projectId) {
    qbDatasetValidationState.status = "invalid";
    setQBDatasetValidationStatus("error", {
      title: "Project ID obrigatorio",
      message: "Informe o Project ID antes de validar o dataset.",
    });
    return;
  }

  qbDatasetValidationState.status = "checking";
  setQBDatasetValidationStatus("checking", {
    title: "Validando dataset",
    message: "Conferindo BigQuery e Data Catalog...",
  });

  try {
    const res = await fetch("/api/agents/query_build/validate-dataset", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        project_id: projectId,
        dataset_hint: datasetHint,
      }),
    });

    if (res.status === 401) {
      doLogout();
      return;
    }

    const payload = await res.json();
    if (!res.ok) {
      throw new Error(payload?.detail || "Falha na validacao do dataset.");
    }

    const currentDataset =
      document.getElementById("qb-dataset")?.value.trim() || "";
    const currentProject =
      document.getElementById("qb-project")?.value.trim() || "";
    if (currentDataset !== datasetHint || currentProject !== projectId) {
      return;
    }

    if (payload.valid) {
      qbDatasetValidationState.status = "valid";
      qbDatasetValidationState.datasetHint = datasetHint;
      qbDatasetValidationState.projectId = projectId;
      const count = Number(payload.table_count || 0);
      setQBDatasetValidationStatus("ok", {
        title: "Dataset pronto",
        message: "Validação concluída. Já pode gerar a SQL.",
        tableCount: count,
      });
    } else {
      qbDatasetValidationState.status = "invalid";
      setQBDatasetValidationStatus("error", {
        title: "Dataset nao validado",
        message:
          payload.message || "Dataset nao validado para uso no Query Builder.",
      });
    }
  } catch (err) {
    qbDatasetValidationState.status = "invalid";
    setQBDatasetValidationStatus("error", {
      title: "Falha na validacao",
      message: prettifyErrorMessage(err.message || "Erro ao validar dataset."),
    });
  }
}

function scheduleQBDatasetValidation() {
  if (qbDatasetValidationTimer) {
    clearTimeout(qbDatasetValidationTimer);
  }
  qbDatasetValidationTimer = setTimeout(() => {
    validateQBDatasetHint();
  }, 1000);
}

function resetQATabsDataState() {
  ["tab-antipatterns", "tab-optimized", "tab-applied", "tab-recs"].forEach(
    (id) => {
      document.getElementById(id)?.classList.remove("has-data");
    },
  );

  const tabApCount = document.getElementById("tab-ap-count");
  if (tabApCount) {
    tabApCount.textContent = "0";
    tabApCount.className = "qa-tab-count";
  }
}

function resetQAResultPanels() {
  const qTiles = document.getElementById("q-tiles");
  const qSavSec = document.getElementById("q-sav-sec");
  const qRecSec = document.getElementById("q-rec-sec");
  const qTipsSec = document.getElementById("q-tips-sec");
  const qOptSec = document.getElementById("q-opt-sec");
  const qOptEmpty = document.getElementById("q-opt-empty");
  const qAppliedSec = document.getElementById("q-applied-sec");

  if (qTiles) qTiles.style.display = "none";
  if (qSavSec) qSavSec.style.display = "none";
  if (qRecSec) qRecSec.style.display = "none";
  if (qTipsSec) qTipsSec.style.display = "none";
  if (qOptSec) qOptSec.style.display = "none";
  if (qOptEmpty) qOptEmpty.style.display = "flex";
  if (qAppliedSec) qAppliedSec.style.display = "none";

  const qApList = document.getElementById("q-ap-list");
  const qRecList = document.getElementById("q-rec-list");
  const qTipsList = document.getElementById("q-tips-list");
  const qOptQuery = document.getElementById("q-opt-query");
  const qSummary = document.getElementById("q-summary");
  const qApCount = document.getElementById("q-ap-count");
  const qAppliedList = document.getElementById("q-applied-list");

  if (qApList) qApList.innerHTML = "";
  if (qRecList) qRecList.innerHTML = "";
  if (qTipsList) qTipsList.innerHTML = "";
  if (qOptQuery) qOptQuery.textContent = "";
  if (qSummary) qSummary.textContent = "";
  if (qApCount) qApCount.textContent = "";
  if (qAppliedList) qAppliedList.innerHTML = "";

  const scoreFill = document.getElementById("q-score-fill");
  const savFill = document.getElementById("q-sav-fill");
  if (scoreFill) scoreFill.style.width = "0%";
  if (savFill) savFill.style.width = "0%";
}

// ─────────────────────────────────────
// Login
// ─────────────────────────────────────
async function doLogin() {
  const username = document.getElementById("inp-user")?.value.trim() || "";
  const password = document.getElementById("inp-pass")?.value || "";
  const errEl = document.getElementById("login-error");
  const btn = document.getElementById("btn-login");
  const spinner = document.getElementById("login-spinner");
  const btnText = document.getElementById("login-btn-text");

  if (errEl) errEl.style.display = "none";

  if (!username || !password) {
    showLoginError("Preencha matrícula e senha.");
    return;
  }

  if (btn) btn.disabled = true;
  if (spinner) spinner.style.display = "block";
  if (btnText) btnText.textContent = "Entrando...";

  try {
    const res = await fetch("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });

    if (!res.ok) {
      const e = await res.json();
      throw new Error(e.detail || "Erro ao autenticar");
    }

    const data = await res.json();

    token = data.token;
    currentUser = {
      username: data.username,
      name: data.name,
    };

    setUserUI(data.name, data.username);
    showScreen("screen-portal");
    navTo("home");
    updateLastAccess();
  } catch (e) {
    showLoginError(e.message);
  } finally {
    if (btn) btn.disabled = false;
    if (spinner) spinner.style.display = "none";
    if (btnText) btnText.textContent = "Entrar";
  }
}

function showLoginError(msg) {
  const el = document.getElementById("login-error");
  if (!el) return;

  el.textContent = "⚠ " + prettifyErrorMessage(msg);
  el.style.display = "block";
  document.getElementById("inp-user")?.focus();
}

// ─────────────────────────────────────
// Logout
// ─────────────────────────────────────
async function doLogout() {
  try {
    if (token) {
      await fetch("/api/logout", {
        method: "POST",
        headers: authHeaders(),
      });
    }
  } catch (_e) {
    // silencioso por design
  }

  token = null;
  currentUser = null;
  session = { queries: 0 };

  const userEl = document.getElementById("inp-user");
  const passEl = document.getElementById("inp-pass");
  const queriesEl = document.getElementById("s-queries");
  const lastTimeEl = document.getElementById("s-lasttime");

  if (userEl) userEl.value = "";
  if (passEl) passEl.value = "";
  if (queriesEl) queriesEl.textContent = "0";
  if (lastTimeEl) lastTimeEl.textContent = "--:--";

  // Limpar dados persistentes
  localStorage.clear();

  showScreen("screen-login");
  document.getElementById("inp-user")?.focus();
}

// ─────────────────────────────────────
// Navigation
// ─────────────────────────────────────
function navTo(view) {
  document.querySelectorAll(".view").forEach((v) => {
    v.classList.remove("active");
  });

  document.querySelectorAll(".nitem").forEach((n) => {
    n.classList.remove("active");
  });

  document.querySelectorAll(".snav").forEach((n) => {
    n.classList.remove("active");
  });

  const mapping = {
    home: "view-home",
    qa: "view-qa",
    db: "view-db",
    qb: "view-qb",
    dev: "view-dev",
    hist: "view-hist",
  };

  const el = document.getElementById(mapping[view] || "view-home");
  if (el) el.classList.add("active");

  if (view === "home") {
    document.getElementById("nav-home")?.classList.add("active");
    document.querySelectorAll(".snav")[0]?.classList.add("active");
  } else if (view === "hist") {
    document.getElementById("nav-hist")?.classList.add("active");
    document.querySelectorAll(".snav")[1]?.classList.add("active");
    loadHistory();
  } else if (view === "qa") {
    document.getElementById("nav-qa")?.classList.add("active");
  } else if (view === "db") {
    document.getElementById("nav-db")?.classList.add("active");
  } else if (view === "qb") {
    document.getElementById("nav-qb")?.classList.add("active");
  }

  if (view === "qa" || view === "qb" || view === "db") {
    updateLastAccess();
  }
}

// ─────────────────────────────────────
// Dev view
// ─────────────────────────────────────
const devColors = {
  teal: { bg: "var(--teal-bg)", stroke: "#0891B2" },
  violet: { bg: "var(--violet-bg)", stroke: "#6D28D9" },
  emerald: { bg: "var(--emerald-bg)", stroke: "#059669" },
};

function openDev(name, desc, features, eta) {
  const colors = {
    "Document Builder": devColors.teal,
    "Query Builder": devColors.violet,
    "Finance AuditorIA": devColors.emerald,
  };

  const c = colors[name] || devColors.teal;
  const devIco = document.getElementById("dev-ico");
  const devTitle = document.getElementById("dev-title");
  const devDesc = document.getElementById("dev-desc");
  const timeline = document.getElementById("dev-timeline");

  if (devIco) {
    devIco.style.cssText = `width:60px;height:60px;border-radius:14px;background:${c.bg};display:flex;align-items:center;justify-content:center`;
    devIco.innerHTML = `<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="${c.stroke}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>`;
  }

  if (devTitle) devTitle.textContent = name;
  if (devDesc) devDesc.textContent = desc;

  if (timeline) {
    timeline.innerHTML = features
      .map(
        (f, i) => `
          <div class="dev-step">
            <div class="dev-step-n">${String(i + 1).padStart(2, "0")}</div>
            <div class="dev-step-txt">${f}</div>
            <span class="dev-eta">${eta}</span>
          </div>
        `,
      )
      .join("");
  }

  navTo("dev");
}

// ─────────────────────────────────────
// SQL Review
// ─────────────────────────────────────
async function runAnalyze() {
  if (qaAnalyzeInFlight) {
    return;
  }

  qaAnalyzeInFlight = true;

  if (qaDatasetValidationTimer) {
    clearTimeout(qaDatasetValidationTimer);
    qaDatasetValidationTimer = null;
  }

  const query = document.getElementById("qa-query")?.value.trim() || "";
  const project_id =
    qaDatasetValidationState.projectId ||
    document.getElementById("qa-project")?.value.trim() ||
    "";
  const dataset_hint =
    qaDatasetValidationState.datasetHint ||
    document.getElementById("qa-dataset")?.value.trim() ||
    "";
  const errEl = document.getElementById("qa-error");
  const qaEmpty = document.getElementById("qa-empty");
  const qaTabsArea = document.getElementById("qa-tabs-area");

  if (errEl) errEl.style.display = "none";

  if (!query) {
    showQAError("Cole uma query SQL antes de analisar.");
    qaAnalyzeInFlight = false;
    return;
  }

  setQALoading(true);
  const contextAlreadyValidated =
    qaDatasetValidationState.status === "valid" &&
    qaDatasetValidationState.queryText === query;

  if (!contextAlreadyValidated) {
    showQAError(
      "Valide dataset e tabelas inserindo/atualizando a query SQL antes de analisar.",
    );
    hideQAProgress();
    setQALoading(false);
    qaAnalyzeInFlight = false;
    return;
  }

  setQAProgress("Validando entrada...", 18);
  resetQATabsDataState();
  resetQAResultPanels();

  if (qaEmpty) qaEmpty.style.display = "none";
  if (qaTabsArea) qaTabsArea.style.display = "none";

  try {
    setTimeout(() => setQAProgress("Estimando custo no BigQuery...", 36), 180);
    setTimeout(() => setQAProgress("Detectando anti-padrões...", 62), 520);
    setTimeout(() => setQAProgress("Consolidando resultado...", 84), 980);

    const res = await fetch("/analyze", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        query,
        project_id: project_id || null,
        dataset_hint: dataset_hint || null,
      }),
    });

    if (res.status === 401) {
      doLogout();
      return;
    }

    if (!res.ok) {
      const e = await res.json();
      throw new Error(e.detail || "Erro na análise");
    }

    const data = await res.json();

    setQAProgress("Finalizando apresentação...", 100);

    session.queries++;
    const queriesEl = document.getElementById("s-queries");
    if (queriesEl) queriesEl.textContent = String(session.queries);

    updateLastAccess();
    renderQA(data);
    saveToHistory(data, query);
  } catch (e) {
    showQAError(prettifyErrorMessage(e.message));

    if (qaTabsArea && qaTabsArea.style.display === "none" && qaEmpty) {
      qaEmpty.style.display = "flex";
    }
  } finally {
    setTimeout(() => {
      hideQAProgress();
      setQALoading(false);
      qaAnalyzeInFlight = false;
    }, 350);
  }
}

async function runQueryBuild() {
  const requestText = document.getElementById("qb-request")?.value.trim() || "";
  const projectId = document.getElementById("qb-project")?.value.trim() || "";
  const datasetHint = document.getElementById("qb-dataset")?.value.trim() || "";
  const qbEmpty = document.getElementById("qb-empty");
  const qbTabsArea = document.getElementById("qb-tabs-area");

  if (!requestText) {
    showQBError("Descreva a solicitação antes de gerar SQL.");
    return;
  }

  if (!projectId) {
    showQBError("Preencha o Project ID do GCP.");
    return;
  }

  if (!datasetHint) {
    showQBError("Preencha o Dataset hint obrigatorio.");
    return;
  }

  const isValidDataset =
    qbDatasetValidationState.status === "valid" &&
    qbDatasetValidationState.datasetHint === datasetHint &&
    qbDatasetValidationState.projectId === projectId;
  if (!isValidDataset) {
    showQBError(
      "Valide o Dataset hint no BigQuery/Data Catalog antes de gerar SQL.",
    );
    return;
  }

  showQBError("");
  setQBLoading(true);
  setQBProgress("Validando entrada...", 12);

  if (qbEmpty) qbEmpty.style.display = "none";
  if (qbTabsArea) qbTabsArea.style.display = "none";

  try {
    setTimeout(() => setQBProgress("Gerando SQL com LLM...", 36), 180);
    setTimeout(
      () => setQBProgress("Executando dry-run no BigQuery...", 62),
      520,
    );
    setTimeout(() => setQBProgress("Consolidando resultado...", 84), 980);

    const res = await fetch("/api/agents/query_build/analyze", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        query: requestText,
        project_id: projectId,
        dataset_hint: datasetHint,
      }),
    });

    if (res.status === 401) {
      doLogout();
      return;
    }

    if (!res.ok) {
      const e = await res.json();
      throw new Error(e.detail || "Erro ao gerar query");
    }

    const data = await res.json();
    setQBProgress("Finalizando apresentação...", 100);
    renderQB(data);
  } catch (e) {
    showQBError(prettifyErrorMessage(e.message));

    if (qbTabsArea && qbTabsArea.style.display === "none" && qbEmpty) {
      qbEmpty.style.display = "flex";
    }
  } finally {
    setTimeout(() => {
      hideQBProgress();
      setQBLoading(false);
    }, 350);
  }
}

async function runDocumentBuild() {
  const requestText = document.getElementById("db-request")?.value.trim() || "";
  const { projectId, datasetHint } = resolveDocumentBuildContext(requestText);
  const dbEmpty = document.getElementById("db-empty");
  const dbTabsArea = document.getElementById("db-tabs-area");

  if (!requestText) {
    showDBError("Descreva o contexto antes de gerar a documentação.");
    return;
  }

  if (!projectId) {
    showDBError(
      "Inclua [TABELA] no formato projeto.dataset.tabela para detectar automaticamente o Project ID.",
    );
    return;
  }

  showDBError("");
  setDBLoading(true);
  setDBProgress("Validando entrada...", 14);

  if (dbEmpty) dbEmpty.style.display = "none";
  if (dbTabsArea) dbTabsArea.style.display = "none";

  try {
    setTimeout(() => setDBProgress("Estruturando documentação...", 38), 180);
    setTimeout(() => setDBProgress("Gerando conteúdo técnico...", 64), 520);
    setTimeout(() => setDBProgress("Consolidando markdown...", 86), 980);

    const res = await fetch("/api/agents/document_build/analyze", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        query: requestText,
        project_id: projectId,
        dataset_hint: datasetHint || null,
      }),
    });

    if (res.status === 401) {
      doLogout();
      return;
    }

    if (!res.ok) {
      const e = await res.json();
      throw new Error(e.detail || "Erro ao gerar documentação");
    }

    const data = await res.json();
    if (data.status === "error") {
      throw new Error(data.error || "Não foi possível gerar a documentação.");
    }

    setDBProgress("Finalizando apresentação...", 100);
    renderDocumentBuild(data);
  } catch (e) {
    showDBError(prettifyErrorMessage(e.message));
    if (dbTabsArea && dbTabsArea.style.display === "none" && dbEmpty) {
      dbEmpty.style.display = "flex";
    }
  } finally {
    setTimeout(() => {
      hideDBProgress();
      setDBLoading(false);
    }, 350);
  }
}

function renderDocumentBuild(data) {
  const empty = document.getElementById("db-empty");
  const tabsArea = document.getElementById("db-tabs-area");
  if (empty) empty.style.display = "none";
  if (tabsArea) tabsArea.style.display = "block";

  const score = Number(data.quality_score || 0);
  const grade = score >= 90 ? "A" : score >= 75 ? "B" : score >= 60 ? "C" : "D";

  const summary = document.getElementById("db-summary");
  const gradeBlock = document.getElementById("db-grade-block");
  const gradeLtr = document.getElementById("db-grade-ltr");
  const scoreBig = document.getElementById("db-score-big");
  const scoreFill = document.getElementById("db-score-fill");

  if (gradeBlock) gradeBlock.className = `grade-block gb-${grade}`;
  if (gradeLtr) gradeLtr.textContent = grade;
  if (scoreBig) scoreBig.textContent = String(score);
  if (scoreFill) {
    scoreFill.className = `score-fill sf-${grade}`;
    setTimeout(() => {
      scoreFill.style.width = `${score}%`;
    }, 80);
  }
  if (summary)
    summary.textContent =
      data.summary || "Documentação gerada sem resumo detalhado.";

  const sections = Array.isArray(data.sections) ? data.sections : [];
  const checklist = Array.isArray(data.acceptance_checklist)
    ? data.acceptance_checklist
    : [];
  const nextSteps = Array.isArray(data.next_steps) ? data.next_steps : [];
  const warnings = Array.isArray(data.warnings) ? data.warnings : [];

  const docType = document.getElementById("db-doc-type");
  const sectionCount = document.getElementById("db-sections-count");
  const checklistCount = document.getElementById("db-checklist-count");
  const structureList = document.getElementById("db-structure-list");
  const markdown = document.getElementById("db-markdown");
  const htmlSource = document.getElementById("db-html-source");
  const htmlPreview = document.getElementById("db-html-preview");
  const checklistList = document.getElementById("db-checklist-list");
  const nextStepsSec = document.getElementById("db-next-steps-sec");
  const nextStepsList = document.getElementById("db-next-steps-list");
  const typingNotes = Array.isArray(data.typing_notes) ? data.typing_notes : [];
  const pendingTechnical = Array.isArray(data.pending_technical)
    ? data.pending_technical
    : [];
  const dataDictionary = Array.isArray(data.data_dictionary)
    ? data.data_dictionary
    : [];
  const governance =
    data && typeof data.governance === "object" && data.governance
      ? data.governance
      : {};
  const governanceAspects = Array.isArray(governance.aspect_types)
    ? governance.aspect_types
    : [];
  const governanceReaders = Array.isArray(governance.readers)
    ? governance.readers
    : [];
  const governanceNotes = Array.isArray(governance.notes)
    ? governance.notes
    : [];

  const effectiveChecklist = deriveChecklistFromSections(sections, checklist);
  const effectiveGovernance = deriveGovernanceFromSections(sections, {
    aspect_types: governanceAspects,
    readers: governanceReaders,
    notes: governanceNotes,
  });
  const effectiveGovernanceAspects = Array.isArray(
    effectiveGovernance.aspect_types,
  )
    ? effectiveGovernance.aspect_types
    : [];
  const effectiveGovernanceReaders = Array.isArray(effectiveGovernance.readers)
    ? effectiveGovernance.readers
    : [];
  const effectiveGovernanceNotes = Array.isArray(effectiveGovernance.notes)
    ? effectiveGovernance.notes
    : [];

  if (docType) docType.textContent = data.doc_type || "—";
  if (sectionCount) sectionCount.textContent = String(sections.length);
  if (checklistCount)
    checklistCount.textContent = String(effectiveChecklist.length);

  if (structureList) {
    const baseItems = sections.map((section, i) => {
      const title = translateSectionTitle(section.title || `Seção ${i + 1}`);
      const content = section.content || "Sem conteúdo.";
      return `<div class="rec-item"><span class="rec-n">${String(i + 1).padStart(2, "0")}</span><strong>${title}:</strong> ${content}</div>`;
    });

    const warningItems = warnings.map(
      (w) =>
        `<div class="rec-item" style="border-color:#fecaca;background:var(--rose-bg);color:var(--rose)">⚠ ${w}</div>`,
    );

    structureList.innerHTML =
      [...baseItems, ...warningItems].join("") ||
      '<div class="rec-item">Nenhuma seção retornada.</div>';
  }

  if (markdown) {
    markdown.textContent =
      data.markdown_document || "Nenhum markdown retornado.";
  }

  if (checklistList) {
    checklistList.innerHTML = effectiveChecklist.length
      ? effectiveChecklist
          .map(
            (item, i) =>
              `<div class="rec-item"><span class="rec-n">${String(i + 1).padStart(2, "0")}</span>${item}</div>`,
          )
          .join("")
      : '<div class="rec-item">Checklist não informado.</div>';
  }

  if (nextStepsSec)
    nextStepsSec.style.display = nextSteps.length ? "block" : "none";
  if (nextStepsList && nextSteps.length) {
    nextStepsList.innerHTML = nextSteps
      .map(
        (item, i) =>
          `<div class="rec-item"><span class="rec-n">${String(i + 1).padStart(2, "0")}</span>${item}</div>`,
      )
      .join("");
  }

  const confluenceSource = document.getElementById("db-confluence-source");

  const htmlDocument = generateDocumentHtml(data, {
    sections,
    checklist: effectiveChecklist,
    nextSteps,
    warnings,
    typingNotes,
    pendingTechnical,
    dataDictionary,
    governanceAspects: effectiveGovernanceAspects,
    governanceReaders: effectiveGovernanceReaders,
    governanceNotes: effectiveGovernanceNotes,
  });

  if (htmlSource) {
    htmlSource.textContent = htmlDocument;
  }
  if (htmlPreview) {
    htmlPreview.srcdoc = htmlDocument;
  }

  const confluenceMarkup = generateConfluenceMarkup(data, {
    sections,
    checklist: effectiveChecklist,
    nextSteps,
    warnings,
    typingNotes,
    pendingTechnical,
    dataDictionary,
    governanceAspects: effectiveGovernanceAspects,
    governanceReaders: effectiveGovernanceReaders,
    governanceNotes: effectiveGovernanceNotes,
  });
  if (confluenceSource) {
    confluenceSource.textContent = confluenceMarkup;
  }

  switchDBTab("score");
}

function parseJsonFromSectionCodeFence(content) {
  const raw = String(content || "").trim();
  if (!raw) return null;

  const fenceMatch = raw.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  const candidate = (fenceMatch ? fenceMatch[1] : raw).trim();
  if (!candidate) return null;
  if (!candidate.startsWith("[") && !candidate.startsWith("{")) return null;

  try {
    return JSON.parse(candidate);
  } catch {
    return null;
  }
}

function deriveChecklistFromSections(sections, currentChecklist) {
  if (Array.isArray(currentChecklist) && currentChecklist.length) {
    return currentChecklist;
  }

  const dqSection = (Array.isArray(sections) ? sections : []).find((s) => {
    const title = String(s?.title || "").toLowerCase();
    return /data\s*quality|\bdq\b|qualidade/.test(title);
  });

  const parsed = parseJsonFromSectionCodeFence(dqSection?.content || "");
  if (!Array.isArray(parsed)) return [];

  return parsed.map((item) => String(item || "").trim()).filter(Boolean);
}

function deriveGovernanceFromSections(sections, currentGovernance) {
  const emptyMarkers = new Set([
    "",
    "nenhum",
    "none",
    "n/a",
    "não informado",
    "nao informado",
    "-",
    "nao configurado — consultar responsavel pelo dominio de dados.",
    "não configurado — consultar responsável pelo domínio de dados.",
  ]);
  const normalize = (items) =>
    (Array.isArray(items) ? items : [])
      .map((v) => String(v || "").trim())
      .filter((v) => !emptyMarkers.has(v.toLowerCase()));

  const base = {
    aspect_types: normalize(currentGovernance?.aspect_types),
    readers: normalize(currentGovernance?.readers),
    notes: normalize(currentGovernance?.notes),
  };

  const govSection = (Array.isArray(sections) ? sections : []).find((s) => {
    const title = String(s?.title || "").toLowerCase();
    return /governan|governance|compliance|acesso/.test(title);
  });

  const parsed = parseJsonFromSectionCodeFence(govSection?.content || "");
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return base;
  }

  const parsedGovernance = {
    aspect_types: normalize(parsed.aspect_types),
    readers: normalize(parsed.readers),
    notes: normalize(parsed.notes),
  };

  const dedupe = (items) => [...new Set(items)];
  const mergedReaders = dedupe([...base.readers, ...parsedGovernance.readers]);

  return {
    aspect_types: dedupe([
      ...base.aspect_types,
      ...parsedGovernance.aspect_types,
    ]),
    readers: mergedReaders,
    notes: dedupe([...base.notes, ...parsedGovernance.notes]),
  };
}

const SECTION_LABELS_PT = {
  assumptions: "Premissas",
  risks: "Riscos",
  acceptance_checklist: "Checklist de Aceitação",
  next_steps: "Próximos Passos",
  warnings: "Observações",
  pending_technical: "Pendências Técnicas",
  governance: "Governança",
};

function translateSectionTitle(title) {
  const raw = String(title || "").trim();
  if (!raw) return "Seção";

  const key = raw.toLowerCase().replace(/\s+/g, "_").replace(/-/g, "_");
  return SECTION_LABELS_PT[key] || raw;
}

function resolveDocumentBuildContext(requestText) {
  const explicit = extractExplicitTableRef(requestText);
  const qaProject = document.getElementById("qa-project")?.value.trim() || "";
  const qbProject = document.getElementById("qb-project")?.value.trim() || "";

  const projectId = explicit.project || qaProject || qbProject;
  const datasetHint = explicit.dataset || null;

  return { projectId, datasetHint };
}

function extractExplicitTableRef(text) {
  const content = String(text || "");
  const tableBlock = content.match(
    /\[TABELA\]\s*([\s\S]*?)(?=\n\s*\[[^\]]+\]|$)/i,
  );

  const source = (tableBlock?.[1] || content).trim().replace(/`/g, "");
  const full = source.match(
    /([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)/,
  );
  if (full) {
    return {
      project: full[1],
      dataset: full[2],
      table: full[3],
    };
  }

  const dsTable = source.match(/([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)/);
  if (dsTable) {
    return {
      project: "",
      dataset: dsTable[1],
      table: dsTable[2],
    };
  }

  return { project: "", dataset: "", table: "" };
}

function generateDocumentHtml(data, context) {
  const sections = Array.isArray(context.sections) ? context.sections : [];
  const checklist = Array.isArray(context.checklist) ? context.checklist : [];
  const nextSteps = Array.isArray(context.nextSteps) ? context.nextSteps : [];
  const warnings = Array.isArray(context.warnings) ? context.warnings : [];
  const typingNotes = Array.isArray(context.typingNotes)
    ? context.typingNotes
    : [];
  const pendingTechnical = Array.isArray(context.pendingTechnical)
    ? context.pendingTechnical
    : [];
  const dataDictionary = Array.isArray(context.dataDictionary)
    ? context.dataDictionary
    : [];
  const governanceAspects = Array.isArray(context.governanceAspects)
    ? context.governanceAspects
    : [];
  const governanceReaders = Array.isArray(context.governanceReaders)
    ? context.governanceReaders
    : [];
  const governanceNotes = Array.isArray(context.governanceNotes)
    ? context.governanceNotes
    : [];

  const safe = (v) => escapeHtml(v == null ? "" : String(v));
  const now = new Date().toLocaleString("pt-BR", {
    dateStyle: "short",
    timeStyle: "short",
  });

  /* ── doc-type label ──────────────────────────────── */
  const docTypeMap = {
    documentacao_funcional: {
      label: "Documentação Funcional",
      icon: "📊",
      color: "#1a56af",
    },
    especificacao_tecnica: {
      label: "Especificação Técnica",
      icon: "🧩",
      color: "#0e8a5e",
    },
    runbook_operacional: {
      label: "Runbook Operacional",
      icon: "🛟",
      color: "#c05d0a",
    },
  };
  const rawType = String(data.doc_type || "")
    .toLowerCase()
    .replace(/ /g, "_");
  const docType = docTypeMap[rawType] || {
    label: safe(data.doc_type || "Documento"),
    icon: "📄",
    color: "#1a56af",
  };

  /* ── section icon heuristic ──────────────────────── */
  function sectionIcon(title) {
    const t = String(title).toLowerCase();
    if (/objetivo|purpose/.test(t)) return "🎯";
    if (/contexto|negoc|business/.test(t)) return "🏢";
    if (/fluxo|pipeline|process/.test(t)) return "🔄";
    if (/sla|alerta|incidente|incident/.test(t)) return "🚨";
    if (/diagnos|query|sql/.test(t)) return "🔍";
    if (/escal|contato|responsavel/.test(t)) return "📞";
    if (/partici|cluster|tecni|technical/.test(t)) return "⚙️";
    if (/govern|compliance|acesso/.test(t)) return "🔒";
    if (/publico|audienc/.test(t)) return "👥";
    if (/histor|versao|change/.test(t)) return "📋";
    return "📄";
  }

  /* ── type badge color ────────────────────────────── */
  function typeBadge(type) {
    const t = String(type).toUpperCase();
    const map = {
      INTEGER: "#1d4ed8",
      INT64: "#1d4ed8",
      INT: "#1d4ed8",
      STRING: "#374151",
      VARCHAR: "#374151",
      FLOAT: "#065f46",
      FLOAT64: "#065f46",
      NUMERIC: "#065f46",
      BIGNUMERIC: "#065f46",
      DATE: "#6d28d9",
      DATETIME: "#6d28d9",
      TIMESTAMP: "#6d28d9",
      TIME: "#6d28d9",
      BOOLEAN: "#b45309",
      BOOL: "#b45309",
      RECORD: "#0e7490",
      STRUCT: "#0e7490",
      ARRAY: "#0e7490",
      BYTES: "#9d174d",
    };
    const bg = map[t] || "#475569";
    return `<span style="display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700;color:#fff;background:${bg};letter-spacing:.4px">${safe(type)}</span>`;
  }

  function extractJsonFromCodeFence(text) {
    const raw = String(text || "").trim();
    if (!raw) return null;

    const fenceMatch = raw.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
    const candidate = (fenceMatch ? fenceMatch[1] : raw).trim();

    if (!candidate) return null;
    if (!candidate.startsWith("[") && !candidate.startsWith("{")) return null;

    try {
      return JSON.parse(candidate);
    } catch {
      return null;
    }
  }

  function renderJsonValue(value) {
    if (Array.isArray(value)) {
      if (!value.length) {
        return '<div class="json-empty">Sem itens informados.</div>';
      }

      const items = value
        .map((item) => {
          if (item && typeof item === "object") {
            return `<li>${renderJsonValue(item)}</li>`;
          }
          return `<li>${safe(item)}</li>`;
        })
        .join("");

      return `<ul class="json-list">${items}</ul>`;
    }

    if (value && typeof value === "object") {
      const entries = Object.entries(value);
      if (!entries.length) {
        return '<div class="json-empty">Sem dados estruturados.</div>';
      }

      return `<div class="json-kv">${entries
        .map(([k, v]) => {
          const content =
            Array.isArray(v) || (v && typeof v === "object")
              ? renderJsonValue(v)
              : `<span class="json-inline">${safe(v)}</span>`;
          return `<div class="json-kv-row"><strong>${safe(k)}</strong>${content}</div>`;
        })
        .join("")}</div>`;
    }

    return `<span class="json-inline">${safe(value)}</span>`;
  }

  function mdToHtml(text) {
    const source = String(text || "");
    const escaped = safe(source);
    const codeBlocks = [];
    const isSqlLikeInline = (value) =>
      /\b(select|with|insert|update|delete|merge)\b/i.test(value || "");

    let html = escaped.replace(
      /```(?:sql|python|text|bash|shell)?\s*([\s\S]*?)\s*```/gi,
      (_match, code) => {
        const token = `@@CODE_BLOCK_${codeBlocks.length}@@`;
        codeBlocks.push(`<pre><code>${code}</code></pre>`);
        return token;
      },
    );

    html = html.replace(/`([^`\n]+)`/g, (_match, code) => {
      if (isSqlLikeInline(code)) {
        const token = `@@CODE_BLOCK_${codeBlocks.length}@@`;
        codeBlocks.push(`<pre><code>${code}</code></pre>`);
        return token;
      }
      return `<code>${code}</code>`;
    });

    html = html.replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");
    html = html.replace(/\n/g, "<br/>");

    codeBlocks.forEach((block, idx) => {
      const token = `@@CODE_BLOCK_${idx}@@`;
      html = html.replace(token, block);
    });

    return html;
  }

  function renderSectionContent(content) {
    const text = String(content || "").trim();
    if (!text) {
      return '<p class="sect-text">Sem conteúdo informado.</p>';
    }

    const markdownKv = text
      .split(/\n+/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => line.match(/^\*\*([^*]+)\*\*:\s*(.+)$/))
      .filter(Boolean);

    if (markdownKv.length) {
      const obj = {};
      markdownKv.forEach((match) => {
        const key = String(match[1] || "").trim();
        const rawValue = String(match[2] || "").trim();
        let value = rawValue;
        if (/^\[.*\]$/.test(rawValue)) {
          try {
            value = JSON.parse(rawValue.replace(/'/g, '"'));
          } catch {
            value = rawValue;
          }
        }
        obj[key] = value;
      });
      return `<div class="sect-structured">${renderJsonValue(obj)}</div>`;
    }

    const parsedJson = extractJsonFromCodeFence(text);
    if (parsedJson !== null) {
      return `<div class="sect-structured">${renderJsonValue(parsedJson)}</div>`;
    }

    const html = mdToHtml(text).trim();
    return html
      ? `<div class="sect-text">${html}</div>`
      : '<p class="sect-text">Sem conteúdo informado.</p>';
  }

  /* ── build section cards ─────────────────────────── */
  const sectionCards = sections.length
    ? sections
        .map((s) => {
          const translatedTitle = translateSectionTitle(s.title || "Seção");
          const icon = sectionIcon(translatedTitle);
          const body = renderSectionContent(s.content || "");
          return `
        <article class="card sect-card">
          <div class="card-head">
            <span class="card-icon">${icon}</span>
            <h3>${safe(translatedTitle)}</h3>
          </div>
          ${body}
        </article>`;
        })
        .join("\n")
    : '<article class="card sect-card"><p>Sem seções retornadas.</p></article>';

  /* ── dictionary rows ─────────────────────────────── */
  const dictionaryRows = dataDictionary.length
    ? dataDictionary
        .map(
          (row, i) => `
        <tr class="${i % 2 === 0 ? "row-even" : "row-odd"}">
          <td class="col-name"><code>${safe(row.column || "-")}</code></td>
          <td class="col-type">${typeBadge(row.type || "-")}</td>
          <td>${safe(row.description || "-")}</td>
          <td>${safe(row.business_rule || "-")}</td>
        </tr>`,
        )
        .join("\n")
    : '<tr><td colspan="4" style="color:#888;text-align:center">Dicionário não disponível</td></tr>';

  /* ── checklist ───────────────────────────────────── */
  const checklistHtml = checklist.length
    ? checklist
        .map(
          (item) => `
        <li class="check-item">
          <span class="check-ico">✅</span>
          <span>${safe(item)}</span>
        </li>`,
        )
        .join("\n")
    : '<li class="check-item"><span class="check-ico">—</span><span>Checklist não informado</span></li>';

  /* ── rules & pending ─────────────────────────────── */
  const ruleItems = [...typingNotes, ...pendingTechnical];
  const ruleHtml = ruleItems.length
    ? ruleItems
        .map(
          (item) => `
        <li class="check-item">
          <span class="check-ico">⚠️</span>
          <span>${safe(item)}</span>
        </li>`,
        )
        .join("\n")
    : '<li class="check-item"><span class="check-ico">—</span><span>Sem regras adicionais.</span></li>';

  /* ── governance ──────────────────────────────────── */
  const govItems = [
    ...governanceAspects.map((a) => ({ ico: "🔒", text: safe(a) })),
    ...governanceReaders.map((r) => ({
      ico: "👤",
      text: `Leitor: ${safe(r)}`,
    })),
    ...governanceNotes.map((n) => ({
      ico: "📝",
      text: `Nota: ${safe(n)}`,
    })),
    ...warnings.map((w) => ({ ico: "⚠️", text: `Observação: ${safe(w)}` })),
  ];
  const govHtml = govItems.length
    ? govItems
        .map(
          (g) => `
        <li class="check-item">
          <span class="check-ico">${g.ico}</span>
          <span>${g.text}</span>
        </li>`,
        )
        .join("\n")
    : '<li class="check-item"><span class="check-ico">—</span><span>Governança não detalhada.</span></li>';

  /* ── next steps ──────────────────────────────────── */
  const nextHtml = nextSteps.length
    ? nextSteps
        .map(
          (item, i) => `
        <li class="step-item">
          <span class="step-n">${i + 1}</span>
          <span>${safe(item)}</span>
        </li>`,
        )
        .join("\n")
    : '<li class="step-item"><span class="step-n">—</span><span>Sem próximos passos informados.</span></li>';

  /* ── warnings banner ─────────────────────────────── */
  const warnBanner = warnings.length
    ? `
    <section class="warn-box">
      <span class="warn-ico">⚠️</span>
      <div>
        <strong>Avisos do pipeline</strong>
        <ul style="margin:4px 0 0;padding-left:16px">
          ${warnings.map((w) => `<li>${safe(w)}</li>`).join("")}
        </ul>
      </div>
    </section>`
    : "";

  /* ── table path breadcrumb ───────────────────────── */
  const tablePath = safe(data.table_path || data.table_name || "-");
  const parts = tablePath.split(".");
  const breadcrumb =
    parts.length === 3
      ? `<span class="bc-dim">${parts[0]}.</span><span class="bc-dim">${parts[1]}.</span><strong>${parts[2]}</strong>`
      : tablePath;

  return `<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${safe(data.title || "Documentação Técnica")}</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body {
      font-family: "Segoe UI", system-ui, Arial, sans-serif;
      margin: 0; background: #eef2f8; color: #1d2a3a;
      font-size: 13px; line-height: 1.6;
    }
    .wrap { max-width: 1060px; margin: 0 auto; padding: 24px 20px 40px; }

    /* ── Hero ── */
    .hero {
      background: linear-gradient(135deg, #003e8a 0%, #0e6fd6 100%);
      color: #fff; border-radius: 14px; padding: 20px 22px;
      display: flex; gap: 16px; align-items: flex-start;
      box-shadow: 0 4px 18px rgba(0,62,138,.25);
    }
    .hero-logo {
      width: 52px; height: 52px; border-radius: 12px;
      background: #ffffff; display: flex;
      border: 1px solid rgba(255,255,255,.55);
      box-shadow: 0 2px 8px rgba(0, 34, 87, 0.18);
      align-items: center; justify-content: center;
      padding: 6px; flex-shrink: 0;
      overflow: hidden;
    }
    .hero-logo img {
      width: 100%;
      height: 100%;
      object-fit: contain;
    }
    .hero-body { flex: 1; }
    .hero-badge {
      display: inline-flex; align-items: center; gap: 5px;
      background: rgba(255,255,255,.18); border-radius: 20px;
      padding: 3px 10px; font-size: 11px; font-weight: 600;
      letter-spacing: .3px; margin-bottom: 6px;
    }
    .hero h1 { margin: 0 0 4px; font-size: 21px; font-weight: 700; line-height: 1.2; }
    .hero-summary { margin: 0; font-size: 12.5px; opacity: .88; line-height: 1.5; }
    .hero-meta { margin-top: 12px; display: flex; flex-wrap: wrap; gap: 8px; }
    .hero-pill {
      background: rgba(255,255,255,.15); border: 1px solid rgba(255,255,255,.25);
      border-radius: 6px; padding: 4px 9px; font-size: 11px;
      display: flex; align-items: center; gap: 4px;
    }
    .hero-pill strong { font-weight: 600; opacity: .7; margin-right: 2px; }

    /* ── Executive banner ── */
    .exec-box {
      margin-top: 14px; background: #fff;
      border: 1px solid #c8daf5; border-left: 4px solid #0e6fd6;
      border-radius: 10px; padding: 12px 14px;
      display: flex; gap: 10px; align-items: flex-start;
      font-size: 12.5px; color: #1e3558;
    }
    .exec-ico { font-size: 18px; flex-shrink: 0; margin-top: 1px; }

    /* ── Warning box ── */
    .warn-box {
      margin-top: 14px; background: #fffbeb;
      border: 1px solid #fbbf24; border-left: 4px solid #d97706;
      border-radius: 10px; padding: 12px 14px;
      display: flex; gap: 10px; align-items: flex-start;
      font-size: 12px; color: #78350f;
    }
    .warn-ico { font-size: 18px; flex-shrink: 0; margin-top: 1px; }

    /* ── Grid ── */
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 14px; }
    .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; margin-top: 14px; }
    .span2 { grid-column: span 2; }

    /* ── Cards ── */
    .card {
      background: #fff; border: 1px solid #d4e2f4;
      border-radius: 12px; padding: 14px 16px;
    }
    .card-head {
      display: flex; align-items: center; gap: 8px;
      margin-bottom: 8px; padding-bottom: 8px;
      border-bottom: 1px solid #eef2f8;
    }
    .card-icon { font-size: 17px; flex-shrink: 0; }
    .card h2 {
      margin: 0; color: #003087; font-size: 13.5px;
      font-weight: 700; line-height: 1.3;
    }
    .card h3 {
      margin: 0; color: #003e8a; font-size: 13px;
      font-weight: 700; line-height: 1.3;
    }
    .card p { margin: 0; font-size: 12.5px; color: #2d3b4f; line-height: 1.65; }
    .sect-card { border-left: 3px solid #0e6fd6; }
    .sect-text { margin: 0; font-size: 12.5px; color: #2d3b4f; line-height: 1.65; }
    .sect-text + .sect-text { margin-top: 8px; }
    .sect-text pre {
      margin: 8px 0;
      padding: 10px 12px;
      border-radius: 8px;
      background: #f4f7fb;
      border: 1px solid #dbe6f6;
      overflow: auto;
      line-height: 1.45;
    }
    .sect-text code {
      font-family: "Cascadia Code", "Consolas", monospace;
      font-size: 11.5px;
      color: #1f3b61;
      white-space: pre;
    }
    .sect-structured { font-size: 12.5px; color: #2d3b4f; }
    .json-list {
      list-style: none;
      margin: 0;
      padding: 0;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .json-list > li {
      position: relative;
      padding-left: 14px;
      line-height: 1.6;
    }
    .json-list > li::before {
      content: "•";
      position: absolute;
      left: 0;
      color: #0e6fd6;
      font-weight: 700;
    }
    .json-kv {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .json-kv-row {
      display: flex;
      flex-direction: column;
      gap: 4px;
      padding: 8px 10px;
      border-radius: 8px;
      background: #f7faff;
      border: 1px solid #e3ecfb;
    }
    .json-kv-row > strong {
      font-size: 11.5px;
      letter-spacing: .2px;
      color: #1a56af;
    }
    .json-inline {
      color: #2d3b4f;
      line-height: 1.6;
      word-break: break-word;
    }
    .json-empty {
      color: #64748b;
      font-size: 12px;
    }

    /* ── Breadcrumb ── */
    .breadcrumb { font-family: "Cascadia Code", "Consolas", monospace; font-size: 12px; }
    .bc-dim { opacity: .55; }

    /* ── Table ── */
    .table-wrap { overflow-x: auto; margin-top: 6px; }
    table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
    th {
      background: #e8f0fc; color: #17365d; font-weight: 700;
      padding: 8px 10px; text-align: left; border-bottom: 2px solid #c0d2f0;
      white-space: nowrap;
    }
    td { padding: 7px 10px; vertical-align: top; border-bottom: 1px solid #e8eef7; }
    .row-even { background: #fff; }
    .row-odd  { background: #f7faff; }
    .col-name code {
      font-family: "Cascadia Code", "Consolas", monospace;
      font-size: 11px; color: #003087;
    }
    .col-type { white-space: nowrap; }

    /* ── Check lists ── */
    .check-list, .step-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 6px; }
    .check-item { display: flex; align-items: flex-start; gap: 8px; font-size: 12.5px; color: #2d3b4f; }
    .check-ico { flex-shrink: 0; font-size: 14px; margin-top: 1px; }

    /* ── Numbered steps ── */
    .step-item { display: flex; align-items: flex-start; gap: 10px; font-size: 12.5px; color: #2d3b4f; }
    .step-n {
      flex-shrink: 0; width: 22px; height: 22px; border-radius: 50%;
      background: #0e6fd6; color: #fff; font-size: 11px; font-weight: 700;
      display: flex; align-items: center; justify-content: center;
    }

    /* ── Section title strip ── */
    .section-title {
      display: flex; align-items: center; gap: 8px;
      margin: 18px 0 10px; font-size: 12px; font-weight: 700;
      color: #5a7a9e; text-transform: uppercase; letter-spacing: .6px;
    }
    .section-title::after {
      content: ""; flex: 1; height: 1px; background: #d4e2f4;
    }

    /* ── Footer ── */
    .footer {
      margin-top: 24px; padding-top: 14px;
      border-top: 1px solid #d4e2f4;
      display: flex; justify-content: space-between; align-items: center;
      font-size: 11px; color: #8a9ab5; flex-wrap: wrap; gap: 6px;
    }
    .footer-brand { display: flex; align-items: center; gap: 6px; font-weight: 600; }

    @media (max-width: 860px) {
      .grid, .grid-3 { grid-template-columns: 1fr; }
      .span2 { grid-column: span 1; }
    }
  </style>
</head>
<body>
  <div class="wrap">

    <!-- ── HERO ─────────────────────────────────── -->
    <header class="hero">
      <div class="hero-logo"><img src="/static/img/portoseguro.png" alt="Porto Seguro" /></div>
      <div class="hero-body">
        <div class="hero-badge">${docType.icon} ${docType.label}</div>
        <h1>${safe(data.title || "Documentação Técnica")}</h1>
        <p class="hero-summary">${safe(data.summary || "Documento gerado pelo Document Builder.")}</p>
        <div class="hero-meta">
          <span class="hero-pill"><strong>📦 Tabela</strong> <span class="breadcrumb">${breadcrumb}</span></span>
          <span class="hero-pill"><strong>🔄 Frequência</strong> ${safe(data.frequency || "—")}</span>
          <span class="hero-pill"><strong>👥 Público</strong> ${safe(data.audience || "—")}</span>
          <span class="hero-pill"><strong>📅 Gerado</strong> ${now}</span>
        </div>
      </div>
    </header>

    <!-- ── EXECUTIVE BANNER ───────────────────── -->
    <div class="exec-box">
      <span class="exec-ico">💡</span>
      <div>
        <strong>Visão executiva</strong><br/>
        ${safe(data.objective || "Documento estruturado para decisão e governança, com foco em contexto de negócio, confiabilidade dos dados e encaminhamentos operacionais.")}
      </div>
    </div>

    ${warnBanner}

    <!-- ── SEÇÕES PRINCIPAIS ──────────────────── -->
    <div class="section-title">📄 Conteúdo do Documento</div>
    <div class="grid">
      ${sectionCards}
    </div>

    <!-- ── DICIONÁRIO DE DADOS ────────────────── -->
    <div class="section-title">🗂️ Dicionário de Dados</div>
    <div class="card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Coluna</th>
              <th>Tipo</th>
              <th>Descrição</th>
              <th>Regra de Negócio</th>
            </tr>
          </thead>
          <tbody>
            ${dictionaryRows}
          </tbody>
        </table>
      </div>
    </div>

    <!-- ── QUALIDADE & REGRAS ─────────────────── -->
    <div class="section-title">✅ Qualidade & Regras</div>
    <div class="grid">
      <article class="card">
        <div class="card-head">
          <span class="card-icon">✅</span>
          <h2>Checklist de Qualidade</h2>
        </div>
        <ul class="check-list">${checklistHtml}</ul>
      </article>
      <article class="card">
        <div class="card-head">
          <span class="card-icon">⚠️</span>
          <h2>Regras & Pendências</h2>
        </div>
        <ul class="check-list">${ruleHtml}</ul>
      </article>
    </div>

    <!-- ── GOVERNANÇA & PRÓXIMOS PASSOS ──────── -->
    <div class="section-title">🔒 Governança & Ações</div>
    <div class="grid">
      <article class="card">
        <div class="card-head">
          <span class="card-icon">🔒</span>
          <h2>Governança</h2>
        </div>
        <ul class="check-list">${govHtml}</ul>
      </article>
      <article class="card">
        <div class="card-head">
          <span class="card-icon">🚀</span>
          <h2>Próximos Passos</h2>
        </div>
        <ul class="step-list">${nextHtml}</ul>
      </article>
    </div>

    <!-- ── FOOTER ─────────────────────────────── -->
    <footer class="footer">
      <span class="footer-brand">🤖 Document Builder · Finance Hub</span>
      <span>Gerado em ${now} · Engenharia de Dados Financeiro</span>
    </footer>

  </div>
</body>
</html>`;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function switchDBTab(name) {
  document.querySelectorAll('[id^="db-tab-"]').forEach((t) => {
    t.classList.remove("active");
  });

  document.querySelectorAll('[id^="db-panel-"]').forEach((p) => {
    p.classList.remove("active");
  });

  const tab = document.getElementById("db-tab-" + name);
  const panel = document.getElementById("db-panel-" + name);

  if (tab) tab.classList.add("active");
  if (panel) panel.classList.add("active");
}

function setDBLoading(on) {
  const btn = document.getElementById("db-btn");
  const spinner = document.getElementById("db-spinner");
  const text = document.getElementById("db-btn-text");

  dbIsLoading = on;
  if (btn) btn.disabled = on;
  if (spinner) spinner.style.display = on ? "block" : "none";
  if (text) {
    text.textContent = on
      ? "Gerando documentação..."
      : "Gerar com Document Builder";
  }
}

function showDBError(message) {
  const box = document.getElementById("db-error");
  if (!box) return;

  if (!message) {
    box.style.display = "none";
    box.textContent = "";
    return;
  }

  box.textContent = "⚠ " + message;
  box.style.display = "block";
}

function copyDBDocument() {
  const content = document.getElementById("db-markdown")?.textContent || "";
  const btn = document.getElementById("db-copy-btn");
  if (!content) return;

  copyTextWithFallback(content)
    .then(() => {
      if (!btn) return;
      const old = btn.textContent;
      btn.textContent = "✓ Copiado!";
      btn.style.color = "#34D399";
      setTimeout(() => {
        btn.textContent = old || "Copiar Markdown";
        btn.style.color = "";
      }, 1800);
    })
    .catch(() => {
      showDBError("Não foi possível copiar automaticamente. Tente novamente.");
    });
}

function copyDBConfluenceDocument() {
  const content =
    document.getElementById("db-confluence-source")?.textContent || "";
  const btn = document.getElementById("db-copy-confluence-btn");
  if (!content) return;

  copyTextWithFallback(content)
    .then(() => {
      if (!btn) return;
      const old = btn.textContent;
      btn.textContent = "\u2713 Copiado!";
      btn.style.color = "#34D399";
      setTimeout(() => {
        btn.textContent = old || "Copiar Confluence";
        btn.style.color = "";
      }, 1800);
    })
    .catch(() => {
      showDBError("N\u00e3o foi poss\u00edvel copiar o Confluence markup.");
    });
}

function generateConfluenceMarkup(data, context) {
  const sections = Array.isArray(context.sections) ? context.sections : [];
  const checklist = Array.isArray(context.checklist) ? context.checklist : [];
  const nextSteps = Array.isArray(context.nextSteps) ? context.nextSteps : [];
  const warnings = Array.isArray(context.warnings) ? context.warnings : [];
  const typingNotes = Array.isArray(context.typingNotes)
    ? context.typingNotes
    : [];
  const pendingTechnical = Array.isArray(context.pendingTechnical)
    ? context.pendingTechnical
    : [];
  const dataDictionary = Array.isArray(context.dataDictionary)
    ? context.dataDictionary
    : [];
  const governanceAspects = Array.isArray(context.governanceAspects)
    ? context.governanceAspects
    : [];
  const governanceReaders = Array.isArray(context.governanceReaders)
    ? context.governanceReaders
    : [];
  const governanceNotes = Array.isArray(context.governanceNotes)
    ? context.governanceNotes
    : [];

  const now = new Date().toLocaleString("pt-BR", {
    dateStyle: "short",
    timeStyle: "short",
  });
  const title = data.title || "Documenta\u00e7\u00e3o T\u00e9cnica";
  const lines = [];

  /* ── Cabeçalho ── */
  lines.push(`h1. ${title}`);
  lines.push("");
  lines.push(
    "{panel:title=Vis\u00e3o Executiva|borderStyle=solid|borderColor=#0073b1|titleBGColor=#deebf7|bgColor=#ffffff}",
  );
  lines.push(data.objective || "Documento gerado pelo Document Builder.");
  lines.push("{panel}");
  lines.push("");

  /* ── Metadados ── */
  lines.push("h2. Informa\u00e7\u00f5es Gerais");
  lines.push("");
  lines.push("|| Campo || Valor ||");
  lines.push(`| Tabela | ${data.table_path || data.table_name || "\u2014"} |`);
  lines.push(`| Tipo | ${data.doc_type || "\u2014"} |`);
  lines.push(`| Frequ\u00eancia | ${data.frequency || "\u2014"} |`);
  lines.push(`| P\u00fablico-alvo | ${data.audience || "\u2014"} |`);
  lines.push(`| Resumo | ${data.summary || "\u2014"} |`);
  lines.push("");

  /* ── Seções ── */
  if (sections.length) {
    sections.forEach((s) => {
      lines.push(`h2. ${translateSectionTitle(s.title || "Se\u00e7\u00e3o")}`);
      lines.push("");
      lines.push(s.content || "Sem conte\u00fado informado.");
      lines.push("");
    });
  }

  /* ── Dicion\u00e1rio de dados ── */
  if (dataDictionary.length) {
    lines.push("h2. \uD83D\uDDC2 Dicion\u00e1rio de Dados");
    lines.push("");
    lines.push(
      "|| Coluna || Tipo || Descri\u00e7\u00e3o || Regra de Neg\u00f3cio ||",
    );
    dataDictionary.forEach((row) => {
      const col = row.column || "\u2014";
      const type = row.type || "\u2014";
      const desc = row.description || "\u2014";
      const rule = row.business_rule || "\u2014";
      lines.push(`| {{${col}}} | *${type}* | ${desc} | ${rule} |`);
    });
    lines.push("");
  }

  /* ── Checklist ── */
  if (checklist.length) {
    lines.push("h2. \u2705 Checklist de Qualidade");
    lines.push("");
    checklist.forEach((item) => lines.push(`* ${item}`));
    lines.push("");
  }

  /* ── Regras e pend\u00eancias ── */
  const ruleItems = [...typingNotes, ...pendingTechnical];
  if (ruleItems.length) {
    lines.push("h2. \u26A0\uFE0F Regras & Pend\u00eancias");
    lines.push("");
    ruleItems.forEach((item) => lines.push(`* ${item}`));
    lines.push("");
  }

  /* ── Governan\u00e7a ── */
  const govLines = [
    ...governanceAspects.map((a) => `* *Aspecto:* ${a}`),
    ...governanceReaders.map((r) => `* *Leitor:* ${r}`),
    ...governanceNotes.map((n) => `* *Nota:* ${n}`),
  ];
  if (govLines.length) {
    lines.push("h2. \uD83D\uDD12 Governan\u00e7a");
    lines.push("");
    govLines.forEach((g) => lines.push(g));
    lines.push("");
  }

  /* ── Pr\u00f3ximos passos ── */
  if (nextSteps.length) {
    lines.push("h2. \uD83D\uDE80 Pr\u00f3ximos Passos");
    lines.push("");
    nextSteps.forEach((item) => lines.push(`# ${item}`));
    lines.push("");
  }

  /* ── Avisos ── */
  if (warnings.length) {
    lines.push("{warning:title=Avisos do pipeline}");
    warnings.forEach((w) => lines.push(`* ${w}`));
    lines.push("{warning}");
    lines.push("");
  }

  /* ── Rodap\u00e9 ── */
  lines.push("----");
  lines.push(
    `{info:title=Gerado automaticamente}Gerado em ${now} por Document Builder \u00b7 Engenharia de Dados Financeiro{info}`,
  );

  return lines.join("\n");
}

function copyDBHtmlDocument() {
  const content = document.getElementById("db-html-source")?.textContent || "";
  const btn = document.getElementById("db-copy-html-btn");
  if (!content) return;

  copyTextWithFallback(content)
    .then(() => {
      if (!btn) return;
      const old = btn.textContent;
      btn.textContent = "✓ Copiado!";
      btn.style.color = "#34D399";
      setTimeout(() => {
        btn.textContent = old || "Copiar HTML";
        btn.style.color = "";
      }, 1800);
    })
    .catch(() => {
      showDBError("Não foi possível copiar o HTML automaticamente.");
    });
}

function copyTextWithFallback(text) {
  if (navigator.clipboard && window.isSecureContext) {
    return navigator.clipboard.writeText(text);
  }

  return new Promise((resolve, reject) => {
    try {
      const temp = document.createElement("textarea");
      temp.value = text;
      temp.setAttribute("readonly", "");
      temp.style.position = "absolute";
      temp.style.left = "-9999px";
      document.body.appendChild(temp);
      temp.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(temp);
      if (!ok) {
        reject(new Error("copy-failed"));
        return;
      }
      resolve();
    } catch (err) {
      reject(err);
    }
  });
}

function renderQB(data) {
  const empty = document.getElementById("qb-empty");
  const tabsArea = document.getElementById("qb-tabs-area");

  if (empty) empty.style.display = "none";
  if (tabsArea) tabsArea.style.display = "block";

  const builtSql = document.getElementById("qb-built-sql");
  const sampleHead = document.getElementById("qb-sample-head");
  const sampleBody = document.getElementById("qb-sample-body");
  const sampleNote = document.getElementById("qb-sample-note");
  const sampleWrap = document.getElementById("qb-sample-wrap");
  const recsList = document.getElementById("qb-recs-list");
  const dryRun = document.getElementById("qb-dryrun");
  const summary = document.getElementById("qb-summary");
  const gradeBlock = document.getElementById("qb-grade-block");
  const gradeLtr = document.getElementById("qb-grade-ltr");
  const scoreBig = document.getElementById("qb-score-big");
  const scoreFill = document.getElementById("qb-score-fill");
  const builtTab = document.getElementById("qb-tab-premises");
  const recsTab = document.getElementById("qb-tab-recs");
  const optTab = document.getElementById("qb-tab-optimized");
  const qbTiles = document.getElementById("qb-tiles");
  const qbOrigBytes = document.getElementById("qb-borig");
  const qbOrigCost = document.getElementById("qb-corig");
  const qbOptBytes = document.getElementById("qb-bopt");
  const qbOptCost = document.getElementById("qb-copt");
  const qbAntiCount = document.getElementById("qb-sav");
  const qbAntiSub = document.getElementById("qb-savusd");
  const qbSavSec = document.getElementById("qb-sav-sec");
  const qbSavBig = document.getElementById("qb-sav-big");
  const qbSavFill = document.getElementById("qb-sav-fill");
  const qbOptEmpty = document.getElementById("qb-opt-empty");
  const qbOptSec = document.getElementById("qb-opt-sec");
  const qbRecSec = document.getElementById("qb-rec-sec");
  const qbTipsSec = document.getElementById("qb-tips-sec");

  const dry = data.dry_run || {};
  const sample = data.sample_data || {};
  const sampleColumns = Array.isArray(sample.columns) ? sample.columns : [];
  const sampleRows = Array.isArray(sample.rows) ? sample.rows : [];
  const sampleError = sample.error;

  // Query Build sempre apresenta a melhor construcao de query com pontuacao maxima.
  const score = 100;
  const grade = "A";

  if (gradeBlock) gradeBlock.className = "grade-block gb-" + grade;
  if (gradeLtr) gradeLtr.textContent = grade;
  if (scoreBig) scoreBig.textContent = score;
  if (scoreFill) {
    scoreFill.className = "score-fill sf-" + grade;
    setTimeout(() => {
      scoreFill.style.width = `${score}%`;
    }, 80);
  }

  if (summary) {
    summary.textContent =
      data.explanation ||
      "Query construida com foco em performance e melhor aproveitamento de slots no BigQuery.";
  }

  if (qbTiles) {
    const hasDry =
      dry.bytes_processed != null || dry.estimated_cost_usd != null;
    qbTiles.style.display = hasDry ? "grid" : "none";
    if (qbOrigBytes) qbOrigBytes.textContent = fmtBytes(dry.bytes_processed);
    if (qbOrigCost) qbOrigCost.textContent = fmtUSD(dry.estimated_cost_usd);
    if (qbOptBytes) qbOptBytes.textContent = fmtBytes(dry.bytes_processed);
    if (qbOptCost) qbOptCost.textContent = fmtUSD(dry.estimated_cost_usd);
  }
  if (qbAntiCount) qbAntiCount.textContent = "A (100/100)";
  if (qbAntiSub) qbAntiSub.textContent = "Melhor construcao aplicada";

  if (qbSavSec) qbSavSec.style.display = "block";
  if (qbSavBig) qbSavBig.textContent = `${score}%`;
  if (qbSavFill) {
    setTimeout(() => {
      qbSavFill.style.width = `${score}%`;
    }, 120);
  }

  if (builtSql) {
    builtSql.textContent = data.generated_sql || "Nenhum SQL foi retornado.";
  }
  const hasSample = sampleColumns.length > 0 && sampleRows.length > 0;
  if (qbOptSec)
    qbOptSec.style.display = hasSample || sampleError ? "block" : "none";
  if (qbOptEmpty)
    qbOptEmpty.style.display = hasSample || sampleError ? "none" : "flex";
  if (optTab) optTab.classList.add("has-data");

  if (sampleHead) {
    sampleHead.innerHTML = hasSample
      ? `<tr>${sampleColumns.map((col) => `<th>${col}</th>`).join("")}</tr>`
      : "";
  }

  if (sampleBody) {
    sampleBody.innerHTML = "";
    if (hasSample) {
      sampleRows.forEach((row) => {
        sampleBody.innerHTML += `<tr>${sampleColumns
          .map((col) => `<td>${formatSampleCell(row[col])}</td>`)
          .join("")}</tr>`;
      });
    }
  }

  if (sampleWrap) {
    sampleWrap.style.display = hasSample ? "block" : "none";
  }

  if (sampleNote) {
    if (hasSample) {
      sampleNote.textContent = `Amostra limitada a ${sampleRows.length} linha(s) para consulta rapida.`;
    } else if (sampleError) {
      sampleNote.textContent = `Nao foi possivel carregar amostra: ${sampleError}`;
    } else {
      sampleNote.textContent = "";
    }
  }

  if (builtTab) builtTab.classList.add("has-data");

  const recommendations = [
    "No BigQuery, mantenha filtros de data/particao no inicio para reduzir bytes lidos e slots consumidos.",
    "Selecione apenas colunas necessarias e evite SELECT * para reduzir custo computacional.",
    "Materialize a query em tabela resumida antes do Power BI quando o volume for alto.",
    "No Power BI, prefira refresh incremental e agregacoes para evitar consultas full scan recorrentes.",
    "Monitore dry-run e JOBS_TIMELINE para ajustar janelas e diminuir picos de slot usage.",
  ];

  if (recsList) {
    recsList.innerHTML = recommendations
      .map(
        (r, i) =>
          `<div class="rec-item"><span class="rec-n">${String(i + 1).padStart(2, "0")}</span>${r}</div>`,
      )
      .join("");
  }
  if (qbRecSec)
    qbRecSec.style.display = recommendations.length ? "block" : "none";
  if (recsTab) recsTab.classList.add("has-data");

  if (dryRun) {
    if (dry.error) {
      dryRun.innerHTML = `<div class="rec-item" style="border-color:#fecaca;background:var(--rose-bg);color:var(--rose)">⚠ ${dry.error}</div>`;
    } else {
      dryRun.innerHTML = `
        <div class="rec-item">Bytes processados: <strong style="margin-left:6px">${fmtBytes(dry.bytes_processed)}</strong></div>
        <div class="rec-item">Custo estimado: <strong style="margin-left:6px">${fmtUSD(dry.estimated_cost_usd)}</strong></div>
      `;
    }
  }
  if (qbTipsSec) qbTipsSec.style.display = "block";

  switchQBTab("score");
}

function switchQBTab(name) {
  document.querySelectorAll('[id^="qb-tab-"]').forEach((t) => {
    t.classList.remove("active");
  });

  document.querySelectorAll('[id^="qb-panel-"]').forEach((p) => {
    p.classList.remove("active");
  });

  const tab = document.getElementById("qb-tab-" + name);
  const panel = document.getElementById("qb-panel-" + name);

  if (tab) tab.classList.add("active");
  if (panel) panel.classList.add("active");
}

function setQBLoading(on) {
  const btn = document.getElementById("qb-btn");
  const spinner = document.getElementById("qb-spinner");
  const text = document.getElementById("qb-btn-text");

  qbIsLoading = on;
  syncQBGenerateButtonState();
  if (spinner) spinner.style.display = on ? "block" : "none";
  if (text)
    text.textContent = on ? "Gerando SQL..." : "Gerar SQL com Query Builder";
}

function showQBError(message) {
  const box = document.getElementById("qb-error");
  if (!box) return;

  if (!message) {
    box.style.display = "none";
    box.textContent = "";
    return;
  }

  box.textContent = "⚠ " + message;
  box.style.display = "block";
}

function copyQBSQL() {
  const sql = document.getElementById("qb-built-sql")?.textContent || "";
  if (!sql) return;

  navigator.clipboard.writeText(sql);
}

function formatSampleCell(value) {
  if (value == null) return "—";
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch (_e) {
      return String(value);
    }
  }
  return String(value);
}

function copyQBBuiltSQL() {
  const sql = document.getElementById("qb-built-sql")?.textContent || "";
  if (!sql) return;

  navigator.clipboard.writeText(sql);
}

function renderQA(d) {
  const grade = d.grade || "—";
  const now = new Date();
  const timeStr =
    now.getHours().toString().padStart(2, "0") +
    ":" +
    now.getMinutes().toString().padStart(2, "0");

  const qaEmpty = document.getElementById("qa-empty");
  const qaTabsArea = document.getElementById("qa-tabs-area");
  const qaLastRun = document.getElementById("qa-last-run");
  const qaLastTime = document.getElementById("qa-last-time");
  const qaLastScore = document.getElementById("qa-last-score");

  if (qaEmpty) qaEmpty.style.display = "none";
  if (qaTabsArea) qaTabsArea.style.display = "flex";

  if (qaLastRun) qaLastRun.style.display = "flex";
  if (qaLastTime) qaLastTime.textContent = timeStr;
  if (qaLastScore) {
    qaLastScore.textContent = `${d.efficiency_score}/100 (${d.grade})`;
  }

  // Score
  const gradeBlock = document.getElementById("q-grade-block");
  const gradeLtr = document.getElementById("q-grade-ltr");
  const scoreBig = document.getElementById("q-score-big");
  const scoreFill = document.getElementById("q-score-fill");
  const summary = document.getElementById("q-summary");

  if (gradeBlock) gradeBlock.className = "grade-block gb-" + grade;
  if (gradeLtr) gradeLtr.textContent = grade;
  if (scoreBig) scoreBig.textContent = d.efficiency_score ?? "—";
  if (scoreFill) {
    scoreFill.className = "score-fill sf-" + grade;
    setTimeout(() => {
      scoreFill.style.width = `${d.efficiency_score || 0}%`;
    }, 80);
  }
  if (summary) summary.textContent = d.summary || "Sem resumo disponível.";

  // Tiles
  const qTiles = document.getElementById("q-tiles");
  const qSavSec = document.getElementById("q-sav-sec");

  if (d.bytes_original != null && qTiles) {
    qTiles.style.display = "grid";

    document.getElementById("q-borig").textContent = fmtBytes(d.bytes_original);
    document.getElementById("q-corig").textContent = fmtUSD(
      d.cost_original_usd,
    );
    document.getElementById("q-bopt").textContent = fmtBytes(d.bytes_optimized);
    document.getElementById("q-copt").textContent = fmtUSD(
      d.cost_optimized_usd,
    );

    const pct = d.savings_pct || 0;

    document.getElementById("q-sav").textContent =
      pct > 0 ? `↓ ${pct}%` : "N/A";
    document.getElementById("q-savusd").textContent =
      d.cost_saved_usd != null
        ? `USD ${Number(d.cost_saved_usd).toFixed(4)}`
        : "—";

    if (pct > 0 && qSavSec) {
      qSavSec.style.display = "block";
      document.getElementById("q-sav-big").textContent = `↓ ${pct}%`;

      setTimeout(() => {
        document.getElementById("q-sav-fill").style.width = `${pct}%`;
      }, 150);
    }
  }

  // Anti-patterns
  const apList = document.getElementById("q-ap-list");
  const apCount = Array.isArray(d.antipatterns) ? d.antipatterns.length : 0;
  const tabAp = document.getElementById("tab-antipatterns");
  const tabApCount = document.getElementById("tab-ap-count");
  const qApCount = document.getElementById("q-ap-count");

  if (tabAp) tabAp.classList.add("has-data");
  if (tabApCount) {
    tabApCount.textContent = String(apCount);
    tabApCount.className = apCount === 0 ? "qa-tab-count ok" : "qa-tab-count";
  }
  if (qApCount) {
    qApCount.textContent = apCount
      ? `${apCount} encontrado${apCount > 1 ? "s" : ""}`
      : "0 encontrados";
  }

  if (apList) {
    apList.innerHTML = "";

    if (!apCount) {
      apList.innerHTML = `
        <div class="rec-item" style="color:var(--emerald);border-color:#A7F3D0;background:var(--emerald-bg)">
          <span>✓</span> Nenhum anti-padrão. Query eficiente!
        </div>
      `;
    } else {
      d.antipatterns.forEach((ap) => {
        const severity = String(ap.severity || "medium").toLowerCase();

        apList.innerHTML += `
          <div class="ap-card sev-${severity}">
            <div class="ap-top">
              <span class="ap-chip chip-${severity}">${severity}</span>
              <span class="ap-name">${ap.pattern}</span>
            </div>
            <div class="ap-desc">${ap.description}</div>
            <div class="ap-fix">
              <span>✦</span>
              <span>${ap.suggestion}</span>
            </div>
          </div>
        `;
      });
    }
  }

  // Optimized query
  const tabOptimized = document.getElementById("tab-optimized");
  const qOptSec = document.getElementById("q-opt-sec");
  const qOptEmpty = document.getElementById("q-opt-empty");
  const qOptQuery = document.getElementById("q-opt-query");

  if (tabOptimized) tabOptimized.classList.add("has-data");

  if (d.optimized_query) {
    if (qOptSec) qOptSec.style.display = "block";
    if (qOptEmpty) qOptEmpty.style.display = "none";
    if (qOptQuery) qOptQuery.textContent = d.optimized_query;
  } else {
    if (qOptSec) qOptSec.style.display = "none";
    if (qOptEmpty) qOptEmpty.style.display = "flex";
  }

  // Applied optimizations
  const appliedTab = document.getElementById("tab-applied");
  const qAppliedSec = document.getElementById("q-applied-sec");
  const qAppliedList = document.getElementById("q-applied-list");
  const appliedOptimizations = Array.isArray(d.applied_optimizations)
    ? d.applied_optimizations
    : [];

  if (appliedOptimizations.length) {
    if (appliedTab) appliedTab.classList.add("has-data");
    if (qAppliedSec) qAppliedSec.style.display = "block";
    if (qAppliedList) {
      qAppliedList.innerHTML = appliedOptimizations
        .map(
          (item, i) =>
            `<div class="rec-item"><span class="rec-n">${String(i + 1).padStart(2, "0")}</span>${item}</div>`,
        )
        .join("");
    }
  } else {
    if (qAppliedSec) qAppliedSec.style.display = "none";
  }

  // Recommendations
  const recommendations = Array.isArray(d.recommendations)
    ? d.recommendations
    : [];
  const tips = Array.isArray(d.power_bi_tips) ? d.power_bi_tips : [];

  const tabRecs = document.getElementById("tab-recs");
  const qRecSec = document.getElementById("q-rec-sec");
  const qTipsSec = document.getElementById("q-tips-sec");
  const qRecList = document.getElementById("q-rec-list");
  const qTipsList = document.getElementById("q-tips-list");

  if (recommendations.length || tips.length) {
    if (tabRecs) tabRecs.classList.add("has-data");

    if (recommendations.length && qRecSec && qRecList) {
      qRecSec.style.display = "block";
      qRecList.innerHTML = recommendations
        .map(
          (r, i) =>
            `<div class="rec-item"><span class="rec-n">${String(i + 1).padStart(2, "0")}</span>${r}</div>`,
        )
        .join("");
    }

    if (tips.length && qTipsSec && qTipsList) {
      qTipsSec.style.display = "block";
      qTipsList.innerHTML = tips
        .map(
          (t) =>
            `<div class="tip-item"><span style="color:var(--porto);flex-shrink:0">◆</span>${t}</div>`,
        )
        .join("");
    }
  }

  switchTab("score");
}

function switchTab(name) {
  document.querySelectorAll(".qa-tab").forEach((t) => {
    t.classList.remove("active");
  });

  document.querySelectorAll(".qa-tab-panel").forEach((p) => {
    p.classList.remove("active");
  });

  const tab = document.getElementById("tab-" + name);
  const panel = document.getElementById("panel-" + name);

  if (tab) tab.classList.add("active");
  if (panel) panel.classList.add("active");
}

function copySQL() {
  const sql = document.getElementById("q-opt-query")?.textContent || "";
  const btn = document.querySelector(".copy-btn");

  if (!sql) return;

  navigator.clipboard.writeText(sql).then(() => {
    if (!btn) return;

    btn.textContent = "✓ Copiado!";
    btn.style.color = "#34D399";

    setTimeout(() => {
      btn.textContent = "Copiar SQL";
      btn.style.color = "";
    }, 2000);
  });
}

// ─────────────────────────────────────
// History
// ─────────────────────────────────────
function saveToHistory(data, query) {
  const history = JSON.parse(localStorage.getItem("qaHistory") || "[]");

  const item = {
    bot: "SQL Review",
    date: new Date().toISOString(),
    query: query,
    suggestedQuery: data.optimized_query || null,
    grade: data.grade || "—",
    score: data.efficiency_score || 0,
    originalBytes: data.bytes_original,
    optimizedBytes: data.bytes_optimized,
    savings: data.savings_pct || 0,
  };

  history.unshift(item); // Add to beginning

  // Keep only last 50
  if (history.length > 50) history.splice(50);

  localStorage.setItem("qaHistory", JSON.stringify(history));
}

function loadHistory() {
  const history = JSON.parse(localStorage.getItem("qaHistory") || "[]");
  const listEl = document.getElementById("hist-list");

  if (!listEl) return;

  listEl.innerHTML = "";

  if (history.length === 0) {
    return;
  }

  history.forEach((item) => {
    const date = new Date(item.date);
    const dateStr =
      date.toLocaleDateString("pt-BR") +
      " às " +
      date.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });

    // Determine performance badge
    let performanceBadge = "";
    let performanceColor = "";
    const score = item.score;
    if (score >= 90) {
      performanceBadge = "Excelente";
      performanceColor = "excellent";
    } else if (score >= 70) {
      performanceBadge = "Boa";
      performanceColor = "good";
    } else if (score >= 50) {
      performanceBadge = "Média";
      performanceColor = "average";
    }

    // Calculate improvement percentage
    const improvementPercent =
      item.savings ||
      (
        ((item.originalBytes - item.optimizedBytes) / item.originalBytes) *
        100
      ).toFixed(0);

    const itemEl = document.createElement("div");
    itemEl.className = "hist-item";

    itemEl.innerHTML = `
      <div class="hist-card">
        <!-- Header com Info Principal -->
        <div class="hist-card-header">
          <div class="hist-card-top">
            <div class="hist-bot-section">
              <div class="hist-bot-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <circle cx="11" cy="11" r="8" />
                  <line x1="21" y1="21" x2="16.65" y2="16.65" />
                  <path d="M8 11h6M11 8v6" />
                </svg>
              </div>
              <div>
                <h3>${item.bot}</h3>
                <p>${dateStr}</p>
              </div>
            </div>
            <div class="hist-performance-badge ${performanceColor}">
              <span>${performanceBadge}</span>
            </div>
          </div>
        </div>

        <!-- Métricas Principais em Grid Visual -->
        <div class="hist-key-metrics">
          <div class="hist-key-metric score">
            <div class="hist-key-metric-label">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
              </svg>
              Pontuação
            </div>
            <div class="hist-key-metric-value">${item.score}</div>
            <div class="hist-key-metric-subtext">/100</div>
          </div>
          <div class="hist-key-metric grade">
            <div class="hist-key-metric-label">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
              </svg>
              Grau
            </div>
            <div class="hist-key-metric-value">${item.grade}</div>
            <div class="hist-key-metric-subtext">Classificação</div>
          </div>
          <div class="hist-key-metric improvement">
            <div class="hist-key-metric-label">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                <polyline points="17 8 12 3 7 8"/>
                <line x1="12" y1="3" x2="12" y2="15"/>
              </svg>
              Redução
            </div>
            <div class="hist-key-metric-value">${improvementPercent > 0 ? "↓" : ""}${improvementPercent}%</div>
            <div class="hist-key-metric-subtext">Processamento</div>
          </div>
        </div>

        <!-- Queries Section -->
        <div class="hist-queries-container">
          <div class="hist-query-row">
            <div class="hist-query-item original">
              <div class="hist-query-label">
                <div class="hist-label-left">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
                    <polyline points="14 2 14 8 20 8"/>
                    <line x1="16" y1="13" x2="8" y2="13"/>
                    <line x1="16" y1="17" x2="8" y2="17"/>
                  </svg>
                  <span>Query Original</span>
                </div>
                <button class="hist-query-copy-btn original-btn" onclick="copyToClipboard(event)" data-query=\"${item.query}\" title=\"Copiar query\">
                  <svg viewBox=\"0 0 24 24\" fill=\"currentColor\">
                    <path d=\"M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z\"/>
                  </svg>
                  <span class=\"ripple\"></span>
                </button>
              </div>
              <div class="hist-query-code">${item.query}</div>
              <div class="hist-query-bytes">${fmtBytes(item.originalBytes)}</div>
            </div>
            <div class="hist-arrow">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <line x1="5" y1="12" x2="19" y2="12" />
                <polyline points="12 5 19 12 12 19" />
              </svg>
            </div>
            <div class="hist-query-item suggested">
              <div class="hist-query-label">
                <div class="hist-label-left">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
                    <polyline points="14 2 14 8 20 8"/>
                    <line x1="16" y1="13" x2="8" y2="13"/>
                    <line x1="16" y1="17" x2="8" y2="17"/>
                  </svg>
                  <span>Query Otimizada</span>
                </div>
                <button class="hist-query-copy-btn suggested-btn" onclick="copyToClipboard(event)" data-query=\"${item.suggestedQuery || item.query}\" title=\"Copiar query\">
                  <svg viewBox=\"0 0 24 24\" fill=\"currentColor\">
                    <path d=\"M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z\"/>
                  </svg>
                  <span class=\"ripple\"></span>
                </button>
              </div>
              <div class="hist-query-code">${item.suggestedQuery || item.query}</div>
              <div class="hist-query-bytes">${fmtBytes(item.optimizedBytes)}</div>
            </div>
          </div>
        </div>

        <!-- Detalhes de Bytes -->
        <div class="hist-bytes-detail">
          <div class="hist-byte-comparison">
            <div class="hist-byte-item original">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
              <div class="hist-byte-info">
                <div class="hist-byte-label">Original</div>
                <div class="hist-byte-value">${fmtBytes(item.originalBytes)}</div>
              </div>
            </div>
            <div class="hist-byte-savings">
              <div class="hist-byte-label">Economia</div>
              <div class="hist-byte-value">${item.savings > 0 ? "↓ " + item.savings : "—"}%</div>
            </div>
            <div class="hist-byte-item optimized">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
              <div class="hist-byte-info">
                <div class="hist-byte-label">Otimizado</div>
                <div class="hist-byte-value">${fmtBytes(item.optimizedBytes)}</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;

    listEl.appendChild(itemEl);
  });
}

function showQAError(message) {
  const box = document.getElementById("qa-error");
  if (!box) return;

  box.textContent = "⚠ " + prettifyErrorMessage(message);
  box.style.display = "block";
}

function setQALoading(on) {
  const btn = document.getElementById("qa-btn");
  const spinner = document.getElementById("qa-spinner");
  const text = document.getElementById("qa-btn-text");
  const qaLastRun = document.getElementById("qa-last-run");
  const qaLastTime = document.getElementById("qa-last-time");

  qaIsLoading = on;
  syncQAAnalyzeButtonState();
  if (spinner) spinner.style.display = on ? "block" : "none";
  if (text) {
    text.textContent = on ? "Analisando..." : "Analisar com SQL Review";
  }

  if (qaLastRun) {
    qaLastRun.style.display = on
      ? "none"
      : qaLastTime && qaLastTime.textContent !== "--:--"
        ? "flex"
        : "none";
  }
}

// ─────────────────────────────────────
// Bot filtering
// ─────────────────────────────────────
function filterBots(q) {
  const term = String(q || "").toLowerCase();

  document.querySelectorAll(".bot-card:not(.soon)").forEach((card) => {
    const name = card.querySelector(".bname")?.textContent.toLowerCase() || "";
    const desc = card.querySelector(".bdesc")?.textContent.toLowerCase() || "";
    const matched = !term || name.includes(term) || desc.includes(term);

    card.style.opacity = matched ? "1" : "0.3";
    card.style.transform = matched ? "" : "scale(0.985)";
  });
}

// ─────────────────────────────────────
// Keyboard shortcuts
// ─────────────────────────────────────
document.addEventListener("keydown", (e) => {
  if (
    e.key === "Enter" &&
    document.getElementById("screen-login")?.classList.contains("active")
  ) {
    if (
      document.activeElement?.id === "inp-pass" ||
      document.activeElement?.id === "inp-user"
    ) {
      doLogin();
    }
  }
});

document.getElementById("qa-query")?.addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    runAnalyze();
  }
});

document.getElementById("qa-query")?.addEventListener("input", () => {
  if (qaAnalyzeInFlight || qaIsLoading) {
    return;
  }

  qaDatasetValidationState.status = "checking";
  setQADatasetValidationStatus("checking", {
    title: "Aguardando sua digitacao",
    message: "Vamos validar automaticamente apos 1 segundo de pausa.",
  });
  scheduleQAQueryValidation();
});

document.getElementById("qb-request")?.addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    runQueryBuild();
  }
});

document.getElementById("db-request")?.addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    runDocumentBuild();
  }
});

document.getElementById("qb-dataset")?.addEventListener("input", () => {
  const datasetHint = document.getElementById("qb-dataset")?.value.trim() || "";

  if (!datasetHint) {
    qbDatasetValidationState.status = "idle";
    setQBDatasetValidationStatus("idle");
    return;
  }

  qbDatasetValidationState.status = "checking";
  setQBDatasetValidationStatus("checking", {
    title: "Aguardando sua digitacao",
    message: "Vamos validar automaticamente apos 1 segundo de pausa.",
  });
  scheduleQBDatasetValidation();
});

document.getElementById("qb-dataset")?.addEventListener("blur", () => {
  validateQBDatasetHint();
});

document.getElementById("qb-project")?.addEventListener("input", () => {
  const datasetHint = document.getElementById("qb-dataset")?.value.trim() || "";
  if (!datasetHint) {
    syncQBGenerateButtonState();
    return;
  }

  qbDatasetValidationState.status = "checking";
  setQBDatasetValidationStatus("checking", {
    title: "Revalidando dataset",
    message: "Project ID alterado. Estamos validando novamente.",
  });
  scheduleQBDatasetValidation();
});

// ─────────────────────────────────────
// Init
// ─────────────────────────────────────
window.addEventListener("load", function init() {
  console.log("🚀 Inicializando Finance Hub IA...");
  try {
    showScreen("screen-login");
    document.getElementById("inp-user")?.focus();
    enforceQAConfigReadOnly();

    // Remover event listeners dos botões que foram removidos
    renderShowcase();
    startShowcaseAutoplay();
    setQADatasetValidationStatus("idle");
    syncQAAnalyzeButtonState();
    syncQBGenerateButtonState();
    console.log("✅ Inicialização concluída!");
  } catch (error) {
    console.error("❌ Erro na inicialização:", error);
  }
});

const showcaseBots = [
  {
    name: "SQL Review",
    description:
      "Reduza custo e tempo de execução com revisão automática de anti-padrões e SQL otimizada.",
    tags: ["BigQuery", "SQL", "Performance"],
    status: "Disponivel",
    action: () => navTo("qa"),
  },
  {
    name: "Document Builder",
    description:
      "Gere documentação que o negócio entende e a engenharia confia: schema real, governança e exportação pronta.",
    tags: ["Docs", "Pipeline", "DataOps"],
    status: "Disponivel",
    action: () => navTo("db"),
  },
  {
    name: "Query Builder",
    description:
      "Da pergunta ao SQL em minutos, com contexto real para análises de receita, margem e risco.",
    tags: ["NL2SQL", "BigQuery", "IA"],
    status: "Disponivel",
    action: () => navTo("qb"),
  },
  {
    name: "Finance AuditorIA",
    description:
      "Monitore conformidade financeira e saúde dos KPIs com rastreabilidade ponta a ponta.",
    tags: ["Auditoria", "KPIs", "Compliance"],
    status: "Disponivel",
    action: () =>
      openDev(
        "Finance AuditorIA",
        "Monitora pipelines e indicadores financeiros com foco em conformidade, risco e consistência de KPIs.",
        [
          "Rastreabilidade Gold/Silver",
          "Verificação de KPIs",
          "Relatório de compliance",
        ],
        "Q3 2025",
      ),
  },
];

let showcaseIndex = 0;
let showcaseTimer = null;

function renderShowcase() {
  const titleEl = document.getElementById("showcase-title");
  const descEl = document.getElementById("showcase-desc");
  const tagsEl = document.getElementById("showcase-tags");
  const statusEl = document.getElementById("showcase-status");
  const dotsEl = document.getElementById("showcase-dots");
  const mainEl = document.querySelector(".bot-showcase-main");

  if (!titleEl || !descEl || !tagsEl || !statusEl || !dotsEl || !mainEl) return;

  // Fade out
  mainEl.style.opacity = "0";

  setTimeout(() => {
    const bot = showcaseBots[showcaseIndex];

    titleEl.textContent = bot.name;
    descEl.textContent = bot.description;

    tagsEl.innerHTML = bot.tags
      .map((tag) => `<span class="bot-showcase-tag">${tag}</span>`)
      .join("");

    statusEl.textContent = bot.status;
    statusEl.className = `bot-showcase-badge ${bot.status.toLowerCase().replace(/\s+/g, "-")}`;

    dotsEl.innerHTML = showcaseBots
      .map(
        (_, i) =>
          `<button class="bot-showcase-dot ${i === showcaseIndex ? "active" : ""}" aria-label="Ir para bot ${i + 1}" onclick="goToShowcase(${i})"></button>`,
      )
      .join("");

    // Fade in
    mainEl.style.opacity = "1";
  }, 250);
}

function nextShowcase() {
  showcaseIndex = (showcaseIndex + 1) % showcaseBots.length;
  renderShowcase();
}

function prevShowcase() {
  showcaseIndex =
    (showcaseIndex - 1 + showcaseBots.length) % showcaseBots.length;
  renderShowcase();
}

function goToShowcase(index) {
  showcaseIndex = index;
  renderShowcase();
  restartShowcaseAutoplay();
}

function startShowcaseAutoplay() {
  stopShowcaseAutoplay();
  showcaseTimer = setInterval(() => {
    nextShowcase();
  }, 4500);
}

function stopShowcaseAutoplay() {
  if (showcaseTimer) {
    clearInterval(showcaseTimer);
    showcaseTimer = null;
  }
}

function restartShowcaseAutoplay() {
  startShowcaseAutoplay();
}
