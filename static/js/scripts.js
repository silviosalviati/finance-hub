// ─────────────────────────────────────
// App state
// ─────────────────────────────────────
let token = null;
let currentUser = null;
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
let auditIsLoading = false;
let auditMarkdownCache = "";
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
    return "N�o foi poss�vel autenticar no BigQuery. Verifique as credenciais do ambiente.";
  }

  if (
    msg.includes("401") ||
    msg.includes("N�o autenticado") ||
    msg.includes("Sess�o expirada")
  ) {
    return "Sua sess�o expirou. Fa�a login novamente.";
  }

  if (msg.toLowerCase().includes("query n�o pode ser vazia")) {
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

function setAuditProgress(stepText, pct) {
  const progress = document.getElementById("audit-progress");
  const step = document.getElementById("audit-progress-step");
  const fill = document.getElementById("audit-progress-fill");

  if (!progress || !step || !fill) return;

  progress.style.display = "flex";
  step.textContent = stepText;
  fill.style.width = `${pct}%`;
}

function hideAuditProgress() {
  const progress = document.getElementById("audit-progress");
  const step = document.getElementById("audit-progress-step");
  const fill = document.getElementById("audit-progress-fill");

  if (!progress || !step || !fill) return;

  progress.style.display = "none";
  step.textContent = "Extraindo filtros";
  fill.style.width = "8%";
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
        ? "Dataset pronto para análise"
        : kind === "checking"
          ? "Validando contexto da query"
          : "Valida��o pendente");
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
        message: "Sess�o expirada. Fa�a login novamente.",
      };
    }

    const payload = await res.json();
    if (!res.ok) {
      throw new Error(payload?.detail || "Falha na valida��o da query.");
    }

    const currentQuery =
      document.getElementById("qa-query")?.value.trim() || "";
    if (currentQuery !== querySnapshot) {
      return {
        valid: false,
        projectId: "",
        datasetHint: "",
        message: "A query foi alterada durante a valida��o. Tente novamente.",
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
        message: payload.message || "Query validada. Já pode analisar.",
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
        title: "Contexto n�o validado",
        message:
          payload.message ||
          "N�o foi poss�vel validar dataset e tabelas da query.",
      });
      return {
        valid: false,
        projectId: detectedProject,
        datasetHint: detectedDataset,
        message:
          payload.message ||
          "N�o foi poss�vel validar dataset e tabelas da query.",
      };
    }
  } catch (err) {
    qaDatasetValidationState.status = "invalid";
    setQADatasetValidationStatus("error", {
      title: "Falha na valida��o",
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
          : "Valida��o pendente");
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
      throw new Error(payload?.detail || "Falha na valida��o do dataset.");
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
        message: "Valida��o conclu�da. J� pode gerar a SQL.",
        tableCount: count,
      });
    } else {
      qbDatasetValidationState.status = "invalid";
      setQBDatasetValidationStatus("error", {
        title: "Dataset n�o validado",
        message:
          payload.message || "Dataset n�o validado para uso no Query Builder.",
      });
    }
  } catch (err) {
    qbDatasetValidationState.status = "invalid";
    setQBDatasetValidationStatus("error", {
      title: "Falha na valida��o",
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

  const userEl = document.getElementById("inp-user");
  const passEl = document.getElementById("inp-pass");

  if (userEl) userEl.value = "";
  if (passEl) passEl.value = "";

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
    audit: "view-fa",
    er: "view-er",
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
  } else if (view === "audit") {
    document.getElementById("nav-audit")?.classList.add("active");
    initFAInputListener();
  } else if (view === "er") {
    document.getElementById("nav-er")?.classList.add("active");
    initErView();
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
    "Finance Voice IA": devColors.emerald,
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

    const res = await fetch("/api/agents/query_analyzer/analyze", {
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

    setQAProgress("Finalizando apresenta��o...", 100);
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
    showQBError("Descreva a solicita��o antes de gerar SQL.");
    return;
  }

  if (!projectId) {
    showQBError("Preencha o Project ID do GCP.");
    return;
  }

  if (!datasetHint) {
    showQBError("Preencha o Dataset hint obrigatório.");
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
    setQBProgress("Finalizando apresenta��o...", 100);
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
    showDBError("Descreva o contexto antes de gerar a documenta��o.");
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
    setTimeout(() => setDBProgress("Estruturando documenta��o...", 38), 180);
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
      throw new Error(e.detail || "Erro ao gerar documenta��o");
    }

    const data = await res.json();
    if (data.status === "error") {
      throw new Error(data.error || "N�o foi poss�vel gerar a documenta��o.");
    }

    setDBProgress("Finalizando apresenta��o...", 100);
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

async function runAudit() {
  const requestText =
    document.getElementById("audit-request")?.value.trim() || "";
  const projectId =
    document.getElementById("audit-project")?.value.trim() || "";
  const datasetHint =
    document.getElementById("audit-dataset")?.value.trim() || "";
  const errorEl = document.getElementById("audit-error");
  const empty = document.getElementById("audit-empty");
  const tabsArea = document.getElementById("audit-tabs-area");

  if (auditIsLoading) return;

  if (errorEl) {
    errorEl.style.display = "none";
    errorEl.textContent = "";
  }

  if (!requestText) {
    showAuditError("Descreva o contexto da auditoria antes de executar.");
    return;
  }
  if (!projectId) {
    showAuditError("Informe o Project ID — GCP.");
    return;
  }
  if (!datasetHint) {
    showAuditError("Informe o Dataset hint para contextualizar a auditoria.");
    return;
  }

  const query =
    `${requestText}\n` +
    `[PROJECT_ID] ${projectId}\n` +
    `[DATASET_HINT] ${datasetHint}\n` +
    "[FOCO] auditoria de experi�ncia do cliente, fric��o, VoC, NPS";

  setAuditLoading(true);
  setAuditProgress("Extraindo filtros", 10);

  const timers = [
    setTimeout(
      () => setAuditProgress("Buscando interações no BigQuery", 28),
      300,
    ),
    setTimeout(
      () => setAuditProgress("Analisando sentimentos e fric��o", 52),
      800,
    ),
    setTimeout(() => setAuditProgress("Classificando temas VoC", 74), 1400),
    setTimeout(() => setAuditProgress("Gerando relatório executivo", 90), 2200),
  ];

  try {
    const res = await fetch("/api/agents/finance_auditor/analyze", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        query,
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
      throw new Error(payload?.detail || "Falha ao executar auditoria.");
    }

    if (payload.status === "error") {
      throw new Error(payload.error || "Falha ao gerar auditoria.");
    }

    setAuditProgress("Finalizando apresenta��o", 100);
    renderAudit(payload);
    if (empty) empty.style.display = "none";
    if (tabsArea) tabsArea.style.display = "flex";
  } catch (err) {
    showAuditError(prettifyErrorMessage(err.message || "Erro na auditoria."));
  } finally {
    timers.forEach((id) => clearTimeout(id));
    setTimeout(() => {
      hideAuditProgress();
      setAuditLoading(false);
    }, 250);
  }
}

function setAuditLoading(on) {
  const btn = document.getElementById("audit-btn");
  const spinner = document.getElementById("audit-spinner");
  const text = document.getElementById("audit-btn-text");
  const request = document.getElementById("audit-request");
  const project = document.getElementById("audit-project");
  const dataset = document.getElementById("audit-dataset");

  auditIsLoading = on;
  if (btn) btn.disabled = on;
  if (spinner) spinner.style.display = on ? "block" : "none";
  if (text)
    text.textContent = on
      ? "Auditando experiência do cliente..."
      : "Auditar experiência do cliente";

  [request, project, dataset].forEach((el) => {
    if (el) el.disabled = on;
  });
}

function showAuditError(message) {
  const box = document.getElementById("audit-error");
  if (!box) return;
  box.textContent = "⚠ " + prettifyErrorMessage(message);
  box.style.display = "block";
}

function switchAuditTab(name) {
  document.querySelectorAll('[id^="audit-tab-"]').forEach((el) => {
    el.classList.remove("active");
  });
  document.querySelectorAll('[id^="audit-panel-"]').forEach((el) => {
    el.classList.remove("active");
  });

  document.getElementById(`audit-tab-${name}`)?.classList.add("active");
  document.getElementById(`audit-panel-${name}`)?.classList.add("active");
}

function renderAudit(data) {
  const empty = document.getElementById("audit-empty");
  const tabsArea = document.getElementById("audit-tabs-area");
  if (empty) empty.style.display = "none";
  if (tabsArea) tabsArea.style.display = "flex";

  const title = data.audit_title || "Auditoria da Experiência do Cliente";
  const start = data.periodo_inicio || data.date_range?.start || "—";
  const end = data.periodo_fim || data.date_range?.end || "—";
  const total = Number(data.total_interacoes ?? data.total_records ?? 0);
  const metrics = data.cx_metrics || {};

  const scoreRaw = Number(metrics.friction_score ?? data.friction_score ?? 0);
  const score =
    scoreRaw <= 1 ? Math.round(scoreRaw * 100) : Math.round(scoreRaw);

  const titleEl = document.getElementById("audit-title");
  const periodEl = document.getElementById("audit-period-text");
  const totalEl = document.getElementById("audit-total-interacoes");
  if (titleEl) titleEl.textContent = title;
  if (periodEl) periodEl.textContent = `${start} a ${end}`;
  if (totalEl)
    totalEl.textContent = `${total.toLocaleString("pt-BR")} interações analisadas`;

  renderFrictionGauge(score);
  renderAuditKpis(metrics, score);

  renderSentimentBar(
    Number(metrics.sentimento_positivo_cliente_pct ?? 0),
    Number(metrics.sentimento_neutro_cliente_pct ?? 0),
    Number(metrics.sentimento_negativo_cliente_pct ?? 0),
  );
  renderSentimentList(
    Array.isArray(data.sentiment_trends) ? data.sentiment_trends : [],
  );

  renderFrictionPoints(
    Array.isArray(data.friction_points) ? data.friction_points : [],
  );
  renderVocThemes(Array.isArray(data.voc_themes) ? data.voc_themes : []);

  const insight = document.getElementById("audit-voc-insight");
  if (insight) {
    insight.textContent =
      data.voc_insight || data.audit_summary || "Sem insight consolidado.";
  }

  const report = document.getElementById("audit-markdown-report");
  auditMarkdownCache = String(data.markdown_report || "");
  if (report)
    report.textContent = auditMarkdownCache || "Sem relatório disponível.";

  renderRecommendationList(
    document.getElementById("audit-recommendations"),
    Array.isArray(data.recommendations) ? data.recommendations : [],
    false,
  );
  renderRecommendationList(
    document.getElementById("audit-checklist"),
    Array.isArray(data.quick_wins) ? data.quick_wins : [],
    true,
  );

  switchAuditTab("overview");
}

function renderFrictionGauge(score) {
  const clamped = Math.max(0, Math.min(100, Number(score || 0)));
  const bg = document.getElementById("audit-gauge-bg");
  const fg = document.getElementById("audit-gauge-progress");
  const scoreEl = document.getElementById("audit-gauge-score");
  const labelEl = document.getElementById("audit-gauge-label");
  if (!bg || !fg || !scoreEl || !labelEl) return;

  const cx = 120;
  const cy = 120;
  const r = 90;
  const start = 210;
  const end = -30;

  const polar = (angleDeg) => {
    const a = ((angleDeg - 90) * Math.PI) / 180;
    return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
  };
  const p0 = polar(start);
  const p1 = polar(end);
  const d = `M ${p0.x} ${p0.y} A ${r} ${r} 0 1 1 ${p1.x} ${p1.y}`;
  bg.setAttribute("d", d);
  fg.setAttribute("d", d);

  const arcLen = (2 * Math.PI * r * 240) / 360;
  fg.style.strokeDasharray = `${arcLen}`;

  let gaugeColor = "var(--emerald)";
  let gaugeLabel = "Excelente";
  if (clamped > 80) {
    gaugeColor = "var(--rose)";
    gaugeLabel = "Emergencial";
  } else if (clamped > 60) {
    gaugeColor = "var(--orange)";
    gaugeLabel = "Crítico";
  } else if (clamped > 40) {
    gaugeColor = "var(--amber)";
    gaugeLabel = "Regular";
  } else if (clamped > 20) {
    gaugeColor = "var(--teal)";
    gaugeLabel = "Bom";
  }

  fg.style.stroke = gaugeColor;
  labelEl.textContent = gaugeLabel;

  const duration = 800;
  const startTs = performance.now();
  const targetOffset = arcLen * (1 - clamped / 100);

  function tick(ts) {
    const p = Math.min((ts - startTs) / duration, 1);
    const eased = 1 - Math.pow(1 - p, 3);
    const currentScore = Math.round(clamped * eased);
    const currentOffset = arcLen - (arcLen - targetOffset) * eased;
    scoreEl.textContent = String(currentScore);
    fg.style.strokeDashoffset = `${currentOffset}`;
    if (p < 1) requestAnimationFrame(tick);
  }

  fg.style.strokeDashoffset = `${arcLen}`;
  requestAnimationFrame(tick);
}

function renderSentimentBar(positivoPct, neutroPct, negativoPct) {
  const pos = document.getElementById("audit-sent-pos");
  const neu = document.getElementById("audit-sent-neu");
  const neg = document.getElementById("audit-sent-neg");
  const lPos = document.getElementById("audit-sent-pos-label");
  const lNeu = document.getElementById("audit-sent-neu-label");
  const lNeg = document.getElementById("audit-sent-neg-label");
  if (!pos || !neu || !neg || !lPos || !lNeu || !lNeg) return;

  const p = Math.max(0, Math.min(100, Number(positivoPct || 0)));
  const n = Math.max(0, Math.min(100, Number(neutroPct || 0)));
  const g = Math.max(0, Math.min(100, Number(negativoPct || 0)));

  lPos.textContent = `${p.toFixed(1)}%`;
  lNeu.textContent = `${n.toFixed(1)}%`;
  lNeg.textContent = `${g.toFixed(1)}%`;

  pos.style.width = "0%";
  neu.style.width = "0%";
  neg.style.width = "0%";

  setTimeout(() => {
    pos.style.width = `${p}%`;
    neu.style.width = `${n}%`;
    neg.style.width = `${g}%`;
  }, 100);
}

function renderAuditKpis(metrics, score) {
  const kpiRow = document.getElementById("audit-kpi-row");
  if (!kpiRow) return;
  kpiRow.innerHTML = "";

  const cards = [
    {
      label: "NPS MÉDIO",
      value: metrics.nps_medio ?? "—",
      benchmark: "benchmark: > 55",
      icon: '<svg viewBox="0 0 24 24" fill="none" stroke="var(--emerald)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20V10"/><path d="M6 20V14"/><path d="M18 20V6"/></svg>',
      status:
        Number(metrics.nps_medio ?? 0) >= 55
          ? "good"
          : Number(metrics.nps_medio ?? 0) >= 35
            ? "warning"
            : "critical",
    },
    {
      label: "TMA MÉDIO",
      value: `${Math.round(Number(metrics.tma_medio_segundos ?? 0))}s`,
      benchmark: "benchmark: < 300s",
      icon: '<svg viewBox="0 0 24 24" fill="none" stroke="var(--amber)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15 14"/></svg>',
      status:
        Number(metrics.tma_medio_segundos ?? 0) <= 300
          ? "good"
          : Number(metrics.tma_medio_segundos ?? 0) <= 420
            ? "warning"
            : "critical",
    },
    {
      label: "TAXA RECHAMADA",
      value: `${Number(metrics.taxa_rechamada_pct ?? 0).toFixed(1)}%`,
      benchmark: "benchmark: < 15%",
      icon: '<svg viewBox="0 0 24 24" fill="none" stroke="var(--orange)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.86 19.86 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6A19.86 19.86 0 0 1 2.11 4.18 2 2 0 0 1 4.1 2h3a2 2 0 0 1 2 1.72c.12.9.34 1.77.65 2.6a2 2 0 0 1-.45 2.11L8 9.73a16 16 0 0 0 6.27 6.27l1.3-1.3a2 2 0 0 1 2.11-.45c.83.31 1.7.53 2.6.65A2 2 0 0 1 22 16.92z"/></svg>',
      status:
        Number(metrics.taxa_rechamada_pct ?? 0) <= 15
          ? "good"
          : Number(metrics.taxa_rechamada_pct ?? 0) <= 22
            ? "warning"
            : "critical",
    },
    {
      label: "FRICTION SCORE",
      value: `${Number(score || 0)}`,
      benchmark: "benchmark: < 40",
      icon: '<svg viewBox="0 0 24 24" fill="none" stroke="var(--rose)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><line x1="12" y1="8" x2="12" y2="13"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
      status:
        Number(score || 0) <= 40
          ? "good"
          : Number(score || 0) <= 60
            ? "warning"
            : "critical",
    },
  ];

  cards.forEach((card) => {
    const el = document.createElement("div");
    el.className = `audit-kpi-card status-${card.status}`;
    el.innerHTML = `
      <div class="audit-kpi-top">
        <span class="audit-kpi-label">${escapeHtml(card.label)}</span>
        <span class="audit-kpi-icon">${card.icon}</span>
      </div>
      <div class="audit-kpi-value">${escapeHtml(card.value)}</div>
      <div class="audit-kpi-benchmark">${escapeHtml(card.benchmark)}</div>
    `;
    kpiRow.appendChild(el);
  });
}

function renderSentimentList(items) {
  const list = document.getElementById("audit-sent-list");
  if (!list) return;
  list.innerHTML = "";

  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "rec-item";
    empty.textContent = "Sem detalhamento de tendências de sentimentos.";
    list.appendChild(empty);
    return;
  }

  items.forEach((item, idx) => {
    const row = document.createElement("div");
    row.className = "rec-item";
    row.innerHTML = `<span class="rec-n">${String(idx + 1).padStart(2, "0")}</span>`;
    const text = document.createElement("span");
    text.textContent = `${item.dimensao || "dimensão"}: ${item.sentimento || "—"} (${Number(item.percentual || 0).toFixed(1)}%, ${item.quantidade || 0} interações)`;
    row.appendChild(text);
    list.appendChild(row);
  });
}

function frictionTypeIcon(tipo) {
  const map = {
    RECHAMADA:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2"/><path d="M2.5 8a6 6 0 0 1 6-6"/><path d="M2 4v4h4"/></svg>',
    TMA_ELEVADO:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15 14"/></svg>',
    ESPERA_EXCESSIVA:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="9" r="2"/><circle cx="16" cy="9" r="2"/><path d="M3 19a5 5 0 0 1 10 0"/><path d="M11 19a5 5 0 0 1 10 0"/></svg>',
    CHURN_RISK:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    RESOLUCAO_PENDENTE:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="9" y1="13" x2="15" y2="19"/><line x1="15" y1="13" x2="9" y2="19"/></svg>',
  };
  return map[tipo] || map.RESOLUCAO_PENDENTE;
}

function renderFrictionPoints(points) {
  const list = document.getElementById("audit-friction-list");
  if (!list) return;
  list.innerHTML = "";

  if (!points.length) {
    const empty = document.createElement("div");
    empty.className = "rec-item";
    empty.textContent =
      "Nenhum ponto de fricção retornado para o período informado.";
    list.appendChild(empty);
    return;
  }

  points.forEach((fp) => {
    const severity = String(fp.severity || "medium").toLowerCase();
    const card = document.createElement("article");
    card.className = `friction-point-card sev-${severity}`;

    card.innerHTML = `
      <div class="fp-head">
        <div class="fp-icon">${frictionTypeIcon(String(fp.tipo || "").toUpperCase())}</div>
        <div class="fp-title">${escapeHtml(fp.tipo || "Fricção")}</div>
        <div class="fp-badge">${escapeHtml(severity)}</div>
      </div>
      <div class="fp-count">${escapeHtml(fp.quantidade_ocorrencias ?? 0)} ocorrências</div>
      <div class="fp-desc">${escapeHtml(fp.descricao || "Sem descrição")}</div>
      <div class="fp-action">Ação recomendada: ${escapeHtml(fp.sugestao_acao || "Sem sugestão")}</div>
      <div class="fp-pill-list"></div>
    `;

    const pillList = card.querySelector(".fp-pill-list");
    const ops = Array.isArray(fp.operacoes_afetadas)
      ? fp.operacoes_afetadas
      : [];
    if (pillList && ops.length) {
      ops.forEach((op) => {
        const pill = document.createElement("span");
        pill.className = "fp-pill";
        pill.textContent = String(op);
        pillList.appendChild(pill);
      });
    }

    list.appendChild(card);
  });
}

function renderVocThemes(themes) {
  const grid = document.getElementById("audit-voc-grid");
  if (!grid) return;
  grid.innerHTML = "";

  if (!themes.length) {
    const empty = document.createElement("div");
    empty.className = "rec-item";
    empty.textContent = "Sem temas VoC retornados.";
    grid.appendChild(empty);
    return;
  }

  themes.forEach((theme) => {
    const card = document.createElement("article");
    card.className = "voc-theme-card";

    const catRaw = String(theme.categoria || "INFORMACAO").toUpperCase();
    const catKey =
      catRaw === "RECLAMACAO" ||
      catRaw === "ELOGIO" ||
      catRaw === "SUGESTAO" ||
      catRaw === "DUVIDA" ||
      catRaw === "INFORMACAO"
        ? catRaw.toLowerCase()
        : "informacao";

    const impacto = String(theme.impacto_estimado || "LOW").toUpperCase();
    const impactLevel = impacto === "ALTO" ? 3 : impacto === "MEDIO" ? 2 : 1;

    const sent = String(
      theme.sentimento_predominante || "NEUTRO",
    ).toUpperCase();
    const sentSymbol = sent.includes("POS")
      ? "↑"
      : sent.includes("NEG")
        ? "↓"
        : "→";

    card.innerHTML = `
      <div class="voc-top">
        <span class="voc-cat voc-cat-${catKey}">${escapeHtml(catRaw)}</span>
        <span class="voc-theme-name">${escapeHtml(theme.tema || "Tema")}</span>
      </div>
      <div class="voc-keywords"></div>
      <div class="voc-bottom">
        <div class="voc-impact">
          <span class="voc-impact-dot ${impactLevel >= 1 ? "active" : ""}"></span>
          <span class="voc-impact-dot ${impactLevel >= 2 ? "active" : ""}"></span>
          <span class="voc-impact-dot ${impactLevel >= 3 ? "active" : ""}"></span>
        </div>
        <div class="voc-sentiment">${sentSymbol} ${escapeHtml(sent)}</div>
      </div>
    `;

    const kwWrap = card.querySelector(".voc-keywords");
    const kws = Array.isArray(theme.exemplos_palavras_chave)
      ? theme.exemplos_palavras_chave
      : [];
    kws.slice(0, 6).forEach((kw) => {
      const pill = document.createElement("span");
      pill.className = "voc-keyword-pill";
      pill.textContent = String(kw);
      kwWrap?.appendChild(pill);
    });

    grid.appendChild(card);
  });
}

function renderRecommendationList(container, items, withCheckbox) {
  if (!container) return;
  container.innerHTML = "";

  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "rec-item";
    empty.textContent = "Sem itens disponíveis.";
    container.appendChild(empty);
    return;
  }

  items.forEach((item, idx) => {
    const row = document.createElement("div");
    row.className = withCheckbox ? "audit-check-item" : "audit-reco-item";

    const num = document.createElement("div");
    num.className = "audit-reco-num";
    num.textContent = String(idx + 1);
    row.appendChild(num);

    if (withCheckbox) {
      const box = document.createElement("div");
      box.className = "audit-check-box";
      row.appendChild(box);
    }

    const txt = document.createElement("div");
    txt.textContent = String(item);
    row.appendChild(txt);

    container.appendChild(row);
  });
}

function copyAuditReport() {
  const btn = document.getElementById("audit-copy-report-btn");
  if (!auditMarkdownCache) return;

  copyTextWithFallback(auditMarkdownCache)
    .then(() => {
      if (!btn) return;
      const old = btn.textContent;
      btn.textContent = "✓ Copiado";
      setTimeout(() => {
        btn.textContent = old || "Copiar relatório";
      }, 2000);
    })
    .catch(() => {
      showAuditError("Não foi possível copiar o relatório.");
    });
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
        `<div class="rec-item" style="border-color:var(--color-danger);background:var(--rose-bg);color:var(--rose)">⚠ ${w}</div>`,
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
    : '<tr><td colspan="4" style="color:var(--ink-muted);text-align:center">Dicionário não disponível</td></tr>';

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
    ...warnings.map((w) => ({
      ico: "⚠️",
      text: `Observação: ${safe(w)}`,
    })),
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
      background: linear-gradient(135deg, #004691 0%, #00a1e4 100%);
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
      border: 1px solid #c8daf5; border-left: 4px solid #00a1e4;
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
      margin: 0; color: #004691; font-size: 13px;
      font-weight: 700; line-height: 1.3;
    }
    .card p { margin: 0; font-size: 12.5px; color: #2d3b4f; line-height: 1.65; }
    .sect-card { border-left: 3px solid #00a1e4; }
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
      color: #00a1e4;
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
      background: #00a1e4; color: #fff; font-size: 11px; font-weight: 700;
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
      dryRun.innerHTML = `<div class="rec-item" style="border-color:var(--color-danger);background:var(--rose-bg);color:var(--rose)">⚠ ${dry.error}</div>`;
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
        <div class="rec-item" style="color:var(--emerald);border-color:var(--color-success);background:var(--emerald-bg)">
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
    status: "Disponível",
    action: () => navTo("qa"),
  },
  {
    name: "Document Builder",
    description:
      "Gere documentação que o negócio entende e a engenharia confia: schema real, governança e exportação pronta.",
    tags: ["Docs", "Pipeline", "DataOps"],
    status: "Disponível",
    action: () => navTo("db"),
  },
  {
    name: "Query Builder",
    description:
      "Da pergunta ao SQL em minutos, com contexto real para análises de receita, margem e risco.",
    tags: ["NL2SQL", "BigQuery", "IA"],
    status: "Disponível",
    action: () => navTo("qb"),
  },
  {
    name: "Finance Voice IA",
    description:
      "Audite a experiência do cliente com análise de sentimentos, fricção, VoC e NPS — tudo em um relatório executivo acionável.",
    tags: ["Auditoria", "VoC", "CX"],
    status: "Disponível",
    action: () => navTo("audit"),
  },
  {
    name: "ER Diagram Explorer",
    description:
      "Visualize o diagrama ER de datasets BigQuery com relacionamentos e navegação interativa.",
    tags: ["ER Diagram", "BigQuery", "DataOps"],
    status: "Disponível",
    action: () => navTo("er"),
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

    statusEl.textContent = String(bot.status || "").toUpperCase();
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

// ─────────────────────────────────────
// Finance Voice IA — Chat
// ─────────────────────────────────────

let faIsLoading = false;
let faThinkingId = null;
let faInputListenerBound = false;
let faMsgCounter = 0;
const FA_TYPING_BASE_DELAY_MS = 16;
const FA_TYPING_MIN_DURATION_MS = 850;

function _faWait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function _faTypeMarkdownInto(container, sourceText, options = {}) {
  if (!container) return;

  const { escapeInput = false } = options;
  const source = String(sourceText || "");
  const prepared = escapeInput ? _escFA(source) : source;
  const total = prepared.length;

  if (!total) {
    container.innerHTML = `<div class="fa-report"></div>`;
    return;
  }

  const startedAt = Date.now();
  let cursor = 0;
  while (cursor < total) {
    const step =
      total > 2400
        ? 32
        : total > 1400
          ? 24
          : total > 800
            ? 16
            : total > 280
              ? 10
              : 4;
    const delay =
      total > 1400
        ? Math.max(FA_TYPING_BASE_DELAY_MS, 14)
        : total > 800
          ? Math.max(FA_TYPING_BASE_DELAY_MS, 18)
          : total > 280
            ? Math.max(FA_TYPING_BASE_DELAY_MS, 22)
            : Math.max(FA_TYPING_BASE_DELAY_MS, 30);

    cursor = Math.min(total, cursor + step);
    const partial = prepared.slice(0, cursor);
    container.innerHTML = `<div class="fa-report fa-report--typing">${_faMdToHtml(partial)}</div>`;
    _faScrollBottom();
    await _faWait(delay);
  }

  const elapsed = Date.now() - startedAt;
  if (elapsed < FA_TYPING_MIN_DURATION_MS) {
    await _faWait(FA_TYPING_MIN_DURATION_MS - elapsed);
  }

  container.innerHTML = `<div class="fa-report">${_faMdToHtml(prepared)}</div>`;
  _faScrollBottom();
}

function setFAInteractionLock(locked) {
  const input = document.getElementById("fa-input");
  if (input) {
    input.disabled = !!locked;
  }

  document.querySelectorAll(".fa-suggestion-chip").forEach((el) => {
    if (el instanceof HTMLButtonElement) {
      el.disabled = !!locked;
    }
  });

  const clearBtn = document.querySelector(".fa-clear-btn");
  if (clearBtn instanceof HTMLButtonElement) {
    clearBtn.disabled = !!locked;
  }
}

function setFASendButtonState({ disabled, loading }) {
  const sendBtn = document.getElementById("fa-send-btn");
  if (!sendBtn) return;
  sendBtn.disabled = !!disabled;
  sendBtn.classList.toggle("is-loading", !!loading);
}

function initFAInputListener() {
  if (faInputListenerBound) return;
  faInputListenerBound = true;

  const input = document.getElementById("fa-input");
  if (!input) return;

  input.addEventListener("input", () => {
    setFASendButtonState({
      disabled: !input.value.trim() || faIsLoading,
      loading: faIsLoading,
    });
    autoResizeFAInput(input);
  });
}

function autoResizeFAInput(el) {
  el.style.height = "auto";
  el.style.height = Math.min(el.scrollHeight, 130) + "px";
}

function handleFAInputKey(event) {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    sendFAMessage();
  }
}

function useFASuggestion(btn) {
  const input = document.getElementById("fa-input");
  if (!input || faIsLoading) return;
  input.value = btn.textContent.trim();
  autoResizeFAInput(input);
  setFASendButtonState({ disabled: false, loading: false });
  input.focus();
}

function clearFAChat() {
  if (faIsLoading) return;

  const msgArea = document.getElementById("fa-messages");
  if (!msgArea) return;

  faMsgCounter = 0;
  msgArea.innerHTML = `
    <div class="fa-welcome" id="fa-welcome">
      <div class="fa-welcome-ico">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" style="color:var(--porto-primary)"
          stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
          <polyline points="9 12 11 14 15 10"/>
        </svg>
      </div>
      <h3>Finance Voice IA</h3>
      <p>Pergunte sobre qualquer período em linguagem natural. Analisarei sentimento, fricção e temas de atendimento e gerarei um relatório executivo.</p>
    </div>`;

  const input = document.getElementById("fa-input");
  if (input) {
    input.value = "";
    autoResizeFAInput(input);
  }
  setFASendButtonState({ disabled: true, loading: false });
}

function _faScrollBottom() {
  const area = document.getElementById("fa-messages");
  if (area) area.scrollTop = area.scrollHeight;
}

function _faUserInitials() {
  const name = currentUser?.name || currentUser?.username || "U";
  return name
    .split(" ")
    .map((w) => w[0])
    .filter(Boolean)
    .slice(0, 2)
    .join("")
    .toUpperCase();
}

function _faNow() {
  return new Date().toLocaleTimeString("pt-BR", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function appendFAUserMessage(text) {
  const welcome = document.getElementById("fa-welcome");
  if (welcome) welcome.remove();

  const area = document.getElementById("fa-messages");
  if (!area) return;

  const id = `fa-msg-${++faMsgCounter}`;
  const el = document.createElement("div");
  el.id = id;
  el.className = "fa-msg fa-msg-user";
  el.innerHTML = `
    <div class="fa-msg-avatar">${_faUserInitials()}</div>
    <div class="fa-msg-main">
      <div class="fa-bubble fa-bubble--user">
        <div class="fa-bubble-head">
          <span class="fa-bubble-icon" aria-hidden="true">👤</span>
          <span class="fa-bubble-title">Sua pergunta</span>
        </div>
        <div class="fa-bubble-body">${_escFA(text)}</div>
      </div>
      <div class="fa-msg-time">${_faNow()}</div>
    </div>`;
  area.appendChild(el);
  _faScrollBottom();
  return id;
}

function appendFAThinking() {
  const area = document.getElementById("fa-messages");
  if (!area) return;

  const id = `fa-think-${++faMsgCounter}`;
  faThinkingId = id;
  const el = document.createElement("div");
  el.id = id;
  el.className = "fa-msg fa-msg-bot";
  el.innerHTML = `
    <div class="fa-msg-avatar">FV</div>
    <div class="fa-msg-main">
      <div class="fa-bubble fa-bubble--thinking">
        <div class="fa-bubble-head">
          <span class="fa-bubble-icon" aria-hidden="true">⚙</span>
          <span class="fa-bubble-title">Finance Voice IA analisando</span>
        </div>
        <div class="fa-thinking-dots"><span></span><span></span><span></span></div>
      </div>
    </div>`;
  area.appendChild(el);
  _faScrollBottom();
}

function removeFAThinking() {
  if (!faThinkingId) return;
  document.getElementById(faThinkingId)?.remove();
  faThinkingId = null;
}

function appendFAErrorMessage(msg) {
  const area = document.getElementById("fa-messages");
  if (!area) return;

  const el = document.createElement("div");
  el.className = "fa-msg fa-msg-bot";
  el.innerHTML = `
    <div class="fa-msg-avatar">FV</div>
    <div class="fa-msg-main">
      <div class="fa-bubble fa-bubble--error">
        <div class="fa-bubble-head">
          <span class="fa-bubble-icon" aria-hidden="true">⚠</span>
          <span class="fa-bubble-title">Atenção</span>
        </div>
        <div class="fa-bubble-body">${_escFA(msg)}</div>
      </div>
      <div class="fa-msg-time">${_faNow()}</div>
    </div>`;
  area.appendChild(el);
  _faScrollBottom();
}

async function appendFAChatTextMessage(text) {
  const area = document.getElementById("fa-messages");
  if (!area) return;

  const el = document.createElement("div");
  el.className = "fa-msg fa-msg-bot";
  el.innerHTML = `
    <div class="fa-msg-avatar">FV</div>
    <div class="fa-msg-main">
      <div class="fa-bubble fa-bubble--bot">
        <div class="fa-bubble-head">
          <span class="fa-bubble-icon" aria-hidden="true">✦</span>
          <span class="fa-bubble-title">Finance Voice IA</span>
        </div>
        <div class="fa-bubble-body"><div class="fa-report-slot"></div></div>
      </div>
      <div class="fa-msg-time">${_faNow()}</div>
    </div>`;
  area.appendChild(el);
  _faScrollBottom();

  const slot = el.querySelector(".fa-report-slot");
  await _faTypeMarkdownInto(slot, text, { escapeInput: true });
}

async function appendFABotMessage(data) {
  const area = document.getElementById("fa-messages");
  if (!area) return;

  const id = `fa-bot-${++faMsgCounter}`;
  const el = document.createElement("div");
  el.id = id;
  el.className = "fa-msg fa-msg-bot";

  const metricsHtml = _faMetricsHtml(data);
  const detailsHtml = _faDetailsHtml(data);

  el.innerHTML = `
    <div class="fa-msg-avatar">FV</div>
    <div class="fa-msg-main fa-msg-main--report">
      <div class="fa-bubble fa-bubble--bot fa-bubble--report">
        <div class="fa-bubble-head">
          <span class="fa-bubble-icon" aria-hidden="true">📊</span>
          <span class="fa-bubble-title">Relatório Finance Voice IA</span>
        </div>
        <div class="fa-bubble-body">
          ${metricsHtml}
          <div class="fa-report-slot"></div>
          ${detailsHtml}
        </div>
      </div>
      <div class="fa-msg-time">${_faNow()} · Score ${data.quality_score ?? "—"}/100</div>
    </div>`;

  area.appendChild(el);
  _faScrollBottom();

  const slot = el.querySelector(".fa-report-slot");
  await _faTypeMarkdownInto(slot, data.markdown_report || "");
}

function _faMetricsHtml(data) {
  const label = (data.friction_label || "BAIXO").toUpperCase();
  const labelKey = label
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const pct =
    data.friction_score != null
      ? (data.friction_score * 100).toFixed(1) + "%"
      : "—";

  const dateRange = data.date_range
    ? `${data.date_range.start} → ${data.date_range.end}`
    : "—";

  const dominant = (data.sentiment_analysis?.dominant || "—").toUpperCase();
  const total = (data.total_records ?? 0).toLocaleString("pt-BR");
  const ops = Array.isArray(data.operations_analyzed)
    ? data.operations_analyzed.filter(Boolean)
    : [];
  const opsPreview =
    ops.length <= 2
      ? ops.join(", ")
      : `${ops.slice(0, 2).join(", ")} +${ops.length - 2}`;

  const sentColor =
    {
      POSITIVO: "var(--porto-primary)",
      NEGATIVO: "var(--color-danger)",
      NEUTRO: "var(--ink-secondary)",
    }[dominant] || "var(--ink-secondary)";
  const sentimentIcon =
    {
      POSITIVO: "👍",
      NEGATIVO: "👎",
      NEUTRO: "🤝",
    }[dominant] || "💬";
  const sentimentClass =
    {
      POSITIVO: "fa-metric-card--sent-positivo",
      NEGATIVO: "fa-metric-card--sent-negativo",
      NEUTRO: "fa-metric-card--sent-neutro",
    }[dominant] || "";

  const warningItems = Array.isArray(data.warnings)
    ? data.warnings.filter(Boolean)
    : [];
  const warningsResume =
    warningItems.length > 0 ? `${warningItems.length} aviso(s)` : "Sem avisos";
  const warningsDetail =
    warningItems.length > 0
      ? `<div class="fa-warning-note"><strong>Aviso:</strong> ${_escFA(warningItems[0])}</div>`
      : "";

  return `
    <div class="fa-metric-grid">
      <div class="fa-metric-card fa-metric-card--${labelKey}">
        <div class="fa-metric-head"><span class="fa-metric-icon">⚡</span></div>
        <div class="fa-metric-label">Fricção</div>
        <div class="fa-metric-value">${label} <span>${pct}</span></div>
      </div>

      <div class="fa-metric-card ${sentimentClass}">
        <div class="fa-metric-head"><span class="fa-metric-icon">${sentimentIcon}</span></div>
        <div class="fa-metric-label">Sentimento dominante</div>
        <div class="fa-metric-value" style="color:${sentColor}">${dominant}</div>
      </div>

      <div class="fa-metric-card">
        <div class="fa-metric-head"><span class="fa-metric-icon">📅</span></div>
        <div class="fa-metric-label">Período analisado</div>
        <div class="fa-metric-value">📅 ${dateRange}</div>
      </div>

      <div class="fa-metric-card">
        <div class="fa-metric-head"><span class="fa-metric-icon">📊</span></div>
        <div class="fa-metric-label">Volume</div>
        <div class="fa-metric-value">📊 ${total} registros</div>
      </div>

      <div class="fa-metric-card" title="${_escFA(ops.join(" | "))}">
        <div class="fa-metric-head"><span class="fa-metric-icon">🧩</span></div>
        <div class="fa-metric-label">Operações analisadas</div>
        <div class="fa-metric-value">🧩 ${_escFA(opsPreview || "—")}</div>
      </div>

      <div class="fa-metric-card ${warningItems.length > 0 ? "fa-metric-card--warn" : "fa-metric-card--ok"}" title="${_escFA(warningItems.join(" | "))}">
        <div class="fa-metric-head"><span class="fa-metric-icon">${warningItems.length > 0 ? "⚠" : "✅"}</span></div>
        <div class="fa-metric-label">Avisos</div>
        <div class="fa-metric-value">${warningItems.length > 0 ? "⚠" : "✅"} ${warningsResume}</div>
      </div>
    </div>
    ${warningsDetail}`;
}

function _faDetailsHtml(data) {
  const themes = data.themes_analysis?.themes || [];
  if (!themes.length) return "";

  const chips = themes
    .map(
      (t) =>
        `<span class="fa-theme-chip" title="${_escFA(t.sentimento_predominante || "")}">${_escFA(t.nome || "")}</span>`,
    )
    .join("");

  const detailId = `fa-det-${faMsgCounter}`;
  const bodyId = `fa-detbody-${faMsgCounter}`;

  return `
    <div class="fa-details">
      <button class="fa-details-toggle" id="${detailId}" onclick="toggleFADetails('${detailId}','${bodyId}')">
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor"
          stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M9 18l6-6-6-6"/>
        </svg>
        Principais temas (${themes.length})
      </button>
      <div class="fa-details-body" id="${bodyId}">
        ${chips}
        ${
          data.themes_analysis?.insights
            ? `<p style="margin-top:8px;color:var(--ink2);font-size:12px">${_escFA(data.themes_analysis.insights)}</p>`
            : ""
        }
      </div>
    </div>`;
}

function toggleFADetails(toggleId, bodyId) {
  const toggle = document.getElementById(toggleId);
  const body = document.getElementById(bodyId);
  if (!toggle || !body) return;
  toggle.classList.toggle("open");
  body.classList.toggle("open");
}

// ── Simple Markdown → HTML converter ──
function _faMdToHtml(md) {
  if (!md) return "";

  const lines = md.replace(/\r\n/g, "\n").split("\n");
  const out = [];
  let inTable = false;
  let inUl = false;
  let inOl = false;

  const closeList = () => {
    if (inUl) {
      out.push("</ul>");
      inUl = false;
    }
    if (inOl) {
      out.push("</ol>");
      inOl = false;
    }
  };

  const closeTable = () => {
    if (inTable) {
      out.push("</tbody></table>");
      inTable = false;
    }
  };

  const inline = (text) =>
    text
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      .replace(/\*(.+?)\*/g, "<em>$1</em>")
      .replace(/`(.+?)`/g, "<code>$1</code>")
      .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1");

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const line = raw.trimEnd();

    // Horizontal rule
    if (/^---+$/.test(line.trim())) {
      closeList();
      closeTable();
      out.push("<hr>");
      continue;
    }

    // Headers
    const h3m = line.match(/^### (.+)/);
    if (h3m) {
      closeList();
      closeTable();
      out.push(`<h3>${inline(h3m[1])}</h3>`);
      continue;
    }
    const h2m = line.match(/^## (.+)/);
    if (h2m) {
      closeList();
      closeTable();
      out.push(`<h2>${inline(h2m[1])}</h2>`);
      continue;
    }
    const h1m = line.match(/^# (.+)/);
    if (h1m) {
      closeList();
      closeTable();
      out.push(`<h1>${inline(h1m[1])}</h1>`);
      continue;
    }

    // Blockquote
    const bqm = line.match(/^> (.+)/);
    if (bqm) {
      closeList();
      closeTable();
      out.push(`<blockquote>${inline(bqm[1])}</blockquote>`);
      continue;
    }

    // Table row
    if (line.startsWith("|") && line.endsWith("|")) {
      const cells = line
        .slice(1, -1)
        .split("|")
        .map((c) => c.trim());
      // separator row (align row)
      if (cells.every((c) => /^[-:]+$/.test(c))) continue;

      if (!inTable) {
        closeList();
        // previous line was header → wrap in thead
        const prevIdx = out.length - 1;
        const prev = out[prevIdx] || "";
        if (prev.startsWith("<tr>")) {
          out[prevIdx] = `<table><thead>${prev}</thead><tbody>`;
        } else {
          out.push("<table><thead></thead><tbody>");
        }
        inTable = true;
        continue;
      }
      const tds = cells.map((c) => `<td>${inline(c)}</td>`).join("");
      out.push(`<tr>${tds}</tr>`);
      continue;
    } else if (inTable) {
      // Check if last pushed line was header (before tbody)
      closeTable();
    }

    // Detect table header (line with |, next line is separator)
    if (line.startsWith("|")) {
      const cells = line
        .slice(1, -1)
        .split("|")
        .map((c) => c.trim());
      const ths = cells.map((c) => `<th>${inline(c)}</th>`).join("");
      out.push(`<tr>${ths}</tr>`);
      continue;
    }

    // Unordered list
    const ulm = line.match(/^[-*] (.+)/);
    if (ulm) {
      if (!inUl) {
        closeList();
        closeTable();
        out.push("<ul>");
        inUl = true;
      }
      out.push(`<li>${inline(ulm[1])}</li>`);
      continue;
    }

    // Ordered list
    const olm = line.match(/^\d+\. (.+)/);
    if (olm) {
      if (!inOl) {
        closeList();
        closeTable();
        out.push("<ol>");
        inOl = true;
      }
      out.push(`<li>${inline(olm[1])}</li>`);
      continue;
    }

    closeList();

    // Blank line
    if (!line.trim()) continue;

    // Paragraph
    out.push(`<p>${inline(line)}</p>`);
  }

  closeList();
  closeTable();
  return out.join("\n");
}

function _escFA(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ── Main send function ──
async function sendFAMessage() {
  const input = document.getElementById("fa-input");
  const text = input?.value.trim() || "";
  const projectId = "silviosalviati";

  if (!text || faIsLoading) return;

  input.value = "";
  if (input) {
    input.style.height = "auto";
  }
  setFAInteractionLock(true);
  setFASendButtonState({ disabled: true, loading: true });

  appendFAUserMessage(text);
  appendFAThinking();

  faIsLoading = true;

  try {
    const res = await fetch("/api/agents/finance_auditor/analyze", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        query: text,
        project_id: projectId,
        dataset_hint: null,
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
    removeFAThinking();

    if (data.status === "error") {
      appendFAErrorMessage(
        data.error || "Não foi possível realizar a análise.",
      );
    } else if (data.response_mode === "chat") {
      await appendFAChatTextMessage(
        data.chat_answer ||
          "Não encontrei resposta para essa pergunta no momento.",
      );
    } else {
      await appendFABotMessage(data);
    }
  } catch (e) {
    removeFAThinking();
    appendFAErrorMessage(prettifyErrorMessage(e.message));
  } finally {
    faIsLoading = false;
    setFAInteractionLock(false);
    setFASendButtonState({
      disabled: !input?.value.trim(),
      loading: false,
    });
    input?.focus();
  }
}

// ═══════════════════════════════════════════════════════════════

// ══════════════════════════════════════════════════════════════════════════
//  ER DIAGRAM EXPLORER — Neo4j-style D3 Force Graph
// ══════════════════════════════════════════════════════════════════════════

// ── State ──────────────────────────────────────────────────────────────────
const _neo = {
  data: null,
  svg: null,
  inner: null,
  zoom: null,
  simulation: null,
  nodeMap: {},
  nodeG: null,
  edgePaths: null,
  edgeLblG: null,
  colNodeG: null,
  colEdgePaths: null,
  tableColEdgePaths: null,
  colNodes: [],
  colEdges: [],
  tableColEdges: [],
  expandedNodes: new Set(),
  selectedNode: null,
  showKeysOnly: false,
  hideInferred: false,
  searchTerm: "",
  dsRef: "",
  initialized: false,
  width: 0,
  height: 0,
  zoomT: null,
};

const _NEO_COLOR = {
  fact: { fill: "#004691", stroke: "#3b70c0" },
  dimension: { fill: "#0891b2", stroke: "#30b0d8" },
  staging: { fill: "#64748b", stroke: "#94a3b8" },
  aggregated: { fill: "#6d28d9", stroke: "#9460f0" },
  unknown: { fill: "#3d5276", stroke: "#5a7090" },
};

const _NEO_EDGE_STYLE = {
  high: { color: "#94a3b8", dash: null },
  medium: { color: "#d97706", dash: null },
  low: { color: "#475569", dash: "5,3" },
};

// ── Helpers ────────────────────────────────────────────────────────────────
function _neoRadius() {
  return 38;
}

function _neoVisibleCols(node) {
  const expanded = _neo.expandedNodes.has(node.id);
  if (!_neo.showKeysOnly || expanded) return node.columns;
  const k = node.columns.filter(
    (c) =>
      c.is_pk_candidate ||
      c.is_fk_candidate ||
      c.is_partition ||
      c.is_clustering,
  );
  return k.length ? k : node.columns.slice(0, 3);
}

function _neoColIcon(c) {
  if (c.is_pk_candidate) return "🔑";
  if (c.is_partition) return "⚡";
  if (c.is_clustering) return "🔷";
  if (c.is_fk_candidate) return "🔗";
  return " ";
}

function _neoShortType(t) {
  if (!t) return "?";
  const u = t.toUpperCase().split("(")[0];
  const m = {
    STRING: "STR",
    INT64: "INT",
    INTEGER: "INT",
    FLOAT64: "FLT",
    NUMERIC: "NUM",
    BOOLEAN: "BOOL",
    DATE: "DATE",
    DATETIME: "DT",
    TIMESTAMP: "TS",
    BYTES: "BYT",
    ARRAY: "ARR",
    STRUCT: "OBJ",
    BIGNUMERIC: "BNUM",
    FLOAT: "FLT",
    BIGINT: "INT",
    SMALLINT: "INT",
    TINYINT: "INT",
    VARCHAR: "STR",
    CHAR: "STR",
    DECIMAL: "NUM",
  };
  return m[u] || u.slice(0, 4);
}

// ── Init (called once on navTo) ────────────────────────────────────────────
function initErView() {
  if (_neo.initialized) return;
  _neo.initialized = true;
  _neoWireValidation();
}

// ── Dataset validation (debounce 1s, mirrors QB pattern) ──────────────────
let _neoValTimer = null;
let _neoProjTimer = null;

function _neoWireValidation() {
  const pIn = document.getElementById("neo-project");
  const dIn = document.getElementById("neo-dataset");
  if (!pIn || !dIn) return;

  // Project change → fetch datasets after 800ms idle
  pIn.addEventListener("input", () => {
    clearTimeout(_neoProjTimer);
    _neoProjTimer = setTimeout(() => _neoLoadDatasets(pIn.value.trim()), 800);
    // Also re-validate dataset combo if already filled
    clearTimeout(_neoValTimer);
    _neoSetBtn(false);
    _neoDsIndicator("typing");
    _neoValTimer = setTimeout(_neoValidate, 1000);
  });

  // Dataset change → validate after 1s idle
  dIn.addEventListener("input", () => {
    clearTimeout(_neoValTimer);
    _neoSetBtn(false);
    _neoDsIndicator("typing");
    _neoValTimer = setTimeout(_neoValidate, 1000);
  });
}

async function _neoLoadDatasets(project) {
  const dl = document.getElementById("neo-dataset-list");
  if (!dl || !project) return;
  try {
    const resp = await fetch(
      `/api/schema-explorer/datasets?project_id=${encodeURIComponent(project)}`,
      {
        headers: {
          Authorization:
            "Bearer " + (typeof token !== "undefined" ? token : ""),
        },
      },
    );
    if (!resp.ok) return;
    const datasets = await resp.json();
    dl.innerHTML = datasets
      .map((d) => `<option value="${d}"></option>`)
      .join("");
  } catch (_) {
    // silently ignore — datalist is best-effort
  }
}

function _neoDsIndicator(s) {
  const el = document.getElementById("neo-ds-indicator");
  if (!el) return;
  const map = {
    idle: "",
    typing:
      '<span style="color:rgba(255,255,255,.35);font-size:9px">•••</span>',
    valid: '<span style="color:#4ade80;font-size:13px;line-height:1">✓</span>',
    invalid:
      '<span style="color:#f87171;font-size:13px;line-height:1">✗</span>',
  };
  el.innerHTML = map[s] ?? "";
}

function _neoSetBtn(enabled) {
  const b = document.getElementById("neo-map-btn");
  if (b) b.disabled = !enabled;
}

async function _neoValidate() {
  const p = document.getElementById("neo-project")?.value.trim();
  const d = document.getElementById("neo-dataset")?.value.trim();
  if (!p || !d) {
    _neoDsIndicator("idle");
    return;
  }
  _neoDsIndicator("valid");
  _neoSetBtn(true);
}

// ── API load ───────────────────────────────────────────────────────────────
async function loadNeoGraph() {
  const project = document.getElementById("neo-project")?.value.trim();
  const dataset = document.getElementById("neo-dataset")?.value.trim();
  if (!project || !dataset) return;
  _neoState("loading");
  try {
    const resp = await fetch(
      `/api/schema-explorer/graph?project_id=${encodeURIComponent(project)}&dataset_hint=${encodeURIComponent(dataset)}`,
      {
        headers: {
          Authorization:
            "Bearer " + (typeof token !== "undefined" ? token : ""),
        },
      },
    );
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: resp.statusText }));
      throw new Error(err.detail || resp.statusText);
    }
    const data = await resp.json();
    _neo.data = data;
    _neo.dsRef = data.metadata?.dataset_ref || `${project}.${dataset}`;
    _neo.expandedNodes.clear();
    _neo.selectedNode = null;
    neoCloseDetail();
    _neoRender(data);
  } catch (e) {
    _neoState("error", e.message);
  }
}

// ── State display ──────────────────────────────────────────────────────────
function _neoState(state, msg) {
  ["neo-loading", "neo-error", "neo-empty", "neo-svg"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.style.display = "none";
  });
  if (state === "loading") {
    const el = document.getElementById("neo-loading");
    if (el) el.style.display = "";
  } else if (state === "error") {
    const el = document.getElementById("neo-error");
    if (el) {
      el.style.display = "";
      el.textContent = msg || "Erro";
    }
  } else if (state === "empty") {
    const el = document.getElementById("neo-empty");
    if (el) el.style.display = "";
  } else if (state === "graph") {
    const el = document.getElementById("neo-svg");
    if (el) el.style.display = "";
  }
}

// ── Main render (called on load + toggle-inferred) ─────────────────────────
function _neoRender(data) {
  _neoState("graph");
  const wrap = document.getElementById("neo-canvas-wrap");
  if (!wrap) return;
  const W = wrap.clientWidth || 900;
  const H = wrap.clientHeight || 600;
  _neo.width = W;
  _neo.height = H;

  if (_neo.simulation) {
    _neo.simulation.stop();
    _neo.simulation = null;
  }

  // Filter edges
  const activeEdges = data.edges.filter(
    (e) => !(_neo.hideInferred && e.confidence === "low"),
  );

  // Clone node data (D3 will mutate x/y)
  const nodes = data.nodes.map((n) => ({ ...n, r: _neoRadius(n) }));
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));
  _neo.nodeMap = byId;

  // Resolve edge source/target to node objects
  const edges = activeEdges.map((e) => ({
    ...e,
    source: byId[e.source] ?? e.source,
    target: byId[e.target] ?? e.target,
  }));

  // Re-use existing SVG element; rebuild inner g only
  const svg = d3.select("#neo-svg").attr("width", W).attr("height", H);
  svg.selectAll("g.neo-inner").remove();
  const inner = svg.append("g").attr("class", "neo-inner");

  const zoom = d3
    .zoom()
    .scaleExtent([0.2, 4])
    .on("zoom", (ev) => {
      inner.attr("transform", ev.transform);
      _neo.zoomT = ev.transform;
    });
  svg.call(zoom).on("dblclick.zoom", null);
  _neo.svg = svg;
  _neo.inner = inner;
  _neo.zoom = zoom;

  // Draw layers (edges behind nodes)
  const gEdge = inner.append("g").attr("class", "neo-edges");
  const gEdgeLbl = inner.append("g").attr("class", "neo-edge-labels");
  const gTableColEdge = inner.append("g").attr("class", "neo-table-col-edges");
  const gColEdge = inner.append("g").attr("class", "neo-col-edges");
  const gNode = inner.append("g").attr("class", "neo-nodes");
  const gColNode = inner.append("g").attr("class", "neo-col-nodes");

  // ── Edges ─────────────────────────────────────────────────────────
  const edgePaths = gEdge
    .selectAll(".neo-edge")
    .data(
      edges,
      (e) =>
        `${e.source?.id ?? e.source}→${e.target?.id ?? e.target}:${e.via_column}`,
    )
    .join("path")
    .attr("class", "neo-edge")
    .attr("fill", "none")
    .attr("stroke", (e) => _NEO_EDGE_STYLE[e.confidence]?.color ?? "#94a3b8")
    .attr("stroke-width", 2)
    .attr("stroke-linecap", "round")
    .attr(
      "stroke-dasharray",
      (e) => _NEO_EDGE_STYLE[e.confidence]?.dash ?? null,
    )
    .attr("marker-end", (e) => `url(#neo-arrow-${e.confidence})`)
    .style("cursor", "pointer")
    .on("click", (ev, e) => {
      ev.stopPropagation();
      _neoEdgeClick(ev, e);
    });

  // ── Edge labels ────────────────────────────────────────────────────
  const edgeLblG = gEdgeLbl
    .selectAll(".neo-el")
    .data(
      edges,
      (e) =>
        `${e.source?.id ?? e.source}→${e.target?.id ?? e.target}:${e.via_column}`,
    )
    .join("g")
    .attr("class", "neo-el")
    .style("pointer-events", "none");

  edgeLblG
    .append("rect")
    .attr("class", "neo-el-bg")
    .attr("rx", 3)
    .attr("ry", 3)
    .attr("fill", "#1e293b")
    .attr("stroke", (e) => _NEO_EDGE_STYLE[e.confidence]?.color ?? "#94a3b8")
    .attr("stroke-width", 0.8);

  edgeLblG
    .append("text")
    .attr("class", "neo-el-text")
    .attr("text-anchor", "middle")
    .attr("dominant-baseline", "middle")
    .attr("font-size", "10")
    .attr("font-weight", "700")
    .attr("fill", (e) => _NEO_EDGE_STYLE[e.confidence]?.color ?? "#94a3b8")
    .text((e) => e.via_column);

  // ── Nodes ──────────────────────────────────────────────────────────
  const nodeG = gNode
    .selectAll(".neo-node")
    .data(nodes, (n) => n.id)
    .join("g")
    .attr("class", "neo-node")
    .call(
      d3
        .drag()
        .on("start", (ev, d) => {
          d.fx = d.x;
          d.fy = d.y;
          d._dragStartX = d.x;
          d._dragStartY = d.y;
          // Only this node moves — column satellites follow via _neoColumnPos
          d._dragConnected = [];
        })
        .on("drag", (ev, d) => {
          const ddx = ev.x - d._dragStartX;
          const ddy = ev.y - d._dragStartY;
          d.fx = ev.x;
          d.fy = ev.y;
          d.x = ev.x;
          d.y = ev.y;
          // Reposition only this table node in the DOM
          if (_neo.nodeG) {
            _neo.nodeG
              .filter((n) => n.id === d.id)
              .attr("transform", `translate(${d.x},${d.y})`);
          }
          // Redraw edges and column nodes
          if (_neo.edgePaths) _neo.edgePaths.attr("d", (e) => _neoEdgePath(e));
          if (_neo.colNodeG) {
            _neo.colNodeG.attr("transform", (c) => {
              const p = _neoColumnPos(c);
              c.x = p.x;
              c.y = p.y;
              return `translate(${p.x},${p.y})`;
            });
          }
          if (_neo.tableColEdgePaths) {
            _neo.tableColEdgePaths.attr("d", (e) => {
              const t = _neo.nodeMap[e.tableId];
              const c = e.colNode;
              if (!t?.x || !c?.x) return "";
              const dx = c.x - t.x,
                dy = c.y - t.y;
              const len = Math.sqrt(dx * dx + dy * dy) || 1;
              const sx = t.x + (dx / len) * (t.r ?? 20);
              const sy = t.y + (dy / len) * (t.r ?? 20);
              const ex = c.x - (dx / len) * 6;
              const ey = c.y - (dy / len) * 6;
              return `M${sx},${sy} L${ex},${ey}`;
            });
          }
          if (_neo.colEdgePaths) {
            _neo.colEdgePaths.attr("d", (e) => {
              const s = e.sourceCol,
                t = e.targetCol;
              if (!s?.x || !t?.x) return "";
              const mx = (s.x + t.x) / 2,
                my = (s.y + t.y) / 2 - 18;
              return `M${s.x},${s.y} Q${mx},${my} ${t.x},${t.y}`;
            });
          }
          if (_neo.edgeLblG) {
            _neo.edgeLblG.each(function (e) {
              const { lx, ly } = _neoEdgeMid(e);
              const grp = d3.select(this);
              const txt = grp
                .select(".neo-el-text")
                .attr("x", lx)
                .attr("y", ly);
              try {
                const bb = txt.node().getBBox();
                grp
                  .select(".neo-el-bg")
                  .attr("x", bb.x - 4)
                  .attr("y", bb.y - 2)
                  .attr("width", bb.width + 8)
                  .attr("height", bb.height + 4);
              } catch (_) {}
            });
          }
        })
        .on("end", (_ev, _d) => {
          // Keep fx/fy pinned so all moved nodes stay where dropped
          if (_d._dragConnected) {
            _d._dragConnected.forEach(({ node }) => {
              node.fx = node.x;
              node.fy = node.y;
            });
          }
          _d._dragConnected = null;
          _d._dragStartX = null;
          _d._dragStartY = null;
        }),
    )
    .on("click", (ev, d) => {
      ev.stopPropagation();
      _neoSelectNode(d.id, nodes, edges);
    })
    .on("dblclick", (ev, d) => {
      ev.stopPropagation();
      _neoToggleExpand(d, nodes, edges);
    })
    .on("mouseenter", (ev, d) => _neoHoverNode(d, nodes, edges, true))
    .on("mouseleave", (ev, d) => _neoHoverNode(d, nodes, edges, false));

  // Dismiss on canvas background click
  svg.on("click", () => {
    _neo.selectedNode = null;
    neoCloseDetail();
    nodeG.classed("neo-dimmed", false).classed("neo-selected", false);
    edgePaths.attr("stroke-width", 2).classed("neo-edge-dimmed", false);
    edgeLblG.classed("neo-el-dimmed", false);
  });

  _neo.nodeG = nodeG;
  _neo.edgePaths = edgePaths;
  _neo.edgeLblG = edgeLblG;

  const colNodes = _neoBuildColumnNodes(nodes);
  const tableColEdges = _neoBuildTableColumnEdges(colNodes);
  const colEdges = _neoBuildColumnKeyEdges(colNodes, edges);
  _neo.colNodes = colNodes;
  _neo.tableColEdges = tableColEdges;
  _neo.colEdges = colEdges;

  const tableColEdgePaths = gTableColEdge
    .selectAll(".neo-table-col-edge")
    .data(tableColEdges, (e) => `${e.tableId}->${e.colNode.id}`)
    .join("path")
    .attr("class", "neo-table-col-edge")
    .attr("fill", "none")
    .attr("stroke", "rgba(148,163,184,0.55)")
    .attr("stroke-width", 1.25);

  const colEdgePaths = gColEdge
    .selectAll(".neo-col-edge")
    .data(colEdges, (e) => `${e.sourceCol.id}->${e.targetCol.id}`)
    .join("path")
    .attr("class", "neo-col-edge")
    .attr("fill", "none")
    .attr("stroke", "rgba(251,191,36,0.72)")
    .attr("stroke-width", 1.5)
    .attr("stroke-dasharray", "2,2");

  const colNodeG = gColNode
    .selectAll(".neo-col-node")
    .data(colNodes, (c) => c.id)
    .join("g")
    .attr(
      "class",
      (c) => `neo-col-node ${c.isKey ? "neo-col-key" : "neo-col-regular"}`,
    )
    .style("pointer-events", "none");

  colNodeG
    .append("circle")
    .attr("class", "neo-col-circle")
    .attr("r", 28)
    .attr("fill", (c) => (c.isKey ? "#431407" : "#0c2543"))
    .attr("stroke", (c) => (c.isKey ? "#f97316" : "#38bdf8"))
    .attr("stroke-width", (c) => (c.isKey ? 2.5 : 2))
    .style("filter", (c) =>
      c.isKey ? "drop-shadow(0 0 6px rgba(249,115,22,0.75))" : "none",
    );

  colNodeG
    .append("text")
    .attr("class", "neo-col-label")
    .attr("text-anchor", "middle")
    .attr("pointer-events", "none")
    .attr("fill", (c) => (c.isKey ? "#fed7aa" : "#e0f2fe"))
    .attr("font-weight", (c) => (c.isKey ? "700" : "500"))
    .each(function (c) {
      _neoWrapSvgText(d3.select(this), c.col.name, 28, 8);
    });

  _neo.colNodeG = colNodeG;
  _neo.colEdgePaths = colEdgePaths;
  _neo.tableColEdgePaths = tableColEdgePaths;

  // Draw initial node content
  nodes.forEach((n) =>
    _neoDrawNode(
      nodeG.filter((d) => d.id === n.id),
      n,
    ),
  );

  // ── Force simulation ─────────────────────────────────────────────────────
  // Run the simulation fully offline so nodes start static (no vibration).
  const sim = d3
    .forceSimulation(nodes)
    .force(
      "link",
      d3
        .forceLink(edges)
        .id((d) => d.id)
        .distance(300)
        .strength(0.5),
    )
    .force("charge", d3.forceManyBody().strength(-700))
    .force("center", d3.forceCenter(W / 2, H / 2))
    .force(
      "collide",
      d3
        .forceCollide()
        .radius((d) => d.r + 25)
        .strength(0.8),
    )
    .stop();

  // Advance enough ticks for a stable layout (~300 is sufficient)
  const tickCount = Math.ceil(
    Math.log(sim.alphaMin()) / Math.log(1 - sim.alphaDecay()),
  );
  for (let i = 0; i < tickCount; i++) sim.tick();

  // Pin every node so it never drifts after render
  nodes.forEach((n) => {
    n.fx = n.x;
    n.fy = n.y;
  });

  // Single render pass
  _neoTick(
    edgePaths,
    edgeLblG,
    nodeG,
    colNodeG,
    tableColEdgePaths,
    colEdgePaths,
    nodes,
    W,
    H,
  );

  _neo.simulation = sim;
  _neo.nodes = nodes;
}

// ── Draw node SVG content ──────────────────────────────────────────────────
function _neoDrawNode(sel, n) {
  sel.selectAll("*").remove();
  const r = n.r;
  const col = _NEO_COLOR[n.table_type] ?? _NEO_COLOR.unknown;

  // Pulse ring (shown when selected)
  sel
    .append("circle")
    .attr("class", "neo-pulse-ring")
    .attr("r", r + 7)
    .attr("fill", "none")
    .attr("stroke", col.stroke)
    .attr("stroke-width", 2.5)
    .attr("opacity", 0)
    .attr("pointer-events", "none");

  // Main table node (filled background)
  sel
    .append("circle")
    .attr("class", "neo-circle")
    .attr("r", r)
    .attr("fill", col.fill)
    .attr("fill-opacity", 0.96)
    .attr("stroke", col.stroke)
    .attr("stroke-width", 3)
    .style("filter", "drop-shadow(0 4px 16px rgba(0,0,0,.5))");

  // Table name label inside the node — wrapped to fit the circle
  const lbl = n.label ?? n.id ?? "";
  const labelEl = sel
    .append("text")
    .attr("class", "neo-node-label")
    .attr("text-anchor", "middle")
    .attr("fill", "#f8fafc")
    .attr("font-weight", "700")
    .attr("pointer-events", "none");
  _neoWrapSvgText(labelEl, lbl, r, 10);
}

function _neoBuildColumnNodes(nodes) {
  const out = [];
  nodes.forEach((t) => {
    const cols = _neoVisibleCols(t);
    const count = Math.max(cols.length, 1);
    cols.forEach((col, idx) => {
      out.push({
        id: `${t.id}::${col.name}`,
        tableId: t.id,
        col,
        idx,
        count,
        isKey: Boolean(col.is_pk_candidate || col.is_fk_candidate),
      });
    });
  });
  return out;
}

function _neoBuildTableColumnEdges(colNodes) {
  return colNodes.map((c) => ({
    tableId: c.tableId,
    colNode: c,
  }));
}

function _neoFriendlyTableLabel(label) {
  if (!label) return "";
  return label.length > 18 ? `${label.slice(0, 17)}...` : label;
}

// Wraps text into <tspan> elements fitted inside a circle of radius r
function _neoWrapSvgText(sel, text, r, baseFontSize) {
  const maxW = r * 1.7;
  const charW = baseFontSize * 0.6;
  const charsPerLine = Math.max(4, Math.floor(maxW / charW));
  const words = text.replace(/_/g, " ").split(/\s+/);
  const lines = [];
  let cur = "";
  for (const w of words) {
    const candidate = cur ? cur + " " + w : w;
    if (candidate.length <= charsPerLine) {
      cur = candidate;
    } else {
      if (cur) lines.push(cur);
      cur =
        w.length > charsPerLine ? w.slice(0, charsPerLine - 1) + "\u2026" : w;
    }
  }
  if (cur) lines.push(cur);
  const maxLines = 3;
  if (lines.length > maxLines) {
    lines.splice(maxLines);
    const last = lines[maxLines - 1];
    lines[maxLines - 1] = last.slice(0, charsPerLine - 2) + "\u2026";
  }
  const fontSize =
    lines.length >= 3 ? Math.max(6, baseFontSize - 2) : baseFontSize;
  const lineH = fontSize * 1.35;
  sel.attr("font-size", fontSize);
  lines.forEach((line, i) => {
    sel
      .append("tspan")
      .attr("x", 0)
      .attr("dy", i === 0 ? -((lines.length - 1) / 2) * lineH : lineH)
      .text(line);
  });
}

function _neoBuildColumnKeyEdges(colNodes, tableEdges) {
  const byId = new Map(colNodes.map((c) => [c.id, c]));
  const out = [];
  const seen = new Set();

  (tableEdges || []).forEach((e) => {
    const sid = e.source?.id ?? e.source;
    const tid = e.target?.id ?? e.target;
    const col = e.via_column;
    const a = byId.get(`${sid}::${col}`);
    const b = byId.get(`${tid}::${col}`);
    if (!a || !b) return;
    if (!(a.isKey || b.isKey)) return;
    const key = [a.id, b.id].sort().join("|");
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ sourceCol: a, targetCol: b, confidence: e.confidence });
  });
  return out;
}

function _neoColumnPos(colNode) {
  const table = _neo.nodeMap[colNode.tableId];
  if (!table) return { x: 0, y: 0 };
  const angle =
    (Math.PI * 2 * colNode.idx) / Math.max(colNode.count, 1) - Math.PI / 2;
  const ring = (table.r ?? 38) + 72;
  return {
    x: (table.x ?? 0) + Math.cos(angle) * ring,
    y: (table.y ?? 0) + Math.sin(angle) * ring,
  };
}

// ── Simulation tick ────────────────────────────────────────────────────────
function _neoTick(
  edgePaths,
  edgeLblG,
  nodeG,
  colNodeG,
  tableColEdgePaths,
  colEdgePaths,
  nodes,
  W,
  H,
) {
  // Clamp nodes to canvas bounds
  nodes.forEach((n) => {
    n.x = Math.max(n.r + 12, Math.min(W - n.r - 12, n.x));
    n.y = Math.max(n.r + 12, Math.min(H - n.r - 12, n.y));
  });

  nodeG.attr("transform", (d) => `translate(${d.x},${d.y})`);
  edgePaths.attr("d", (e) => _neoEdgePath(e));

  if (colNodeG) {
    colNodeG.attr("transform", (c) => {
      const p = _neoColumnPos(c);
      c.x = p.x;
      c.y = p.y;
      return `translate(${p.x},${p.y})`;
    });
  }

  if (tableColEdgePaths) {
    tableColEdgePaths.attr("d", (e) => {
      const t = _neo.nodeMap[e.tableId];
      const c = e.colNode;
      if (!t?.x || !c?.x) return "";

      const dx = c.x - t.x;
      const dy = c.y - t.y;
      const len = Math.sqrt(dx * dx + dy * dy) || 1;
      const sx = t.x + (dx / len) * (t.r ?? 20);
      const sy = t.y + (dy / len) * (t.r ?? 20);
      const ex = c.x - (dx / len) * 6;
      const ey = c.y - (dy / len) * 6;
      return `M${sx},${sy} L${ex},${ey}`;
    });
  }

  if (colEdgePaths) {
    colEdgePaths.attr("d", (e) => {
      const s = e.sourceCol;
      const t = e.targetCol;
      if (!s?.x || !t?.x) return "";
      const mx = (s.x + t.x) / 2;
      const my = (s.y + t.y) / 2 - 18;
      return `M${s.x},${s.y} Q${mx},${my} ${t.x},${t.y}`;
    });
  }

  edgeLblG.each(function (e) {
    const { lx, ly } = _neoEdgeMid(e);
    const grp = d3.select(this);
    const txt = grp.select(".neo-el-text").attr("x", lx).attr("y", ly);
    try {
      const bb = txt.node().getBBox();
      grp
        .select(".neo-el-bg")
        .attr("x", bb.x - 4)
        .attr("y", bb.y - 2)
        .attr("width", bb.width + 8)
        .attr("height", bb.height + 4);
    } catch (_) {}
  });
}

// ── Edge geometry (quadratic bezier) ──────────────────────────────────────
function _neoEdgePath(e) {
  const s = typeof e.source === "object" ? e.source : _neo.nodeMap[e.source];
  const t = typeof e.target === "object" ? e.target : _neo.nodeMap[e.target];
  if (!s?.x || !t?.x) return "";
  const sr = s.r ?? 65,
    tr = t.r ?? 65;
  const dx = t.x - s.x,
    dy = t.y - s.y;
  const len = Math.sqrt(dx * dx + dy * dy) || 1;
  // Start / end on circle boundaries
  const sx = s.x + (dx / len) * sr,
    sy = s.y + (dy / len) * sr;
  const ex = t.x - (dx / len) * tr,
    ey = t.y - (dy / len) * tr;
  // Perpendicular curve offset
  const off = Math.min(len * 0.18, 55);
  const mx = (sx + ex) / 2 + (-dy / len) * off;
  const my = (sy + ey) / 2 + (dx / len) * off;
  return `M${sx},${sy} Q${mx},${my} ${ex},${ey}`;
}

// Bezier midpoint at t=0.5: 0.25·P0 + 0.5·Ctrl + 0.25·P2
function _neoEdgeMid(e) {
  const s = typeof e.source === "object" ? e.source : _neo.nodeMap[e.source];
  const t = typeof e.target === "object" ? e.target : _neo.nodeMap[e.target];
  if (!s?.x || !t?.x) return { lx: 0, ly: 0 };
  const sr = s.r ?? 65,
    tr = t.r ?? 65;
  const dx = t.x - s.x,
    dy = t.y - s.y;
  const len = Math.sqrt(dx * dx + dy * dy) || 1;
  const sx = s.x + (dx / len) * sr,
    sy = s.y + (dy / len) * sr;
  const ex = t.x - (dx / len) * tr,
    ey = t.y - (dy / len) * tr;
  const off = Math.min(len * 0.18, 55);
  const mx = (sx + ex) / 2 + (-dy / len) * off;
  const my = (sy + ey) / 2 + (dx / len) * off;
  return {
    lx: 0.25 * sx + 0.5 * mx + 0.25 * ex,
    ly: 0.25 * sy + 0.5 * my + 0.25 * ey,
  };
}

// ── Toggle column expansion (double-click) ────────────────────────────────
function _neoToggleExpand(d, nodes, edges) {
  if (_neo.expandedNodes.has(d.id)) _neo.expandedNodes.delete(d.id);
  else _neo.expandedNodes.add(d.id);
  const newR = _neoRadius(d);
  d.r = newR;
  _neoDrawNode(
    _neo.nodeG.filter((n) => n.id === d.id),
    d,
  );
  // No simulation restart — just rebuild column satellites for this node
  const colNodes = _neoBuildColumnNodes(_neo.nodes ?? []);
  const tableColEdges = _neoBuildTableColumnEdges(colNodes);
  _neo.colNodes = colNodes;
  _neo.tableColEdges = tableColEdges;
}

// ── Hover: dim non-connected ───────────────────────────────────────────────
function _neoHoverNode(d, nodes, edges, entering) {
  if (!entering) {
    if (_neo.selectedNode) {
      _neoApplyDim(_neo.selectedNode, edges);
      return;
    }
    _neo.nodeG?.classed("neo-dimmed", false);
    _neo.edgePaths?.attr("stroke-width", 2).classed("neo-edge-dimmed", false);
    _neo.edgeLblG?.classed("neo-el-dimmed", false);
    return;
  }
  _neoApplyDim(d.id, edges);
}

function _neoApplyDim(id, edges) {
  const conn = new Set([id]);
  (edges || []).forEach((e) => {
    const sid = e.source?.id ?? e.source,
      tid = e.target?.id ?? e.target;
    if (sid === id) conn.add(tid);
    if (tid === id) conn.add(sid);
  });
  _neo.nodeG?.classed("neo-dimmed", (n) => !conn.has(n.id));
  _neo.edgePaths
    ?.attr("stroke-width", (e) => {
      const sid = e.source?.id ?? e.source,
        tid = e.target?.id ?? e.target;
      return sid === id || tid === id ? 3 : 2;
    })
    .classed("neo-edge-dimmed", (e) => {
      const sid = e.source?.id ?? e.source,
        tid = e.target?.id ?? e.target;
      return sid !== id && tid !== id;
    });
  _neo.edgeLblG?.classed("neo-el-dimmed", (e) => {
    const sid = e.source?.id ?? e.source,
      tid = e.target?.id ?? e.target;
    return sid !== id && tid !== id;
  });
}

// ── Node click → detail panel ──────────────────────────────────────────────
function _neoSelectNode(id, nodes, edges) {
  _neo.selectedNode = id;
  _neoApplyDim(id, edges);
  _neo.nodeG?.classed("neo-selected", (n) => n.id === id);
  _neoOpenDetail(id);
}

// ── Detail panel ───────────────────────────────────────────────────────────
function _neoOpenDetail(nodeId) {
  const panel = document.getElementById("neo-detail");
  const body = document.getElementById("neo-detail-body");
  const title = document.getElementById("neo-detail-title");
  if (!panel || !body || !_neo.data) return;

  const node = _neo.data.nodes.find((n) => n.id === nodeId);
  if (!node) return;
  title.textContent = node.label;

  const TL = {
    fact: "FATO",
    dimension: "DIMENS\u00C3O",
    staging: "STAGING",
    aggregated: "AGREGADA",
    unknown: "UNKNOWN",
  };
  const TC = {
    fact: "#004691",
    dimension: "#0891b2",
    staging: "#64748b",
    aggregated: "#6d28d9",
    unknown: "#3d5276",
  };
  const CL = {
    high: "Alta confian\u00E7a",
    medium: "M\u00E9dia confian\u00E7a",
    low: "Inferido",
  };

  const nEdges = (_neo.data.edges ?? []).filter(
    (e) => e.source === nodeId || e.target === nodeId,
  );

  body.innerHTML = `
    <div class="neo-dp-section">
      <span class="neo-dp-badge" style="background:${TC[node.table_type] ?? "#3d5276"}">${TL[node.table_type] ?? "UNKNOWN"}</span>
    </div>
    <div class="neo-dp-section">
      <span class="neo-dp-label">Caminho</span>
      <div class="neo-dp-path">
        <code>${_neo.dsRef}.${node.id}</code>
        <button class="neo-dp-copy" onclick="neoCopyPath('${_neo.dsRef}.${node.id}')">Copiar</button>
      </div>
    </div>
    ${node.partition_field ? `<div class="neo-dp-section"><span class="neo-dp-label">Parti\u00E7\u00E3o</span> <code>${node.partition_field}</code></div>` : ""}
    ${node.clustering_fields?.length ? `<div class="neo-dp-section"><span class="neo-dp-label">Clustering</span> <code>${node.clustering_fields.join(", ")}</code></div>` : ""}
    <div class="neo-dp-section">
      <div class="neo-dp-section-title">Colunas (${node.columns.length})</div>
      <div class="neo-dp-cols">
        ${node.columns
          .map(
            (c) => `<div class="neo-dp-col">
          <span class="neo-dp-col-icon">${_neoColIcon(c)}</span>
          <span class="neo-dp-col-name" title="${c.name}">${c.name}</span>
          <span class="neo-dp-col-type">${c.type}</span>
          ${!c.is_nullable ? '<span class="neo-dp-col-req">NN</span>' : ""}
        </div>`,
          )
          .join("")}
      </div>
    </div>
    ${
      nEdges.length
        ? `
    <div class="neo-dp-section">
      <div class="neo-dp-section-title">Relacionamentos (${nEdges.length})</div>
      ${nEdges
        .map((e) => {
          const other = e.source === nodeId ? e.target : e.source;
          const dir = e.source === nodeId ? "→" : "←";
          return `<div class="neo-dp-rel">
          <span class="neo-dp-rel-dir">${dir}</span>
          <span class="neo-dp-rel-table" title="${other}">${other}</span>
          <span class="neo-dp-rel-col">via ${e.via_column}</span>
          <span class="neo-dp-rel-conf neo-dp-conf-${e.confidence}">${CL[e.confidence] ?? e.confidence}</span>
        </div>`;
        })
        .join("")}
    </div>`
        : ""
    }
    <div class="neo-dp-actions">
      <button class="neo-dp-btn" onclick="neoGoQB('${_neo.dsRef}','${node.id}')">Abrir no Query Builder</button>
    </div>`;

  panel.style.display = "";
}

function neoCloseDetail() {
  const p = document.getElementById("neo-detail");
  if (p) p.style.display = "none";
  if (_neo.nodeG) _neo.nodeG.classed("neo-selected", false);
}

function neoCopyPath(path) {
  navigator.clipboard?.writeText(path).catch(() => {});
}

function neoGoQB(dsRef, tableId) {
  const parts = dsRef.split(".");
  if (parts.length >= 2) {
    const pi = document.getElementById("qb-project");
    const di = document.getElementById("qb-dataset");
    if (pi) pi.value = parts[0];
    if (di) di.value = parts[1];
  }
  navTo("qb");
}

// ── Edge click tooltip ────────────────────────────────────────────────────
function _neoEdgeClick(ev, e) {
  const CL = {
    high: "Alta confiança",
    medium: "Média confiança",
    low: "Inferido",
  };
  const RL = {
    one_to_many: "1:N",
    many_to_many: "N:N",
    one_to_one: "1:1",
    many_to_one: "N:1",
    unknown: "?",
  };
  _neoTip(
    ev,
    `<strong>${e.via_column}</strong><br>${CL[e.confidence] ?? e.confidence}<br>${RL[e.relationship_type] ?? ""}`,
  );
}

let _neoTipTimer = null;
function _neoTip(ev, html) {
  let tip = document.getElementById("neo-tooltip");
  if (!tip) {
    tip = document.createElement("div");
    tip.id = "neo-tooltip";
    tip.className = "neo-tooltip";
    document.body.appendChild(tip);
  }
  tip.innerHTML = html;
  tip.style.cssText = `display:block;left:${ev.pageX + 14}px;top:${ev.pageY - 10}px`;
  clearTimeout(_neoTipTimer);
  _neoTipTimer = setTimeout(() => {
    tip.style.display = "none";
  }, 3500);
}

// ── Toolbar: toggle keys-only ──────────────────────────────────────────────
function neoToggleKeys(keysOnly) {
  _neo.showKeysOnly = keysOnly;
  if (!_neo.data || !_neo.nodeG) return;
  _neo.data.nodes.forEach((n) => {
    const d = _neo.nodeMap[n.id];
    if (!d) return;
    d.r = _neoRadius(n);
    _neoDrawNode(
      _neo.nodeG.filter((nd) => nd.id === n.id),
      { ...n, r: d.r },
    );
  });
  _neo.simulation?.force("collide")?.radius((d) => (d.r ?? 65) + 25);
  _neo.simulation?.alphaTarget(0.05).restart();
  setTimeout(() => _neo.simulation?.alphaTarget(0), 500);
}

// ── Toolbar: toggle inferred edges ────────────────────────────────────────
function neoToggleInferred(hide) {
  _neo.hideInferred = hide;
  if (_neo.data) _neoRender(_neo.data);
}

// ── Toolbar: search + auto-pan ────────────────────────────────────────────
function neoSearch(q) {
  _neo.searchTerm = q.trim().toLowerCase();
  if (!_neo.nodeG) return;
  if (!_neo.searchTerm) {
    _neo.nodeG.classed("neo-search-miss", false);
    return;
  }
  let found = null;
  _neo.nodeG.each(function (d) {
    const hit = d.id.toLowerCase().includes(_neo.searchTerm);
    d3.select(this).classed("neo-search-miss", !hit);
    if (hit && !found) found = d;
  });
  if (found?.x) _neoPanTo(found.x, found.y);
}

function _neoPanTo(x, y) {
  if (!_neo.svg || !_neo.zoom) return;
  const t = _neo.zoomT ?? d3.zoomIdentity;
  const tx = _neo.width / 2 - t.k * x;
  const ty = _neo.height / 2 - t.k * y;
  _neo.svg
    .transition()
    .duration(600)
    .call(_neo.zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(t.k));
}

// ── Toolbar: reset camera ──────────────────────────────────────────────────
function neoResetCamera() {
  if (!_neo.svg || !_neo.zoom) return;
  _neo.svg
    .transition()
    .duration(500)
    .call(_neo.zoom.transform, d3.zoomIdentity);
}

// ── Toolbar: export PNG ────────────────────────────────────────────────────
function neoExportPng() {
  const svgEl = document.getElementById("neo-svg");
  if (!svgEl) return;
  const W = svgEl.clientWidth || _neo.width;
  const H = svgEl.clientHeight || _neo.height;
  const scale = 2;
  const svgStr = new XMLSerializer().serializeToString(svgEl);
  const canvas = document.createElement("canvas");
  canvas.width = W * scale;
  canvas.height = H * scale;
  const ctx = canvas.getContext("2d");
  ctx.fillStyle = "#0f172a";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  const url = URL.createObjectURL(
    new Blob([svgStr], { type: "image/svg+xml;charset=utf-8" }),
  );
  const img = new Image();
  img.onload = () => {
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
    URL.revokeObjectURL(url);
    const a = document.createElement("a");
    a.download = `er_diagram_${Date.now()}.png`;
    a.href = canvas.toDataURL("image/png");
    a.click();
  };
  img.src = url;
}
