// ─────────────────────────────────────
// App state
// ─────────────────────────────────────
let token = null;
let currentUser = null;
let qaDatasetValidationTimer = null;
let qaIsLoading = false;
let qaAnalyzeInFlight = false;
let _qaHitlThreadId = null;
let _qaLastResult = null; // { query, data } — cache do último resultado
const qaDatasetValidationState = {
  status: "idle",
  datasetHint: "",
  projectId: "",
  queryText: "",
};
let qbDatasetValidationTimer = null;
let qbIsLoading = false;
let _qbHitlThreadId = null;
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
  if (v == null) return "—";
  const n = Number(v);
  // A maioria das consultas processa MB, não TB — a $5/TB o custo real cai
  // abaixo da 4ª casa decimal e "toFixed(4)" mostraria sempre "0.0000",
  // parecendo grátis/quebrado mesmo quando o valor está correto.
  if (n > 0 && n < 0.0001) return "< USD 0.0001";
  return "USD " + n.toFixed(4);
}

function fmtPct(v) {
  if (v == null) return "—";
  const pct = Number(v) * 100;
  return (pct < 0.1 && pct > 0 ? "<0.1" : pct.toFixed(1)) + "%";
}

function authHeaders() {
  return {
    "Content-Type": "application/json",
    Authorization: "Bearer " + token,
  };
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
  const msgLower = msg.toLowerCase();

  if (msg.includes("Project ID")) {
    return "Informe um Project ID válido do GCP.";
  }

  if (
    msgLower.includes("default credentials were not found") ||
    msgLower.includes("set-up-adc") ||
    msgLower.includes("google.auth.exceptions.defaultcredentialserror")
  ) {
    return "Nao foi possivel autenticar no servico de IA (LLM). Configure ADC/Google credentials no ambiente.";
  }

  if (
    (msgLower.includes("credenciais") || msgLower.includes("credentials")) &&
    (msgLower.includes("bigquery") || msgLower.includes("dataset"))
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
  fill.classList.remove("qa-progress-indeterminate");
}

let _qaIndeterminateTimer = null;
function startQAIndeterminateFallback(stepText) {
  if (_qaIndeterminateTimer) clearTimeout(_qaIndeterminateTimer);
  _qaIndeterminateTimer = setTimeout(() => {
    const fill = document.getElementById("qa-progress-fill");
    const step = document.getElementById("qa-progress-step");
    if (fill) fill.classList.add("qa-progress-indeterminate");
    if (step && stepText) step.textContent = stepText;
  }, 3500);
}

function clearQAIndeterminateFallback() {
  if (_qaIndeterminateTimer) {
    clearTimeout(_qaIndeterminateTimer);
    _qaIndeterminateTimer = null;
  }
  const fill = document.getElementById("qa-progress-fill");
  if (fill) fill.classList.remove("qa-progress-indeterminate");
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

// ── Painel central de geração SQL ──────────────────────────────────────────
// Substitui a mini-barra no dock durante runQueryBuild(). Aparece no centro
// da área de conteúdo — mesmo lugar onde o resultado vai surgir — com fase
// descritiva, subtexto e contador de tempo ao vivo.

let _qbGenTimer = null;
let _qbGenSeconds = 0;

const _QB_GEN_PHASES = {
  validating: { title: "Validando entrada",    sub: "Verificando esquema e dataset" },
  generating: { title: "Gerando SQL com IA",   sub: "O LLM está interpretando sua solicitação" },
  dryrun:     { title: "Executando dry-run",   sub: "Testando a query no BigQuery sem custo real" },
  reviewing:  { title: "Revisando resultado",  sub: "Checando padrões e otimizações" },
};

function showQBGenerating() {
  const container = document.getElementById("qb-generating");
  const qbEmpty = document.getElementById("qb-empty");
  const qbTabsArea = document.getElementById("qb-tabs-area");
  const hitl = document.getElementById("qb-hitl-panel");
  const learning = document.getElementById("qb-gerencia-learning");

  if (qbEmpty) qbEmpty.style.display = "none";
  if (qbTabsArea) qbTabsArea.style.display = "none";
  if (hitl) hitl.style.display = "none";
  if (learning) learning.style.display = "none";

  if (!container) return;

  _qbGenSeconds = 0;
  const phase = _QB_GEN_PHASES.validating;

  container.innerHTML = `
    <div class="qa-empty" style="height: 100%">
      <div class="qa-empty-ico">${_QB_ICON_SVG}</div>
      <h3 id="qb-gen-phase">${phase.title}<span class="fa-thinking-dots"><span></span><span></span><span></span></span></h3>
      <p class="qb-gen-sub" id="qb-gen-sub">${phase.sub}</p>
      <span class="qb-gen-timer" id="qb-gen-timer">00:00</span>
    </div>`;

  container.style.display = "flex";
  container.style.flexDirection = "column";
  container.style.alignItems = "center";
  container.style.justifyContent = "center";

  if (_qbGenTimer) clearInterval(_qbGenTimer);
  _qbGenTimer = setInterval(() => {
    _qbGenSeconds++;
    const mm = String(Math.floor(_qbGenSeconds / 60)).padStart(2, "0");
    const ss = String(_qbGenSeconds % 60).padStart(2, "0");
    const timerEl = document.getElementById("qb-gen-timer");
    if (timerEl) timerEl.textContent = `${mm}:${ss}`;
  }, 1000);
}

function setGeneratingPhase(phase) {
  const info = _QB_GEN_PHASES[phase];
  if (!info) return;
  const phaseEl = document.getElementById("qb-gen-phase");
  const subEl = document.getElementById("qb-gen-sub");
  if (phaseEl) phaseEl.innerHTML = `${info.title}<span class="fa-thinking-dots"><span></span><span></span><span></span></span>`;
  if (subEl) subEl.textContent = info.sub;
}

function hideQBGenerating() {
  if (_qbGenTimer) {
    clearInterval(_qbGenTimer);
    _qbGenTimer = null;
  }
  const container = document.getElementById("qb-generating");
  if (!container) return;
  container.style.display = "none";
  container.innerHTML = "";
}

// ───────────────────────────────────────────────────────────────────────────

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
  const box = document.getElementById("qa-ctx-indicator");
  const titleEl = document.getElementById("qa-ctx-title");
  const msgEl = document.getElementById("qa-ctx-message");
  const iconChecking = document.getElementById("qa-ctx-icon-checking");
  const iconOk = document.getElementById("qa-ctx-icon-ok");
  const iconError = document.getElementById("qa-ctx-icon-error");

  if (!box) { syncQAAnalyzeButtonState(); return; }

  [iconChecking, iconOk, iconError].forEach(ic => { if (ic) ic.style.display = "none"; });

  if (kind === "idle") {
    box.style.display = "none";
  } else if (kind === "checking") {
    box.style.display = "flex";
    box.className = "qa-ctx-box qa-ctx-checking";
    if (iconChecking) iconChecking.style.display = "block";
    if (titleEl) titleEl.textContent = payload.title || "Validando contexto…";
    if (msgEl) msgEl.textContent = payload.message || "Detectando dataset e tabelas na query…";
  } else if (kind === "ok") {
    box.style.display = "flex";
    box.className = "qa-ctx-box qa-ctx-ok";
    if (iconOk) iconOk.style.display = "block";
    if (titleEl) titleEl.textContent = payload.title || "Contexto válido";
    if (msgEl) msgEl.textContent = payload.message || "Query validada. Já pode analisar.";
  } else if (kind === "error") {
    box.style.display = "flex";
    box.className = "qa-ctx-box qa-ctx-error";
    if (iconError) iconError.style.display = "block";
    if (titleEl) titleEl.textContent = payload.title || "Contexto inválido";
    if (msgEl) msgEl.textContent = payload.message || "Não foi possível validar dataset e tabelas.";
  }

  syncQAAnalyzeButtonState();
}

async function validateQAQueryContext() {
  const query = document.getElementById("qa-query")?.value.trim() || "";
  const currentProject = qaDatasetValidationState.projectId || "";

  qaDatasetValidationState.queryText = query;

  if (!query) {
    qaDatasetValidationState.status = "idle";
    qaDatasetValidationState.projectId = "";
    qaDatasetValidationState.datasetHint = "";
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
        message: "Sessão expirada. Faça login novamente.",
      };
    }

    const payload = await res.json();
    if (!res.ok) {
      throw new Error(payload?.detail || "Falha na validação da query.");
    }

    const currentQuery =
      document.getElementById("qa-query")?.value.trim() || "";
    if (currentQuery !== querySnapshot) {
      return {
        valid: false,
        projectId: "",
        datasetHint: "",
        message: "A query foi alterada durante a validação. Tente novamente.",
      };
    }

    const detectedProject = (payload.project_id || "").trim();
    const detectedDataset = (
      payload.dataset_hint ||
      payload.dataset_id ||
      ""
    ).trim();

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
        title: "Contexto não validado",
        message:
          payload.message ||
          "Não foi possível validar dataset e tabelas da query.",
      });
      return {
        valid: false,
        projectId: detectedProject,
        datasetHint: detectedDataset,
        message:
          payload.message ||
          "Não foi possível validar dataset e tabelas da query.",
      };
    }
  } catch (err) {
    qaDatasetValidationState.status = "invalid";
    setQADatasetValidationStatus("error", {
      title: "Falha na validação",
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
  const requestText = document.getElementById("qb-request")?.value.trim() || "";

  if (!btn) return;

  let blockedByDataset = !dataset;
  if (dataset) {
    blockedByDataset =
      qbDatasetValidationState.status !== "valid" ||
      qbDatasetValidationState.datasetHint !== dataset ||
      qbDatasetValidationState.projectId !== projectId;
  }

  btn.disabled = qbIsLoading || blockedByDataset || !requestText;
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
          : "Validação pendente");
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
      throw new Error(payload?.detail || "Falha na validação do dataset.");
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
        title: "Dataset não validado",
        message:
          payload.message || "Dataset não validado para uso no Query Builder.",
      });
    }
  } catch (err) {
    qbDatasetValidationState.status = "invalid";
    setQBDatasetValidationStatus("error", {
      title: "Falha na validação",
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
  ["tab-optimized", "tab-applied", "tab-recs"].forEach((id) => {
    document.getElementById(id)?.classList.remove("has-data");
  });
}

function resetQAResultPanels() {
  const hitlPanel = document.getElementById("qa-hitl-panel");
  if (hitlPanel) hitlPanel.style.display = "none";
  _qaHitlThreadId = null;

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
      is_admin: !!data.is_admin,
      gerencia: data.gerencia || "",
    };

    setUserUI(data.name, data.username);
    const adminNav = document.getElementById("admin-nav-section");
    if (adminNav) adminNav.style.display = currentUser.is_admin ? "" : "none";
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

  // Limpar dados persistentes
  localStorage.clear();

  // Recarrega a página em vez de só trocar de tela — cada agente acumula
  // estado em variáveis de módulo (qbDatasetValidationState, _qbPickerResolved,
  // qaDatasetValidationState, etc.) que não eram resetadas no logout, então o
  // próximo usuário a logar no mesmo navegador via SPA herdava a sessão
  // (dataset resolvido, SQL gerada, gerência) do usuário anterior. Recarregar
  // reexecuta os scripts do zero e elimina essa classe inteira de vazamento
  // de estado entre sessões, sem precisar resetar cada variável manualmente.
  window.location.reload();
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
    "admin-users": "view-admin-users",
    "admin-config": "view-admin-config",
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
  } else if (view === "audit") {
    document.getElementById("nav-audit")?.classList.add("active");
    initFAInputListener();
    initFASuggestions();
  } else if (view === "qb") {
    document.getElementById("nav-qb")?.classList.add("active");
    if (!_qbPickerResolved && qbDatasetValidationState.status !== "valid") {
      if (currentUser?.is_admin) {
        _qbShowGerenciaPicker(_QB_GERENCIA_TOPICS);
      } else {
        const topic = _qbFindGerenciaTopic(currentUser?.gerencia);
        if (topic) {
          _qbShowGerenciaPicker([topic]);
        } else {
          _qbShowNoGerenciaGuidance();
        }
      }
    }
  } else if (view === "er") {
    document.getElementById("nav-er")?.classList.add("active");
    initErView();
    _loadProjectsIntoSelect("neo-project", () => {
      const project = document.getElementById("neo-project")?.value.trim();
      if (project) _loadDatasetsIntoSelect(project, "neo-dataset");
    });
  } else if (view === "admin-users") {
    document.getElementById("nav-admin-users")?.classList.add("active");
    adminLoadUsers();
  } else if (view === "admin-config") {
    document.getElementById("nav-admin-config")?.classList.add("active");
    adminLoadConfig();
  }
}

function _setQBGerenciaMode(on) {
  const configLabel = document.getElementById("qb-config-label");
  const projectField = document.getElementById("qb-project-field");
  const datasetField = document.getElementById("qb-dataset-field");
  // Usuário já tem a gerência fixada (mostrada na mensagem do QB e no
  // perfil, no canto inferior esquerdo) — repetir aqui é redundante, então
  // some a seção "Configuração" inteira, não só os campos de projeto/dataset.
  if (configLabel) configLabel.style.display = on ? "none" : "block";
  if (projectField) projectField.style.display = on ? "none" : "flex";
  if (datasetField) datasetField.style.display = on ? "none" : "flex";
}

// Identidade visual própria do Query Builder (QB) — ícone <> e cartão
// centralizado no mesmo estilo de qb-empty, em vez do balão de chat do
// Finance Voice (avatar "FV" não fazia sentido aqui: quem fala é o QB).
const _QB_ICON_SVG = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="var(--porto)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>`;

// Mesmas gerências/ícones do seletor do Finance Voice (initFASuggestions)
// — lista fixa, reaproveitada aqui para o ponto de entrada do QB ter o
// mesmo layout e dinâmica (cartão com ícone, clique inicia o fluxo).
const _QB_GERENCIA_TOPICS = [
  {
    label: "Contas a pagar",
    gerencia: "contas_a_pagar",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"></rect><path d="M7 8h10"></path><path d="M7 12h10"></path><path d="M7 16h6"></path></svg>`,
  },
  {
    label: "Contas a receber",
    gerencia: "contas_receber",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="6" width="18" height="12" rx="2"></rect><path d="M3 10h18"></path><path d="M8 14h3"></path><path d="M15 14h1"></path></svg>`,
  },
  {
    label: "Experiência do cliente",
    gerencia: "experiencia_cliente",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 21s-6.5-4.35-9-8.13C1.24 10.3 2.26 6.5 5.8 5.37c2.03-.65 4.18.03 5.2 1.64 1.02-1.61 3.17-2.29 5.2-1.64 3.54 1.13 4.56 4.93 2.8 7.5C18.5 16.65 12 21 12 21z"></path></svg>`,
  },
  {
    label: "Cobrança",
    gerencia: "cobranca",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1v22"></path><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path></svg>`,
  },
  {
    label: "Fluxo de Caixa",
    gerencia: "fluxo_caixa",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 17l6-6 4 4 7-7"></path><path d="M14 8h6v6"></path></svg>`,
  },
];

let _qbPickerResolved = false;

function _qbFindGerenciaTopic(gerencia) {
  const key = String(gerencia || "").trim().toLowerCase();
  if (!key) return null;
  return _QB_GERENCIA_TOPICS.find((t) => t.gerencia === key) || null;
}

function _qbShowGerenciaPicker(topics) {
  const picker = document.getElementById("qb-gerencia-picker");
  const headEl = document.getElementById("qb-gerencia-picker-head");
  const titleEl = document.getElementById("qb-gerencia-picker-title");
  const hintEl = document.getElementById("qb-gerencia-picker-hint");
  const list = document.getElementById("qb-gerencia-picker-list");
  const pillEl = document.getElementById("qb-area-pill");
  if (!picker || !list) return;

  // Mesma estrutura de duas colunas do Finance Voice (fa-sidebar + área
  // principal) — os cartões entram na coluna de configuração, no lugar dos
  // campos de projeto/dataset, e o painel da direita segue com o estado
  // ocioso normal (sem duplicar mensagem de boas-vindas).
  _setQBGerenciaMode(true);

  // Modo seleção: cabeçalho + grade de cartões visíveis, pill de área
  // resolvida escondida — usado tanto na entrada inicial quanto ao reabrir
  // via "Trocar área" (_qbReopenGerenciaPicker).
  if (headEl) headEl.style.display = "";
  list.style.display = "";
  list.classList.add("fa-topic-grid");
  if (pillEl) pillEl.style.display = "none";

  // Pré-carrega projetos em segundo plano — se o usuário escolher "Outros
  // Assuntos Financeiro", o seletor manual já aparece com a lista pronta.
  if (!document.getElementById("qb-project")?.options.length ||
      document.getElementById("qb-project")?.options[0]?.value === "") {
    _loadProjectsIntoSelect("qb-project");
  }

  const single = topics.length === 1 ? topics[0] : null;
  if (titleEl) {
    titleEl.textContent = single
      ? `Pronto para criar consultas sobre ${_qbCapitalize(single.label)}`
      : "Sobre qual área você quer criar consultas?";
  }
  if (hintEl) {
    hintEl.textContent = single
      ? `Confirme abaixo e o Query Builder prepara sugestões de consulta sobre ${_qbCapitalize(single.label)} para você.`
      : "Escolha uma área abaixo e o Query Builder já prepara sugestões de consulta para você.";
  }

  picker.style.display = "block";
  list.innerHTML = "";
  topics.forEach((topic) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "fa-topic-card";
    btn.setAttribute("aria-label", topic.label);
    btn.innerHTML = `
      <span class="fa-topic-icon" aria-hidden="true">${topic.icon}</span>
      <span class="fa-topic-label">${topic.label}</span>
    `;
    btn.onclick = () => {
      _qbPickerResolved = true;
      // Trava os cartões durante a resolução assíncrona (evita clique
      // duplo numa área diferente enquanto a primeira ainda carrega). Some
      // só depois que _resolveQBGerencia confirmar sucesso (vira o pill via
      // _qbShowAreaPill) — se a resolução falhar, a grade continua visível
      // (já destravada) pra tentar de novo, em vez de deixar a tela sem
      // nenhuma opção.
      setFAInteractionLock(true);
      _resolveQBGerencia(topic.gerencia);
    };
    list.appendChild(btn);
  });
}

// Troca a grade de seleção por um resumo compacto de uma linha, depois que
// _resolveQBGerencia confirma a área escolhida — em vez da opção
// simplesmente desaparecer (estado anterior), o usuário vê qual área está
// ativa e tem uma ação explícita pra trocar.
function _qbShowAreaPill(topic) {
  const headEl = document.getElementById("qb-gerencia-picker-head");
  const listEl = document.getElementById("qb-gerencia-picker-list");
  const pillEl = document.getElementById("qb-area-pill");
  const iconEl = document.getElementById("qb-area-pill-icon");
  const labelEl = document.getElementById("qb-area-pill-label");
  const changeBtn = document.getElementById("qb-area-pill-change");
  if (!pillEl) return;

  if (headEl) headEl.style.display = "none";
  if (listEl) listEl.style.display = "none";
  if (iconEl) iconEl.innerHTML = topic?.icon || _QB_ICON_SVG;
  if (labelEl) labelEl.textContent = topic ? _qbCapitalize(topic.label) : "Sua área";
  // Usuário não-admin está fixo na própria gerência (RBAC) — não há outra
  // área pra trocar, então a ação só aparece pra admin.
  if (changeBtn) changeBtn.style.display = currentUser?.is_admin ? "" : "none";
  pillEl.style.display = "flex";
}

// Reabre a grade de seleção a partir do pill ("Trocar área") — só admin vê
// o botão que chama isto, já que é o único perfil com mais de uma área
// disponível.
function _qbReopenGerenciaPicker() {
  // Dataset resolvido pra área anterior não vale mais até a nova escolha
  // confirmar — sem isso o botão "Gerar SQL" ficaria habilitado apontando
  // pro dataset errado enquanto o usuário ainda está escolhendo.
  qbDatasetValidationState.status = "idle";
  syncQBGenerateButtonState();
  _qbShowGerenciaPicker(_QB_GERENCIA_TOPICS);
}

// Usuário sem gerência cadastrada (ou gerência fora da lista fixa) e sem
// vir do Explorador de Esquema — não há mais seletor manual de projeto/
// dataset dentro do QB, então a única saída é apontar para o Explorador,
// que já preenche tudo automaticamente ao abrir uma tabela (neoGoQB).
function _qbShowNoGerenciaGuidance() {
  const picker = document.getElementById("qb-gerencia-picker");
  const titleEl = document.getElementById("qb-gerencia-picker-title");
  const hintEl = document.getElementById("qb-gerencia-picker-hint");
  const list = document.getElementById("qb-gerencia-picker-list");
  const requestField = document.getElementById("qb-request-field");
  const btn = document.getElementById("qb-btn");
  if (!picker || !list) return;

  _setQBGerenciaMode(true);
  if (requestField) requestField.style.display = "none";
  if (btn) btn.style.display = "none";

  if (titleEl) titleEl.textContent = "Nenhuma área associada ao seu perfil";
  if (hintEl) {
    hintEl.textContent =
      "Abra o Explorador de Esquema, escolha uma tabela e gere a consulta a partir dela.";
  }

  picker.style.display = "block";
  list.innerHTML = `
    <button type="button" class="fa-topic-card" onclick="navTo('er')">
      <span class="fa-topic-icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
      </span>
      <span class="fa-topic-label">Abrir Explorador de Esquema</span>
    </button>`;
}

function _qbCapitalize(text) {
  const t = String(text || "").trim();
  return t ? t.charAt(0).toUpperCase() + t.slice(1) : t;
}

function _qbGerenciaPhaseText(phase) {
  switch (phase) {
    case "catalog":
      return "Lendo tabelas, colunas e descrições";
    case "suggestions":
      return "Preparando sugestões de perguntas";
    default:
      return "Carregando o contexto da sua gerência";
  }
}

function _qbShowGerenciaLearning(label) {
  const container = document.getElementById("qb-gerencia-learning");
  const empty = document.getElementById("qb-empty");
  const tabsArea = document.getElementById("qb-tabs-area");
  if (!container) return null;

  if (empty) empty.style.display = "none";
  if (tabsArea) tabsArea.style.display = "none";
  container.style.display = "block";
  container.innerHTML = `
    <div class="qa-empty" style="height: 100%">
      <div class="qa-empty-ico">${_QB_ICON_SVG}</div>
      <h3 id="qb-ger-phase">
        ${_qbGerenciaPhaseText(null)}<span class="fa-thinking-dots"><span></span><span></span><span></span></span>
      </h3>
      <p>Preparando o Query Builder para ${label ? _escapeHtml(_qbCapitalize(label)) : "sua gerência"}.</p>
    </div>`;

  return {
    setPhase(phase) {
      const phaseEl = document.getElementById("qb-ger-phase");
      if (!phaseEl) return;
      phaseEl.innerHTML = `${_qbGerenciaPhaseText(phase)}<span class="fa-thinking-dots"><span></span><span></span><span></span></span>`;
    },
  };
}

// As sugestões em si vivem só na faixa fixa perto do input (qb-suggestions-block,
// igual ao fa-quick-suggestions do Finance Voice) — essa tela só dá as boas-vindas.
function _qbShowGerenciaReady(label) {
  const container = document.getElementById("qb-gerencia-learning");
  if (!container) return;

  const niceLabel = _escapeHtml(_qbCapitalize(label));

  container.innerHTML = `
    <div class="qa-empty" style="height: 100%">
      <div class="qa-empty-ico">${_QB_ICON_SVG}</div>
      <h3>Estou pronto para criar consultas sobre ${niceLabel}.</h3>
      <p style="max-width: 360px">Escolha uma sugestão abaixo ou descreva sua necessidade no campo ao lado.</p>
    </div>`;
}

function _qbHideGerenciaLearning() {
  const container = document.getElementById("qb-gerencia-learning");
  const empty = document.getElementById("qb-empty");
  if (container) container.style.display = "none";
  if (empty) empty.style.display = "flex";
}

async function _resolveQBGerencia(gerencia) {
  showQBError("");
  // Trava o botão já de partida — fica destravado só depois que o dataset
  // resolver E houver texto na solicitação (escolhido ou digitado).
  const btn = document.getElementById("qb-btn");
  if (btn) btn.disabled = true;

  const topic = _qbFindGerenciaTopic(gerencia);
  const phaseHandle = _qbShowGerenciaLearning(gerencia);

  try {
    const res = await fetch("/api/agents/query_build/resolve-gerencia", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ gerencia }),
    });

    if (res.status === 401) {
      doLogout();
      return;
    }

    const data = await res.json();

    if (!data.valid) {
      qbDatasetValidationState.status = "invalid";
      _qbHideGerenciaLearning();
      showQBError(data.message || "Não foi possível resolver a gerência.");
      syncQBGenerateButtonState();
      setFAInteractionLock(false);
      return;
    }

    const projectSel = document.getElementById("qb-project");
    const datasetSel = document.getElementById("qb-dataset");
    if (projectSel) {
      if (![...projectSel.options].some((o) => o.value === data.project_id)) {
        projectSel.add(new Option(data.project_id, data.project_id));
      }
      projectSel.value = data.project_id;
    }
    if (datasetSel) {
      if (![...datasetSel.options].some((o) => o.value === data.dataset_id)) {
        datasetSel.add(new Option(data.dataset_id, data.dataset_id));
      }
      datasetSel.value = data.dataset_id;
    }

    qbDatasetValidationState.status = "valid";
    qbDatasetValidationState.datasetHint = data.dataset_id;
    qbDatasetValidationState.projectId = data.project_id;

    syncQBGenerateButtonState();
    _qbShowAreaPill(topic);
    setFAInteractionLock(false);

    phaseHandle?.setPhase("suggestions");
    const res2 = await fetch("/api/agents/query_build/suggestions", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        project_id: data.project_id,
        dataset_hint: data.dataset_id,
        table_id: "",
      }),
    });
    const sugData = await res2.json();
    _qbShowGerenciaReady(data.gerencia);
    _renderQBSuggestionChips(sugData.suggestions);
  } catch (e) {
    qbDatasetValidationState.status = "invalid";
    _qbHideGerenciaLearning();
    showQBError(prettifyErrorMessage(e.message));
    syncQBGenerateButtonState();
    setFAInteractionLock(false);
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
function forceReanalyze() {
  if (_qaHitlThreadId) {
    const ok = window.confirm(
      "Existe uma análise aguardando sua decisão. Reanalisar agora vai descartá-la. Confirma?",
    );
    if (!ok) return;
  }
  _qaLastResult = null;
  _qaHitlThreadId = null;
  runAnalyze();
}

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
  const project_id = qaDatasetValidationState.projectId || "";
  const dataset_hint = qaDatasetValidationState.datasetHint || "";
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

  // Reaproveitamento de resultado: mesma query, mesmo projeto e dataset já analisados
  if (
    _qaLastResult &&
    _qaLastResult.query === query &&
    _qaLastResult.projectId === project_id &&
    _qaLastResult.datasetHint === dataset_hint &&
    _qaLastResult.data?.status === "ok"
  ) {
    setQALoading(false);
    qaAnalyzeInFlight = false;
    resetQATabsDataState();
    resetQAResultPanels();
    if (qaEmpty) qaEmpty.style.display = "none";
    if (qaTabsArea) qaTabsArea.style.display = "none";
    const cachedData = { ..._qaLastResult.data, _cached: true };
    renderQA(cachedData);
    return;
  }

  setQAProgress("Validando entrada...", 18);
  resetQATabsDataState();
  resetQAResultPanels();
  _qaLastResult = null;

  if (qaEmpty) qaEmpty.style.display = "none";
  if (qaTabsArea) qaTabsArea.style.display = "none";

  try {
    setTimeout(() => setQAProgress("Estimando custo no BigQuery...", 36), 180);
    setTimeout(() => setQAProgress("Detectando anti-padrões...", 62), 520);
    setTimeout(() => setQAProgress("Consolidando resultado...", 84), 980);
    startQAIndeterminateFallback("Aguardando resposta do servidor…");

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
    clearQAIndeterminateFallback();

    setQAProgress("Finalizando apresentação...", 100);

    if (data.status === "awaiting_approval") {
      showQAHitlPanel(data);
    } else {
      _qaLastResult = { query, projectId: project_id, datasetHint: dataset_hint, data };
      renderQA(data);
      saveToHistory(data, query);
    }
  } catch (e) {
    clearQAIndeterminateFallback();
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
    showQBError(
      "Abra o Query Builder pelo Schema Explorer para carregar o contexto do projeto.",
    );
    return;
  }

  if (!datasetHint) {
    showQBError(
      "Abra o Query Builder pelo Schema Explorer para carregar o dataset.",
    );
    return;
  }

  const isValidDataset =
    qbDatasetValidationState.status === "valid" &&
    qbDatasetValidationState.datasetHint === datasetHint &&
    qbDatasetValidationState.projectId === projectId;
  if (!isValidDataset) {
    showQBError(
      "Contexto ainda não validado. Volte ao Schema Explorer e abra novamente o Query Builder.",
    );
    return;
  }

  showQBError("");
  setQBLoading(true);
  showQBGenerating();

  try {
    setTimeout(() => setGeneratingPhase("generating"), 180);
    setTimeout(() => setGeneratingPhase("dryrun"), 520);
    setTimeout(() => setGeneratingPhase("reviewing"), 980);

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
    hideQBGenerating();

    if (data.status === "awaiting_approval") {
      showQBHitlPanel(data);
    } else if (data.status === "error") {
      showQBError(prettifyErrorMessage(data.error || "Erro ao gerar query"));
      if (qbEmpty) qbEmpty.style.display = "flex";
    } else {
      renderQB(data);
    }
  } catch (e) {
    hideQBGenerating();
    showQBError(prettifyErrorMessage(e.message));
    if (qbEmpty) qbEmpty.style.display = "flex";
  } finally {
    hideQBGenerating();
    setTimeout(() => setQBLoading(false), 350);
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
    "[FOCO] auditoria de experiência do cliente, fricção, VoC, NPS";

  setAuditLoading(true);
  setAuditProgress("Extraindo filtros", 10);

  const timers = [
    setTimeout(
      () => setAuditProgress("Buscando interações no BigQuery", 28),
      300,
    ),
    setTimeout(
      () => setAuditProgress("Analisando sentimentos e fricção", 52),
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

    setAuditProgress("Finalizando apresentação", 100);
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
  const qaProject = qaDatasetValidationState.projectId || "";
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
      display: flex; flex-direction: column; justify-content: center; align-items: center;
      font-size: 11px; color: #8a9ab5; flex-wrap: wrap; gap: 6px; text-align: center;
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
  const hitlPanel = document.getElementById("qb-hitl-panel");
  const gerenciaLearning = document.getElementById("qb-gerencia-learning");

  if (empty) empty.style.display = "none";
  if (hitlPanel) hitlPanel.style.display = "none";
  if (gerenciaLearning) gerenciaLearning.style.display = "none";
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
  const qbCostEst = document.getElementById("qb-cost-est");
  const qbBytesProc = document.getElementById("qb-bytes-proc");
  const qbBytesProcSub = document.getElementById("qb-bytes-proc-sub");
  const qbCostTierBadge = document.getElementById("qb-cost-tier-badge");
  const qbCostTierSub = document.getElementById("qb-cost-tier-sub");
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

  const score = typeof data.quality_score === "number" ? data.quality_score : 100;
  const grade =
    score >= 90 ? "A" : score >= 75 ? "B" : score >= 60 ? "C" : score >= 40 ? "D" : "F";
  const qualityIssues = Array.isArray(data.quality_issues) ? data.quality_issues : [];

  const costTierMap = {
    baixo: { emoji: "🟢", label: "Baixo" },
    moderado: { emoji: "🟡", label: "Moderado" },
    alto: { emoji: "🔴", label: "Alto" },
  };
  const tierInfo = costTierMap[data.cost_tier] || null;

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
    if (dry.bytes_processed != null) {
      const tierLabel = tierInfo ? tierInfo.label.toLowerCase() : "indefinido";
      const scanPart =
        data.table_scan_pct != null ? `, ${fmtPct(data.table_scan_pct)} da tabela` : "";
      summary.textContent = `Esta consulta deve processar ${fmtBytes(dry.bytes_processed)}${scanPart} — custo ${tierLabel}.`;
    } else {
      summary.textContent =
        data.explanation ||
        "Query construida com foco em performance e melhor aproveitamento de slots no BigQuery.";
    }
  }

  if (qbTiles) {
    const hasDry =
      dry.bytes_processed != null || dry.estimated_cost_usd != null;
    qbTiles.style.display = hasDry ? "grid" : "none";
    if (qbCostEst) qbCostEst.textContent = fmtPct(data.table_scan_pct);
    if (qbBytesProc) qbBytesProc.textContent = fmtBytes(dry.bytes_processed);
    if (qbBytesProcSub) qbBytesProcSub.textContent = "processados nesta execução";
    if (qbCostTierBadge)
      qbCostTierBadge.textContent = tierInfo ? `${tierInfo.emoji} ${tierInfo.label}` : "—";
    if (qbCostTierSub)
      qbCostTierSub.textContent = tierInfo ? "faixa de custo desta consulta" : "—";
  }

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
    ...qualityIssues,
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
        <div class="rec-item">Dados escaneados: <strong style="margin-left:6px">${fmtPct(data.table_scan_pct)} da tabela</strong></div>
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
  const textarea = document.getElementById("qb-request");

  qbIsLoading = on;
  syncQBGenerateButtonState();
  btn?.classList.toggle("is-loading", !!on);
  if (textarea) textarea.disabled = !!on;
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

function showQBHitlPanel(data) {
  _qbHitlThreadId = data.thread_id;

  const panel = document.getElementById("qb-hitl-panel");
  const empty = document.getElementById("qb-empty");
  const tabsArea = document.getElementById("qb-tabs-area");
  const subtitle = document.getElementById("qb-hitl-subtitle");
  const container = document.getElementById("qb-hitl-issues");
  const improveBtn = document.getElementById("qb-hitl-improve");
  const acceptBtn = document.getElementById("qb-hitl-accept");
  const processing = document.getElementById("qb-hitl-processing");

  if (improveBtn) improveBtn.disabled = false;
  if (acceptBtn) acceptBtn.disabled = false;
  if (processing) processing.style.display = "none";

  if (empty) empty.style.display = "none";
  if (tabsArea) tabsArea.style.display = "none";
  document.getElementById("qb-gerencia-learning")?.style.setProperty("display", "none");
  if (panel) panel.style.display = "flex";

  if (subtitle) {
    subtitle.textContent = `A consulta gerada tem nota ${data.quality_score ?? "—"}/100`;
  }

  if (container) {
    const issues = Array.isArray(data.quality_issues) ? data.quality_issues : [];
    container.innerHTML = issues.length
      ? issues
          .map(
            (issue) => `
        <div class="ap-card sev-medium">
          <div class="ap-top">
            <span class="ap-chip chip-medium">QUALIDADE</span>
          </div>
          <div class="ap-desc">${issue}</div>
        </div>
      `,
          )
          .join("")
      : '<div class="diff-empty">Nenhum problema específico detalhado.</div>';
  }
}

async function resumeQB(decision) {
  if (!_qbHitlThreadId) {
    showQBError("Sessão de geração de SQL expirou. Por favor, inicie uma nova solicitação.");
    return;
  }

  const improveBtn = document.getElementById("qb-hitl-improve");
  const acceptBtn = document.getElementById("qb-hitl-accept");
  const processing = document.getElementById("qb-hitl-processing");
  const procTitle = document.getElementById("qb-hitl-proc-title");
  const procDesc = document.getElementById("qb-hitl-proc-desc");
  const hitlPanel = document.getElementById("qb-hitl-panel");

  if (improveBtn) improveBtn.disabled = true;
  if (acceptBtn) acceptBtn.disabled = true;

  if (procTitle) procTitle.textContent = decision === "melhorar" ? "Melhorando query..." : "Aplicando decisão...";
  if (procDesc) procDesc.textContent = decision === "melhorar"
    ? "Voltando ao construtor de SQL para corrigir os problemas identificados"
    : "Seguindo com a SQL gerada para a amostra de dados";

  if (hitlPanel) { hitlPanel.scrollTop = 0; hitlPanel.style.overflowY = "hidden"; }
  if (processing) processing.style.display = "flex";

  setQBLoading(true);

  try {
    const res = await fetch("/api/agents/query_build/resume", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ thread_id: _qbHitlThreadId, decision }),
    });

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || "Erro ao retomar geração de SQL");

    if (hitlPanel) { hitlPanel.style.display = "none"; hitlPanel.style.overflowY = ""; }
    if (processing) processing.style.display = "none";
    _qbHitlThreadId = null;

    if (data.status === "awaiting_approval") {
      showQBHitlPanel(data);
    } else if (data.status === "error") {
      showQBError(prettifyErrorMessage(data.error || "Erro ao gerar query"));
    } else {
      renderQB(data);
    }
  } catch (e) {
    if (processing) processing.style.display = "none";
    if (hitlPanel) hitlPanel.style.overflowY = "";
    if (improveBtn) improveBtn.disabled = false;
    if (acceptBtn) acceptBtn.disabled = false;
    showQBError(prettifyErrorMessage(e.message));
  } finally {
    setTimeout(() => setQBLoading(false), 350);
  }
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

  const cachedNotice = document.getElementById("q-cached-notice");
  if (cachedNotice) cachedNotice.style.display = d._cached ? "flex" : "none";

  // Tiles
  const qTiles = document.getElementById("q-tiles");
  const qSavSec = document.getElementById("q-sav-sec");

  const qSavTile = document.getElementById("q-sav-tile");
  const qSlotsTile = document.getElementById("q-slots-tile");

  if (d.bytes_original != null && qTiles) {
    qTiles.style.display = "grid";

    document.getElementById("q-borig").textContent = fmtBytes(d.bytes_original);
    document.getElementById("q-corig").textContent = fmtUSD(
      d.cost_original_usd,
    );
    document.getElementById("q-bopt").textContent =
      d.bytes_optimized != null ? fmtBytes(d.bytes_optimized) : "—";
    document.getElementById("q-copt").textContent =
      d.cost_optimized_usd != null ? fmtUSD(d.cost_optimized_usd) : "—";

    const pct = d.savings_pct || 0;
    const impact = d.optimization_impact || "none";
    const showSlotsTile = pct === 0 && impact === "slots_only";

    document.getElementById("q-sav").textContent =
      pct > 0 ? `↓ ${pct}%` : "N/A";
    document.getElementById("q-savusd").textContent =
      d.cost_saved_usd != null ? fmtUSD(d.cost_saved_usd) : "—";

    // Alterna entre tile "Economia" e tile "Slots/Compute"
    if (qSavTile) qSavTile.style.display = showSlotsTile ? "none" : "";
    if (qSlotsTile) qSlotsTile.style.display = showSlotsTile ? "" : "none";

    if (pct > 0 && qSavSec) {
      qSavSec.style.display = "block";
      document.getElementById("q-sav-big").textContent = `↓ ${pct}%`;

      setTimeout(() => {
        document.getElementById("q-sav-fill").style.width = `${pct}%`;
      }, 150);
    } else if (qSavSec) {
      qSavSec.style.display = "none";
    }
  }

  // Optimized query
  const tabOptimized = document.getElementById("tab-optimized");
  const qOptSec = document.getElementById("q-opt-sec");
  const qOptEmpty = document.getElementById("q-opt-empty");
  const qOptQuery = document.getElementById("q-opt-query");
  const qDiffSec = document.getElementById("q-diff-sec");
  const qDiffContent = document.getElementById("q-diff-content");

  if (tabOptimized) tabOptimized.classList.add("has-data");

  // Data existence warning + data quality notice
  const qWarnSec = document.getElementById("q-data-warn");
  if (qWarnSec) {
    const msgs = [];
    if (d.data_existence_warning) msgs.push(d.data_existence_warning);
    if (d.data_quality === "no_cost_data") {
      msgs.push("ℹ Score calculado sem dados de custo (dry-run indisponível) — valores de bytes e USD não exibidos.");
    } else if (d.data_quality === "partial") {
      msgs.push("ℹ Dados de custo parciais — economia estimada pode ser imprecisa.");
    }
    if (msgs.length) {
      qWarnSec.textContent = msgs.join("\n\n");
      qWarnSec.style.display = "block";
    } else {
      qWarnSec.style.display = "none";
    }
  }

  if (d.optimized_query) {
    if (qOptSec) qOptSec.style.display = "block";
    if (qOptEmpty) qOptEmpty.style.display = "none";
    if (qOptQuery) qOptQuery.textContent = d.optimized_query;

    if (qDiffSec && qDiffContent && d.original_query) {
      qDiffContent.innerHTML = _buildSqlDiff(d.original_query, d.optimized_query);
      qDiffSec.style.display = "block";
    }
  } else {
    if (qOptSec) qOptSec.style.display = "none";
    if (qOptEmpty) qOptEmpty.style.display = "flex";
    if (qDiffSec) qDiffSec.style.display = "none";
  }

  // Intelligence summary
  const qIntelSec = document.getElementById("q-intel-sec");
  const qIntelContent = document.getElementById("q-intel-content");
  if (qIntelSec && qIntelContent && d.intelligence_summary) {
    qIntelContent.textContent = d.intelligence_summary;
    qIntelSec.style.display = "block";
  } else if (qIntelSec) {
    qIntelSec.style.display = "none";
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
  const btn = document.getElementById("qa-copy-sql-btn");

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

function showQAHitlPanel(data) {
  _qaHitlThreadId = data.thread_id;

  const panel = document.getElementById("qa-hitl-panel");
  const empty = document.getElementById("qa-empty");
  const tabsArea = document.getElementById("qa-tabs-area");
  const container = document.getElementById("qa-hitl-antipatterns");
  const approveBtn = document.getElementById("qa-hitl-approve");
  const skipBtn = document.getElementById("qa-hitl-skip");
  const processing = document.getElementById("qa-hitl-processing");

  // Reset state — pode vir de uma análise anterior onde os botões ficaram disabled
  if (approveBtn) approveBtn.disabled = false;
  if (skipBtn) skipBtn.disabled = false;
  if (processing) processing.style.display = "none";

  if (empty) empty.style.display = "none";
  if (tabsArea) tabsArea.style.display = "none";
  if (panel) panel.style.display = "flex";

  const costBadge = document.getElementById("qa-hitl-cost");
  if (costBadge) {
    if (data.bytes_processed != null) {
      costBadge.textContent = `${fmtBytes(data.bytes_processed)} · ${fmtUSD(data.estimated_cost_usd)}`;
      costBadge.style.display = "inline-block";
    } else {
      costBadge.style.display = "none";
    }
  }

  if (container) {
    const sevClass = { CRITICAL: "sev-critical", HIGH: "sev-high", MEDIUM: "sev-medium", LOW: "sev-low" };
    const chipClass = { CRITICAL: "chip-critical", HIGH: "chip-high", MEDIUM: "chip-medium", LOW: "chip-low" };
    container.innerHTML = (data.antipatterns || []).map(ap => {
      const sev = (ap.severity || "").toUpperCase();
      return `
        <div class="ap-card ${sevClass[sev] || "sev-low"}">
          <div class="ap-top">
            <span class="ap-chip ${chipClass[sev] || "chip-low"}">${sev}</span>
            <span class="ap-name">${ap.pattern}</span>
          </div>
          <div class="ap-desc">${ap.description}</div>
          ${ap.suggestion ? `<div class="ap-fix"><svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>${ap.suggestion}</div>` : ""}
        </div>
      `;
    }).join("");
  }
}

function _buildSqlDiff(original, optimized) {
  const origLines = (original || "").split("\n");
  const optLines = (optimized || "").split("\n");
  const origSet = new Set(origLines.map(l => l.trim()));
  const optSet = new Set(optLines.map(l => l.trim()));

  const removed = origLines.filter(l => !optSet.has(l.trim()) && l.trim()).map(l =>
    `<div class="diff-line diff-removed"><span class="diff-sign">−</span><span>${l.replace(/</g,"&lt;")}</span></div>`
  );
  const added = optLines.filter(l => !origSet.has(l.trim()) && l.trim()).map(l =>
    `<div class="diff-line diff-added"><span class="diff-sign">+</span><span>${l.replace(/</g,"&lt;")}</span></div>`
  );

  if (!removed.length && !added.length) {
    return '<div class="diff-empty">Nenhuma diferença encontrada.</div>';
  }

  return [
    removed.length ? `<div class="diff-section-label">Removido</div>${removed.join("")}` : "",
    added.length ? `<div class="diff-section-label">Adicionado</div>${added.join("")}` : "",
  ].join("");
}

async function resumeQA(decision) {
  if (!_qaHitlThreadId) {
    showQAError("Sessão de análise expirou. Por favor, inicie uma nova análise.");
    return;
  }

  const approveBtn = document.getElementById("qa-hitl-approve");
  const skipBtn = document.getElementById("qa-hitl-skip");
  const processing = document.getElementById("qa-hitl-processing");
  const procTitle = document.getElementById("qa-hitl-proc-title");
  const procDesc = document.getElementById("qa-hitl-proc-desc");

  if (approveBtn) approveBtn.disabled = true;
  if (skipBtn) skipBtn.disabled = true;

  if (procTitle) procTitle.textContent = decision === "approve" ? "Otimizando query..." : "Gerando relatório...";
  if (procDesc) procDesc.textContent = decision === "approve"
    ? "Aplicando correções automáticas nos anti-padrões"
    : "Compilando análise sem aplicar otimizações";

  const hitlPanel = document.getElementById("qa-hitl-panel");
  if (hitlPanel) { hitlPanel.scrollTop = 0; hitlPanel.style.overflowY = "hidden"; }
  if (processing) processing.style.display = "flex";

  setQAProgress(decision === "approve" ? "Otimizando query..." : "Gerando relatório...", 50);

  try {
    const res = await fetch("/api/agents/query_analyzer/resume", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ thread_id: _qaHitlThreadId, decision }),
    });

    const data = await res.json();

    if (!res.ok) throw new Error(data.detail || "Erro ao retomar análise");

    const panel = document.getElementById("qa-hitl-panel");
    if (panel) { panel.style.display = "none"; panel.style.overflowY = ""; }
    if (processing) processing.style.display = "none";
    _qaHitlThreadId = null;

    setQAProgress("Finalizando apresentação...", 100);
    const _resumeQuery = document.getElementById("qa-query")?.value || "";
    _qaLastResult = {
      query: _resumeQuery,
      projectId: qaDatasetValidationState.projectId || "",
      datasetHint: qaDatasetValidationState.datasetHint || "",
      data,
    };
    renderQA(data);
    saveToHistory(data, _resumeQuery);
  } catch (e) {
    if (processing) processing.style.display = "none";
    if (hitlPanel) hitlPanel.style.overflowY = "";
    if (approveBtn) approveBtn.disabled = false;
    if (skipBtn) skipBtn.disabled = false;
    showQAError(e.message);
  } finally {
    setTimeout(() => { hideQAProgress(); setQALoading(false); qaAnalyzeInFlight = false; }, 350);
  }
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

document.getElementById("qb-request")?.addEventListener("input", () => {
  syncQBGenerateButtonState();
});

document.getElementById("db-request")?.addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    runDocumentBuild();
  }
});

// ─────────────────────────────────────
// Init
// ─────────────────────────────────────
window.addEventListener("load", function init() {
  console.log("🚀 Inicializando Finance Hub IA...");
  try {
    showScreen("screen-login");
    document.getElementById("inp-user")?.focus();
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
      "Converse com os dados da Diretoria Financeira — contas a pagar, contas a receber, cobrança e experiência do cliente em linguagem natural.",
    tags: ["Financeiro", "Cobrança", "IA"],
    status: "Disponível",
    action: () => navTo("audit"),
  },
  {
    name: "Schema Explorer",
    description:
      "Visualize o diagrama ER de datasets BigQuery com relacionamentos e navegação interativa.",
    tags: ["Schema Explorer", "BigQuery", "DataOps"],
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
let faThinkingHandle = null;
let faLearningHandle = null;
let faInputListenerBound = false;
let faMsgCounter = 0;
const FA_TYPING_BASE_DELAY_MS = 16;
// Era 850ms — espera artificial pura, somada DEPOIS que a resposta real já
// chegou inteira (ver docs/plans/2026-06-21-tempo-resposta-prd.md, item P0).
// Ainda dá tempo de perceber o efeito de digitação sem fazer o usuário
// esperar à toa por algo que já está pronto.
const FA_TYPING_MIN_DURATION_MS = 300;

function _faWait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Sugestões cruzam tema (detectado por palavra-chave na pergunta original)
// com persona: diretor pensa em impacto/meta, gerente em segmentação/comparação,
// coordenador em ação/prazo — "geral" mantém o tom neutro de antes.
const _FA_FOLLOWUP_BY_THEME = [
  {
    re: /pix|clientes?|receb/,
    geral: [
      "Quais clientes via Pix mais cresceram em relação ao período anterior?",
      "Qual a concentração de receita nos 10 principais clientes pagantes via Pix?",
      "Existe diferença de inadimplência entre Pix e outros meios de pagamento?",
      "Como a receita via Pix se distribuiu entre os canais de pagamento no período?",
      "Quais clientes pararam de pagar via Pix recentemente?",
      "Qual o ticket médio das transações via Pix nesse período?",
    ],
    diretor: [
      "Qual o impacto da receita via Pix no resultado consolidado do período?",
      "Como a concentração nos principais clientes pagantes afeta o risco da carteira?",
      "Essa tendência de Pix sustenta a meta de receita do trimestre?",
      "Essa receita via Pix está alinhada com o plano de expansão do negócio?",
      "Qual o retorno desse canal de pagamento frente ao custo de operação?",
      "Existe dependência excessiva de poucos clientes nessa receita?",
    ],
    gerente: [
      "Quais segmentos de cliente explicam o crescimento via Pix?",
      "Quero abrir a concentração de receita por região ou produto.",
      "Como a inadimplência via Pix se compara entre carteiras?",
      "Como o ticket médio via Pix varia entre os segmentos de cliente?",
      "Quero comparar a adesão ao Pix entre as carteiras regionais.",
      "Quais campanhas explicam o crescimento recente via Pix?",
    ],
    coordenador: [
      "Quais clientes Pix preciso acompanhar de perto esta semana?",
      "Quais contas com maior concentração precisam de ação imediata?",
      "Que casos de inadimplência via Pix devo priorizar hoje?",
      "Quais clientes com queda recente de pagamento via Pix preciso contatar?",
      "Que ajustes operacionais preciso fazer para sustentar esse volume?",
      "Quais alertas de inadimplência via Pix preciso resolver hoje?",
    ],
  },
  {
    re: /contas a pagar|fornecedor|despesa/,
    geral: [
      "Quais fornecedores concentram o maior volume a pagar?",
      "Quais vencimentos críticos estão previstos para os próximos 7 dias?",
      "Onde houve maior aumento de despesa em relação ao período anterior?",
      "Como o volume a pagar evoluiu nos últimos meses?",
      "Quais categorias de despesa têm a maior participação no total?",
      "Existe concentração de pagamentos em poucos fornecedores?",
    ],
    diretor: [
      "Qual o impacto desse volume a pagar no fluxo de caixa do trimestre?",
      "Esses fornecedores representam algum risco de concentração para o negócio?",
      "Como essa despesa se compara ao orçamento aprovado?",
      "Essa despesa compromete a margem projetada para o período?",
      "Quais fornecedores estratégicos merecem renegociação de prazo?",
      "Como esse volume a pagar se compara ao mesmo período do ano anterior?",
    ],
    gerente: [
      "Quero abrir o volume a pagar por categoria de despesa.",
      "Quais fornecedores tiveram maior variação de custo no período?",
      "Como os vencimentos críticos se distribuem entre as áreas?",
      "Quero comparar o volume a pagar entre filiais ou unidades.",
      "Quais contratos têm maior impacto no custo fixo mensal?",
      "Como a sazonalidade afeta o volume de contas a pagar?",
    ],
    coordenador: [
      "Quais pagamentos preciso liberar nos próximos 7 dias?",
      "Quais fornecedores preciso contatar hoje por atraso?",
      "Que vencimentos críticos exigem ação imediata?",
      "Quais aprovações de pagamento estão pendentes agora?",
      "Quais duplicatas com erro preciso corrigir antes do vencimento?",
      "Que fornecedores preciso notificar sobre mudança de prazo?",
    ],
  },
  {
    re: /cobran/,
    geral: [
      "Quais faixas de atraso concentram mais valor em aberto?",
      "Quais carteiras tiveram piora de recuperação no período?",
      "Que ações priorizar para reduzir inadimplência nesta semana?",
      "Qual o percentual de recuperação nos últimos 30 dias?",
      "Como o tempo médio de atraso evoluiu no período?",
      "Quais canais de cobrança têm melhor taxa de resposta?",
    ],
    diretor: [
      "Qual o impacto da inadimplência atual no resultado do período?",
      "Como a taxa de recuperação se compara à meta da diretoria?",
      "Existe algum risco de concentração de perda em carteiras específicas?",
      "Essa inadimplência está dentro do apetite de risco aprovado?",
      "Qual o custo da operação de cobrança frente ao valor recuperado?",
      "Como nossa taxa de recuperação se compara ao mercado?",
    ],
    gerente: [
      "Quero abrir a inadimplência por carteira ou segmento de cliente.",
      "Quais faixas de atraso pioraram mais em relação ao período anterior?",
      "Como a recuperação varia entre as equipes de cobrança?",
      "Quero comparar a efetividade dos canais de cobrança entre si.",
      "Como a inadimplência se comporta por faixa de renda ou perfil de cliente?",
      "Quais scripts ou abordagens tiveram melhor resultado no período?",
    ],
    coordenador: [
      "Quais casos de maior valor em aberto preciso tratar hoje?",
      "Quem são os responsáveis pelas carteiras com piora de recuperação?",
      "Que ações de cobrança preciso disparar esta semana?",
      "Quais clientes preciso escalar para cobrança judicial?",
      "Que renegociações estão pendentes de aprovação hoje?",
      "Quais contas vencem nas próximas 24h sem contato registrado?",
    ],
  },
  {
    re: /fluxo de caixa|caixa/,
    geral: [
      "Quais entradas e saídas mais pressionam o caixa neste período?",
      "Qual a projeção do caixa para os próximos 30 dias?",
      "Onde há maior risco de descasamento entre recebimentos e pagamentos?",
      "Qual o saldo de caixa projetado para o fim do mês?",
      "Quais meses do ano historicamente pressionam mais o caixa?",
      "Como as despesas recorrentes impactam o caixa disponível?",
    ],
    diretor: [
      "Qual o impacto dessa posição de caixa na liquidez do trimestre?",
      "Essa projeção sustenta os compromissos estratégicos dos próximos 30 dias?",
      "Existe risco de descasamento que exija decisão da diretoria?",
      "Esse caixa sustenta os investimentos planejados para o próximo trimestre?",
      "Existe necessidade de captação para cobrir o período mais apertado?",
      "Como a posição de caixa atual se compara à meta anual?",
    ],
    gerente: [
      "Quero abrir as entradas e saídas por área ou centro de custo.",
      "Como a projeção de caixa varia entre os cenários otimista e conservador?",
      "Quais áreas mais contribuem para o risco de descasamento?",
      "Quero abrir a projeção de caixa por cenário (otimista/conservador) e área.",
      "Quais recebimentos atrasados mais afetam a previsão de caixa?",
      "Como o ciclo financeiro (prazo médio de pagamento e recebimento) evoluiu?",
    ],
    coordenador: [
      "Quais pagamentos preciso priorizar para não comprometer o caixa esta semana?",
      "Que recebimentos preciso acompanhar de perto nos próximos dias?",
      "Onde preciso agir hoje para reduzir o risco de descasamento?",
      "Quais pagamentos preciso adiar para não estourar o caixa esta semana?",
      "Que recebimentos preciso antecipar para cobrir compromissos urgentes?",
      "Quais contas preciso monitorar de hoje até o fim da semana?",
    ],
  },
];

const _FA_FOLLOWUP_DEFAULT = {
  geral: [
    "Qual recorte por período você quer aprofundar agora?",
    "Quais segmentos ou clientes merecem um detalhamento maior?",
    "Quer que eu compare esse resultado com o período anterior?",
    "Quer ver esse resultado em outro formato, como gráfico ou tabela?",
    "Há algum recorte por canal ou produto que valha explorar?",
    "Quer que eu detalhe os números por trás dessa conclusão?",
  ],
  diretor: [
    "Qual o impacto disso no resultado do período?",
    "Como isso se compara à meta ou ao orçamento aprovado?",
    "Existe algum risco que mereça atenção da diretoria?",
    "Esse resultado está alinhado com a meta do trimestre?",
    "Quais decisões esse número deveria embasar agora?",
    "Existe algum cenário de risco que precise ser monitorado?",
  ],
  gerente: [
    "Quero abrir esse resultado por segmento, região ou produto.",
    "Como isso se compara ao período anterior?",
    "Quais áreas explicam a maior parte dessa variação?",
    "Quero abrir esse número por unidade, canal ou produto.",
    "Como esse resultado se compara à média histórica?",
    "Quais fatores mais influenciaram essa variação?",
  ],
  coordenador: [
    "O que preciso tratar com prioridade hoje a partir desse resultado?",
    "Quais casos específicos preciso acompanhar esta semana?",
    "Quem são os responsáveis pelos pontos mais críticos aqui?",
    "Quais ações concretas esse resultado sugere para esta semana?",
    "Quem precisa ser avisado sobre esse número?",
    "Existe algum prazo associado a esse achado que eu deva priorizar?",
  ],
};

function _faSuggestedFollowups(query, persona) {
  const q = String(query || "").toLowerCase();
  const p = String(persona || "").toLowerCase();
  const theme = _FA_FOLLOWUP_BY_THEME.find((t) => t.re.test(q));
  const bucket = theme || _FA_FOLLOWUP_DEFAULT;
  return bucket[p] || bucket.geral;
}

// Barra de sugestões fixa acima do input (estilo Veezoo) — substitui o antigo
// bloco "Próximas perguntas sugeridas" dentro da bolha. Sempre traz até 6
// sugestões; mostra as 4 primeiras e esconde as 2 últimas atrás de "Mostrar mais".
function _faRenderQuickSuggestions(suggestions) {
  const bar = document.getElementById("fa-quick-suggestions");
  if (!bar) return;

  const list = (Array.isArray(suggestions) ? suggestions : [])
    .map((s) => String(s || "").trim())
    .filter(Boolean)
    .slice(0, 6);

  if (!list.length) {
    bar.innerHTML = "";
    bar.hidden = true;
    return;
  }

  const chipHtml = (text) =>
    `<button type="button" class="fa-suggestion-chip" data-followup="${_escFA(text)}">` +
    `<span class="fa-suggestion-chip-text">${_escFA(text)}</span></button>`;

  const visible = list.slice(0, 4);
  const extra = list.slice(4);
  const extraHtml = extra.length
    ? `<span class="fa-suggestions-extra" id="fa-suggestions-extra" hidden>${extra.map(chipHtml).join("")}</span>` +
      `<button type="button" class="fa-suggestions-toggle" id="fa-suggestions-toggle" aria-expanded="false">` +
      `Mostrar mais ${_faIcon("chevron-down", 11)}</button>`
    : "";

  bar.innerHTML =
    `<span class="fa-suggestions-icon" aria-hidden="true">${_faIcon("sparkle", 13)}</span>` +
    visible.map(chipHtml).join("") +
    extraHtml;
  bar.hidden = false;

  const toggle = document.getElementById("fa-suggestions-toggle");
  if (!toggle) return;
  toggle.addEventListener("click", () => {
    const extraEl = document.getElementById("fa-suggestions-extra");
    if (!extraEl) return;
    const expanded = toggle.getAttribute("aria-expanded") === "true";
    extraEl.hidden = expanded;
    toggle.setAttribute("aria-expanded", String(!expanded));
    toggle.innerHTML = expanded
      ? `Mostrar mais ${_faIcon("chevron-down", 11)}`
      : `Mostrar menos ${_faIcon("chevron-up", 11)}`;
  });
}

function _faPrepareAnswerMarkdown(text, data = {}) {
  let prepared = String(text || "");
  prepared = prepared.replace(/```sql[\s\S]*?```/gi, "");
  prepared = prepared.replace(/```[\s\S]*?```/g, (block) => {
    return /select|from|where|group by|order by|join/i.test(block) ? "" : block;
  });
  prepared = prepared.replace(/\n{3,}/g, "\n\n").trim();

  if (!prepared) return prepared;

  // Só completa o cabeçalho que falta quando o texto já É um relatório no
  // formato PADRÃO (Resumo executivo + achados) e só esqueceu o heading.
  // Dois outros formatos do Composer não levam "Resumo executivo" por
  // desenho, e injetá-lo viraria um cabeçalho vazio logo seguido de outro:
  // (1) prosa pura de "não encontrei dados" (REGRAS ANTI-META-RESPOSTA em
  // supervisor_prompts.py) — sem seção "##" nenhuma; (2) análise profunda
  // (RESPONSE_MODE_ANALISE_PROFUNDA em response_mode.py) — estrutura própria
  // "O que aconteceu? / Por que aconteceu? / ...". `data.composer_mode` é o
  // sinal confiável (vem do backend); o teste de heading é rede de segurança
  // pra quando esse campo não vier (ex.: respostas antigas em cache).
  const hasAnySection = /^##\s+/m.test(prepared);
  const isDeepAnalysis =
    data.composer_mode === "analise_profunda" ||
    /^##\s*(o que aconteceu|por que aconteceu|qual o impacto|o que fazer|o que priorizar)/im.test(prepared);
  if (hasAnySection && !isDeepAnalysis && !data.skipExecutiveSummary && !/##\s+resumo executivo/i.test(prepared)) {
    prepared = `## Resumo executivo\n\n${prepared}`;
  }

  return prepared;
}

// Set de ícones SVG inline (stroke=currentColor) — substitui os emojis nos
// cabeçalhos de seção, badges de persona e cartões de artefato por algo
// consistente com o resto da identidade visual (cor controlada via CSS).
const _FA_ICON_PATHS = {
  clipboard:
    '<rect x="4" y="3" width="10" height="13" rx="1.5"/><rect x="6.5" y="1.3" width="5" height="2.6" rx="1"/>' +
    '<line x1="6.5" y1="9" x2="11.5" y2="9"/><line x1="6.5" y1="12" x2="11.5" y2="12"/>',
  search: '<circle cx="7.5" cy="7.5" r="4.7"/><line x1="11" y1="11" x2="15.5" y2="15.5"/>',
  grid:
    '<rect x="2" y="3" width="14" height="12" rx="1.5"/><line x1="2" y1="7.5" x2="16" y2="7.5"/>' +
    '<line x1="2" y1="11.5" x2="16" y2="11.5"/><line x1="9" y1="3" x2="9" y2="15"/>',
  "check-circle": '<circle cx="9" cy="9" r="7.2"/><polyline points="5.5 9 8 11.5 12.5 6.2"/>',
  "alert-triangle":
    '<path d="M9 2 L16.5 15.5 L1.5 15.5 Z"/><line x1="9" y1="7" x2="9" y2="10.6"/>' +
    '<circle cx="9" cy="13" r="0.9" fill="currentColor" stroke="none"/>',
  message:
    '<path d="M3 4.5h12a1.5 1.5 0 0 1 1.5 1.5v6a1.5 1.5 0 0 1-1.5 1.5H8l-3.5 3v-3H3A1.5 1.5 0 0 1 1.5 12V6A1.5 1.5 0 0 1 3 4.5Z"/>',
  flag: '<line x1="4" y1="2" x2="4" y2="16"/><path d="M4 3 h9 l-2.2 3 L13 9 H4 Z"/>',
  target: '<circle cx="9" cy="9" r="7"/><circle cx="9" cy="9" r="4"/><circle cx="9" cy="9" r="1" fill="currentColor" stroke="none"/>',
  zap: '<path d="M9.5 1.5 L4 10 H8.5 L7.5 16.5 L14 7.5 H9.5 Z"/>',
  sliders:
    '<line x1="4" y1="3" x2="4" y2="15"/><circle cx="4" cy="7" r="1.6"/>' +
    '<line x1="9" y1="3" x2="9" y2="15"/><circle cx="9" cy="11" r="1.6"/>' +
    '<line x1="14" y1="3" x2="14" y2="15"/><circle cx="14" cy="6" r="1.6"/>',
  priority: '<line x1="3" y1="5" x2="15" y2="5"/><line x1="3" y1="9" x2="11" y2="9"/><line x1="3" y1="13" x2="7" y2="13"/>',
  sparkle:
    '<line x1="9" y1="2" x2="9" y2="16"/><line x1="2" y1="9" x2="16" y2="9"/>' +
    '<line x1="4.2" y1="4.2" x2="13.8" y2="13.8"/><line x1="13.8" y1="4.2" x2="4.2" y2="13.8"/>',
  "trend-up": '<line x1="3" y1="14" x2="15" y2="3"/><polyline points="8 3 15 3 15 10"/>',
  "trend-down": '<line x1="3" y1="3" x2="15" y2="14"/><polyline points="15 7 15 14 8 14"/>',
  star: '<path d="M9 1.5 L11 7 L16.5 9 L11 11 L9 16.5 L7 11 L1.5 9 L7 7 Z" fill="currentColor" stroke="none"/>',
  user: '<circle cx="9" cy="6" r="3"/><path d="M3 16c0-3.5 2.7-6 6-6s6 2.5 6 6"/>',
  code: '<polyline points="6 4 2 9 6 14"/><polyline points="12 4 16 9 12 14"/>',
  database:
    '<ellipse cx="9" cy="4" rx="6" ry="2.2"/><path d="M3 4v10c0 1.2 2.7 2.2 6 2.2s6-1 6-2.2V4"/>' +
    '<path d="M3 9c0 1.2 2.7 2.2 6 2.2s6-1 6-2.2"/>',
  activity: '<polyline points="2 9 5 9 7 4 10 14 12 9 16 9"/>',
  "bar-chart":
    '<line x1="4" y1="15" x2="4" y2="9"/><line x1="9" y1="15" x2="9" y2="5"/>' +
    '<line x1="14" y1="15" x2="14" y2="11"/><line x1="2" y1="15" x2="16" y2="15"/>',
  clock: '<circle cx="9" cy="9" r="7"/><line x1="9" y1="9" x2="9" y2="5"/><line x1="9" y1="9" x2="12" y2="11"/>',
  "chevron-down": '<polyline points="4 7 9 12 14 7"/>',
  "chevron-up": '<polyline points="4 11 9 6 14 11"/>',
  "arrow-right": '<line x1="3" y1="9" x2="14" y2="9"/><polyline points="9.5 4 14.5 9 9.5 14"/>',
};

function _faIcon(name, size = 14) {
  const inner = _FA_ICON_PATHS[name] || _FA_ICON_PATHS.sparkle;
  return (
    `<svg width="${size}" height="${size}" viewBox="0 0 18 18" fill="none" stroke="currentColor" ` +
    `stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${inner}</svg>`
  );
}

// Um único mapa cobre a classe visual (cor/fundo da seção) e o ícone do
// cabeçalho, para o ícone sempre bater com o tipo de conteúdo respondido —
// inclui tanto o formato padrão (Resumo/Achados/...) quanto as 5 seções do
// modo "análise profunda" (O que aconteceu?/Por que.../...).
const _FA_SECTION_KINDS = [
  { re: /resumo/i, kind: "summary", icon: "clipboard" },
  { re: /achado|insight/i, kind: "insights", icon: "search" },
  { re: /tabela|detalhamento/i, kind: "details", icon: "grid" },
  { re: /a[cç][aã]o|recomend/i, kind: "actions", icon: "check-circle" },
  { re: /risc/i, kind: "risks", icon: "alert-triangle" },
  { re: /o que aconteceu/i, kind: "fact", icon: "flag" },
  { re: /por que aconteceu|causa raiz/i, kind: "rootcause", icon: "target" },
  { re: /qual o impacto/i, kind: "impact", icon: "zap" },
  { re: /o que fazer/i, kind: "solution", icon: "sliders" },
  { re: /o que priorizar/i, kind: "priority", icon: "priority" },
];

function _faClassifySection(title) {
  const text = String(title || "");
  for (const entry of _FA_SECTION_KINDS) {
    if (entry.re.test(text)) return entry;
  }
  return { kind: "default", icon: "sparkle" };
}

// Realça percentuais (+12%/-8,5%) e valores em R$ dentro de texto corrido —
// chips coloridos por sinal (verde/vermelho) e cor de marca para dinheiro,
// reforçando visualmente os números que mais importam na resposta.
function _faHighlightNumbersInNode(textNode) {
  const original = textNode.nodeValue;
  if (!original || !/[%]|R\$/.test(original)) return;
  const escaped = _escFA(original);
  const replaced = escaped
    .replace(/([+-]?\d{1,3}(?:[.,]\d+)?\s?%)/g, (m) => {
      const trimmed = m.trim();
      const negative = trimmed.startsWith("-");
      const cls = negative ? "fa-delta--down" : "fa-delta--up";
      return `<span class="fa-delta ${cls}">${_faIcon(negative ? "trend-down" : "trend-up", 11)}${trimmed}</span>`;
    })
    .replace(/(R\$\s?\d[\d.,]*\s?(?:milh(?:ão|ões)|bilh(?:ão|ões)|mil)?)/g, (m) => `<span class="fa-money">${m.trim()}</span>`);
  if (replaced === escaped) return;
  const span = document.createElement("span");
  span.innerHTML = replaced;
  textNode.replaceWith(...Array.from(span.childNodes));
}

function _faHighlightNumbers(report) {
  const selector = [
    ".fa-report-section--summary p",
    ".fa-report-section--insights li",
    ".fa-report-section--fact p",
    ".fa-report-section--impact p",
    ".fa-report-section--rootcause li",
    ".fa-report-section--solution li",
    ".fa-report-section--actions li",
    ".fa-report-section--priority li",
  ].join(", ");
  report.querySelectorAll(selector).forEach((el) => {
    const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
    const textNodes = [];
    let node;
    while ((node = walker.nextNode())) textNodes.push(node);
    textNodes.forEach(_faHighlightNumbersInNode);
  });
}

// Persona "diretor": achados que começam com um número (R$/percentual) viram
// cartões de estatística (número grande + descrição) em vez de bullet simples
// — visão executiva pede a conclusão em destaque, não o texto corrido.
function _faDiretorStatCards(report) {
  const items = Array.from(
    report.querySelectorAll(
      ".fa-report-section--insights .fa-report-bullets li, .fa-report-section--fact .fa-report-bullets li",
    ),
  );
  if (items.length < 2) return;

  const headlineRe = /^(R\$\s?[\d.,]+\s?(?:milh(?:ão|ões)|bilh(?:ão|ões)|mil)?|[+-]?\d{1,3}(?:[.,]\d+)?\s?%)/;
  const hits = items
    .map((li) => ({ li, match: (li.textContent || "").match(headlineRe) }))
    .filter((m) => m.match);
  if (hits.length < 2) return;

  hits.forEach(({ li, match }) => {
    const text = li.textContent || "";
    const headline = match[1].trim();
    const rest = text.slice(match[0].length).replace(/^[\s,:;\-–—]+/, "").trim();
    const isPct = /%$/.test(headline);
    const negative = isPct && headline.startsWith("-");
    const trendIcon = isPct ? _faIcon(negative ? "trend-down" : "trend-up", 16) : "";
    li.classList.add("fa-stat-card");
    li.innerHTML =
      `<span class="fa-stat-headline${negative ? " fa-stat-headline--down" : ""}">${trendIcon}${_escFA(headline)}</span>` +
      `<span class="fa-stat-desc">${_escFA(rest)}</span>`;
  });
  hits[0].li.closest(".fa-report-bullets")?.classList.add("fa-report-bullets--stats");
}

// Ações/recomendações/prioridades (qualquer persona) viram cards de
// checklist com ícone de check e, quando o texto menciona um prazo, um chip
// de urgência — "o que fazer e até quando" merece destaque visual em vez de
// um parágrafo corrido, do coordenador ao diretor.
const _FA_URGENCY_RE = /\b(imediat\w*|hoje|24h|24-48h|24\/48h|48h|72h|24-72h|esta semana|nas pr[oó]ximas \d+h)\b/i;

function _faActionCards(report) {
  const items = report.querySelectorAll(
    ".fa-report-section--actions .fa-report-bullets li, " +
      ".fa-report-section--priority .fa-report-bullets li, " +
      ".fa-report-section--solution .fa-report-bullets li",
  );
  items.forEach((li) => {
    if (li.classList.contains("fa-action-card")) return;
    const text = li.textContent || "";
    const urgencyMatch = text.match(_FA_URGENCY_RE);
    const original = li.innerHTML;
    li.classList.add("fa-action-card");
    const urgencyHtml = urgencyMatch
      ? `<span class="fa-urgency-chip">${_faIcon("clock", 11)}${_escFA(urgencyMatch[0])}</span>`
      : "";
    li.innerHTML =
      `<span class="fa-action-check">${_faIcon("check-circle", 13)}</span>` +
      `<span class="fa-action-body">${original}</span>${urgencyHtml}`;
  });
}

// "Próximo passo:" é um lead-in livre que o LLM usa (em qualquer persona/
// seção) para indicar a ação imediata sugerida ao final de um trecho — não é
// um cabeçalho de seção, então o ícone é injetado por padrão de texto, igual
// ao resto do enriquecimento pós-render, em vez de depender de um template.
const _FA_NEXT_STEP_RE = /^pr[oó]ximo\s+passo\b/i;

function _faIconizeNextStep(report) {
  report.querySelectorAll("p, li").forEach((el) => {
    if (el.querySelector(":scope > .fa-next-step-ico")) return;
    const firstChild = el.firstChild;
    const isStrongLead =
      firstChild &&
      firstChild.nodeType === Node.ELEMENT_NODE &&
      firstChild.tagName === "STRONG";
    const leadText = (isStrongLead ? firstChild.textContent : el.textContent || "").trim();
    if (!_FA_NEXT_STEP_RE.test(leadText)) return;

    const badge = document.createElement("span");
    badge.className = "fa-next-step-ico";
    badge.innerHTML = _faIcon("arrow-right", 13);

    if (isStrongLead) {
      firstChild.classList.add("fa-next-step-strong");
      firstChild.prepend(badge);
    } else {
      el.prepend(badge);
    }
  });
}

// Extrai e remove a seção "Próximas perguntas sugeridas" do relatório — essas
// perguntas vivem na barra fixa (fa-quick-suggestions), nunca dentro do
// report. O LLM ainda inclui essa seção na narrativa às vezes; aqui ela é
// retirada e reaproveitada em vez de simplesmente descartada.
function _faExtractSuggestedQuestions(report) {
  const heading = Array.from(report.querySelectorAll("h2")).find((h) =>
    /pr[oó]ximas perguntas sugeridas/i.test(h.textContent || ""),
  );
  if (!heading) return null;

  const section = heading.closest(".fa-report-section");
  const list = (section || report).querySelector("ul");
  const items = list
    ? Array.from(list.querySelectorAll("li"))
        .map((li) => li.textContent?.trim() || "")
        .filter(Boolean)
    : [];

  (section || heading).remove();
  return items;
}

// "faixa_risco" / "FAIXA_RISCO" / "valorTotalAberto" -> "Faixa Risco" /
// "Valor Total Aberto". Texto que já tem espaço/acentuação fica intacto —
// só corrige nome técnico de coluna que o LLM esqueceu de traduzir.
const _FA_HEADER_SMALL_WORDS = new Set(["de", "da", "do", "das", "dos", "e", "em", "a", "o"]);

function _faHumanizeHeaderText(text) {
  const raw = String(text || "").trim();
  if (!raw || raw.includes(" ")) return raw;
  const looksLikeIdentifier = /_/.test(raw) || /^[A-Z0-9_]+$/.test(raw) || /[a-z][A-Z]/.test(raw);
  if (!looksLikeIdentifier) return raw;

  const words = raw
    .replace(/_/g, " ")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);

  return words
    .map((w, i) => (i > 0 && _FA_HEADER_SMALL_WORDS.has(w) ? w : w.charAt(0).toUpperCase() + w.slice(1)))
    .join(" ");
}

// Número cru sem separador (ex.: "42681309.28") -> formato pt-BR
// (ex.: "42.681.309,28"). Datas ISO (ex.: "2026-06-20") -> "20/06/2026".
// Texto que já vem formatado (com "." de milhar ou "," decimal) não é tocado.
function _faFormatTableCellText(text) {
  const raw = String(text || "").trim();
  if (!raw) return raw;

  if (/^-?\d+(\.\d+)?$/.test(raw)) {
    const num = Number(raw);
    return num.toLocaleString("pt-BR", {
      minimumFractionDigits: raw.includes(".") ? 2 : 0,
      maximumFractionDigits: 2,
    });
  }

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/);
  if (isoMatch) {
    const [, y, mo, d, h, mi] = isoMatch;
    return h ? `${d}/${mo}/${y} ${h}:${mi}` : `${d}/${mo}/${y}`;
  }

  return raw;
}

function _faHumanizeTable(table) {
  table.querySelectorAll("thead th").forEach((th) => {
    th.textContent = _faHumanizeHeaderText(th.textContent);
  });
  table.querySelectorAll("tbody td").forEach((td) => {
    td.textContent = _faFormatTableCellText(td.textContent);
  });
}

function _faEnhanceReportDom(container, persona = "geral") {
  if (!container) return null;
  const report = container.querySelector(".fa-report");
  if (!report) return null;
  report.dataset.faPersona = String(persona || "geral").toLowerCase();

  const headingsForWrap = Array.from(report.querySelectorAll(":scope > h2"));
  headingsForWrap.forEach((heading) => {
    if (heading.parentElement?.classList.contains("fa-report-section")) return;
    const section = document.createElement("section");
    const { kind } = _faClassifySection(heading.textContent || "");
    section.className = `fa-report-section fa-report-section--${kind}`;
    report.insertBefore(section, heading);
    section.appendChild(heading);

    let cursor = section.nextSibling;
    while (cursor) {
      const next = cursor.nextSibling;
      if (
        cursor.nodeType === Node.ELEMENT_NODE &&
        cursor.tagName === "H2"
      ) {
        break;
      }
      section.appendChild(cursor);
      cursor = next;
    }
  });

  const extractedSuggestions = _faExtractSuggestedQuestions(report);

  report.querySelectorAll("table").forEach((table) => {
    _faHumanizeTable(table);
    if (table.parentElement?.classList.contains("fa-report-table-wrap")) return;
    const wrap = document.createElement("div");
    wrap.className = "fa-report-table-wrap";
    table.parentNode.insertBefore(wrap, table);
    wrap.appendChild(table);
  });

  report.querySelectorAll("h2").forEach((heading) => {
    if (heading.querySelector(".fa-sec-ico")) return;
    const { icon } = _faClassifySection(heading.textContent || "");
    const badge = document.createElement("span");
    badge.className = "fa-sec-ico";
    badge.innerHTML = _faIcon(icon, 13);
    heading.prepend(badge);
  });

  report.querySelectorAll(".fa-report-section--summary p:first-of-type").forEach((p) => {
    p.classList.add("fa-report-lead");
  });

  report.querySelectorAll(".fa-report-section ul").forEach((list) => {
    list.classList.add("fa-report-bullets");
  });

  _faHighlightNumbers(report);
  _faIconizeNextStep(report);
  if (report.dataset.faPersona === "diretor") _faDiretorStatCards(report);
  _faActionCards(report);

  return extractedSuggestions;
}

// Núcleo do efeito de "digitação": revela `text` palavra a palavra, chamando
// `renderChunk(revealedSoFar, isFinal)` a cada tick. Compartilhado entre o
// relatório do bot (markdown) e a pergunta do usuário (texto puro) — mesmo
// ritmo nos dois lados da conversa.
async function _faRevealText(text, renderChunk) {
  const prepared = String(text || "");
  const total = prepared.length;
  if (!total) return;

  // Tokens = palavra + espaço(s)/quebra(s) que a antecedem, preservando o
  // texto original ao serem concatenados. Revelar palavra por palavra (em
  // vez de blocos de caracteres de tamanho fixo) evita o corte no meio da
  // palavra que dava o ar de barra de progresso/robótico.
  const tokens = prepared.match(/\s*\S+/g) || [prepared];
  const consumedLength = tokens.reduce((n, t) => n + t.length, 0);
  if (consumedLength < total) {
    tokens[tokens.length - 1] += prepared.slice(consumedLength);
  }

  const wordCount = tokens.length;
  // Duração total cresce de forma sub-linear com o tamanho do texto: trechos
  // curtos "digitam" rápido, longos não demoram uma eternidade. Teto baixado
  // de 6.500ms pra 1.800ms — respostas longas chegavam a levar 6,5s extras
  // de animação sintética depois que o conteúdo real já estava pronto
  // (ver docs/plans/2026-06-21-tempo-resposta-prd.md, item P0).
  const targetDurationMs = Math.min(1800, Math.max(300, 70 * Math.pow(wordCount, 0.55)));
  const desiredTickMs = 70;
  const wordsPerTick = Math.max(1, Math.round(wordCount / (targetDurationMs / desiredTickMs)));
  const ticks = Math.ceil(wordCount / wordsPerTick);
  const baseDelay = targetDurationMs / ticks;

  const startedAt = Date.now();
  let revealed = "";
  for (let i = 0; i < tokens.length; i += wordsPerTick) {
    const chunk = tokens.slice(i, i + wordsPerTick).join("");
    revealed += chunk;

    // Jitter sutil para fugir do ritmo robótico/uniforme.
    const jitter = baseDelay * (Math.random() * 0.4 - 0.2);
    let delay = Math.max(FA_TYPING_BASE_DELAY_MS, baseDelay + jitter);

    // Pequena pausa após pontuação de frase, como alguém respirando ao
    // digitar — reforça a sensação de pessoa real, não de barra de progresso.
    if (/[.!?:]["'’”)\]]?$/.test(chunk.trimEnd())) {
      delay += 130;
    }

    renderChunk(revealed, false);
    await _faWait(delay);
  }

  const elapsed = Date.now() - startedAt;
  if (elapsed < FA_TYPING_MIN_DURATION_MS) {
    await _faWait(FA_TYPING_MIN_DURATION_MS - elapsed);
  }

  renderChunk(prepared, true);
}

async function _faTypeMarkdownInto(container, sourceText, options = {}) {
  if (!container) return;

  const { escapeInput = false } = options;
  const persona = String((options.data || {}).persona || "geral").trim().toLowerCase();
  const source = String(sourceText || "");
  const prepared = escapeInput ? _escFA(source) : _faPrepareAnswerMarkdown(source, options.data || {});

  if (!prepared) {
    container.innerHTML = `<div class="fa-report"></div>`;
    return;
  }

  // Enriquecimento (cards de seção, chips de número, tabela humanizada...)
  // roda em TODO tick, não só no final — cada tick já reconstrói o HTML do
  // zero a partir do markdown revelado até aquele ponto (innerHTML inteiro
  // é substituído), então reaplicar o enriquecimento nessa base nova é
  // idempotente, sem acúmulo entre ticks. Sem isso, o relatório aparecia
  // "cru" enquanto digitava e só virava a versão formatada de uma vez no
  // fim — salto visível que não existe nos produtos de referência.
  let extractedSuggestions;
  await _faRevealText(prepared, (revealed, isFinal) => {
    const cls = isFinal ? "fa-report" : "fa-report fa-report--typing";
    container.innerHTML = `<div class="${cls}">${_faMdToHtml(revealed)}</div>`;
    extractedSuggestions = _faEnhanceReportDom(container, persona);
    _faFollowGrowingAnswer();
  });

  return extractedSuggestions;
}

// Mesmo efeito de digitação do bot, mas para texto puro (a pergunta do
// usuário) — sem interpretar markdown, só escapando o HTML.
async function _faTypeUserTextInto(container, text) {
  if (!container) return;
  await _faRevealText(_escFA(text), (revealed) => {
    container.innerHTML = revealed;
  });
}

function setFAInteractionLock(locked) {
  const input = document.getElementById("fa-input");
  if (input) {
    input.disabled = !!locked;
  }

  document.querySelectorAll(".fa-topic-card, .fa-suggestion-chip").forEach((el) => {
    if (el instanceof HTMLButtonElement) {
      el.disabled = !!locked;
    }
  });
}

function initFASuggestions() {
  const container = document.getElementById("fa-suggestions");
  if (!container) return;

  const topics = [
    {
      label: "Contas a pagar",
      prompt: "Quero falar sobre contas a pagar",
      gerencia: "contas_a_pagar",
      icon: `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <rect x="3" y="4" width="18" height="16" rx="2"></rect>
          <path d="M7 8h10"></path>
          <path d="M7 12h10"></path>
          <path d="M7 16h6"></path>
        </svg>`,
    },
    {
      label: "Contas a receber",
      prompt: "Quero falar sobre contas a receber",
      gerencia: "contas_receber",
      icon: `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <rect x="3" y="6" width="18" height="12" rx="2"></rect>
          <path d="M3 10h18"></path>
          <path d="M8 14h3"></path>
          <path d="M15 14h1"></path>
        </svg>`,
    },
    {
      label: "Experi\u00eancia do cliente",
      prompt: "Quero falar sobre experi\u00eancia do cliente",
      gerencia: "experiencia_cliente",
      icon: `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 21s-6.5-4.35-9-8.13C1.24 10.3 2.26 6.5 5.8 5.37c2.03-.65 4.18.03 5.2 1.64 1.02-1.61 3.17-2.29 5.2-1.64 3.54 1.13 4.56 4.93 2.8 7.5C18.5 16.65 12 21 12 21z"></path>
        </svg>`,
    },
    {
      label: "Cobran\u00e7a",
      prompt: "Quero falar sobre cobran\u00e7a",
      gerencia: "cobranca",
      icon: `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M12 1v22"></path>
          <path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path>
        </svg>`,
    },
    {
      label: "Fluxo de Caixa",
      prompt: "Quero falar sobre fluxo de caixa",
      gerencia: "fluxo_caixa",
      icon: `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M3 17l6-6 4 4 7-7"></path>
          <path d="M14 8h6v6"></path>
        </svg>`,
    },
    {
      label: "Outros Assuntos Financeiro",
      prompt: "Quero falar sobre outros assuntos financeiros",
      icon: `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="12" cy="12" r="3"></circle>
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1.51-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33h.01a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51h.01a1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82v.01a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"></path>
        </svg>`,
    },
  ];

  // Mesmo molde de personalização do Query Builder (navTo("qb")): admin vê
  // o texto genérico, usuário com gerência cadastrada vê o título/dica
  // falando da própria área — mas, diferente do QB, o grid continua
  // mostrando todas as áreas (Finance Voice é conversacional, não fica
  // travado num único dataset).
  const matchedTopic = !currentUser?.is_admin
    ? topics.find(
        (t) => t.gerencia && t.gerencia === String(currentUser?.gerencia || "").trim().toLowerCase()
      )
    : null;

  const titleEl = document.getElementById("fa-suggestions-title");
  const hintEl = document.getElementById("fa-suggestions-hint");
  if (titleEl) {
    titleEl.textContent = matchedTopic
      ? `Pronto para gerar insights sobre ${_qbCapitalize(matchedTopic.label)}`
      : "Sobre qual área você quer gerar insights?";
  }
  if (hintEl) {
    hintEl.textContent = matchedTopic
      ? `Toque em ${_qbCapitalize(matchedTopic.label)} abaixo para começar, ou explore outra área financeira.`
      : "Escolha uma área abaixo e o Finance Voice já prepara os insights para você.";
  }

  container.innerHTML = "";
  container.classList.add("fa-topic-grid");
  topics.forEach((topic) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "fa-topic-card";
    btn.setAttribute("aria-label", topic.label);
    btn.dataset.prompt = topic.prompt;
    if (topic.gerencia) {
      btn.dataset.gerencia = topic.gerencia;
    }
    btn.innerHTML = `
      <span class="fa-topic-icon" aria-hidden="true">${topic.icon}</span>
      <span class="fa-topic-label">${topic.label}</span>
    `;
    btn.onclick = () => useFASuggestion(btn);
    container.appendChild(btn);
  });

  const welcome = document.getElementById("fa-welcome");
  const welcomeText = welcome?.querySelector("p");
  if (welcomeText) {
    welcomeText.textContent = "Sobre o que voc\u00ea quer falar neste momento?";
  }

  const input = document.getElementById("fa-input");
  if (input) {
    input.placeholder = "Sobre o que voc\u00ea quer falar neste momento?";
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

  // Impede rolar manualmente para dentro do espaçador (vazio) enquanto a
  // resposta ainda não chegou — ver _faClampScrollBelowSpacer.
  const messagesArea = document.getElementById("fa-messages");
  if (messagesArea) {
    messagesArea.addEventListener("scroll", _faClampScrollBelowSpacer, { passive: true });
    messagesArea.addEventListener("scroll", _faMaybeResumeStickToBottom, { passive: true });
    messagesArea.addEventListener("wheel", _faPauseStickToBottom, { passive: true });
    messagesArea.addEventListener("touchmove", _faPauseStickToBottom, { passive: true });
  }

  const jumpBottomBtn = document.getElementById("fa-jump-bottom");
  if (jumpBottomBtn) {
    jumpBottomBtn.addEventListener("click", _faJumpToBottomNow);
  }

  // Delegação global do botão "copiar" do SQL — sem inline JS, sem injeção.
  // closest("button") em vez de checar o target direto: chips de sugestão
  // têm um <span> interno (truncamento do texto) que recebe o clique antes
  // do botão que o envolve.
  document.addEventListener("click", (ev) => {
    const target = ev.target instanceof Element ? ev.target.closest("button") : null;
    if (!target) return;

    // data-followup também aparece em chips fora do Finance Voice (QB
    // reaproveita o estilo .fa-suggestion-chip) — sem esse closest, clicar
    // numa sugestão do Query Builder também disparava sendFAMessage() em
    // segundo plano contra o fa-input escondido.
    const followup = target.closest("#fa-quick-suggestions") && target.getAttribute("data-followup");
    if (followup) {
      const inputEl = document.getElementById("fa-input");
      if (!inputEl || faIsLoading) return;
      inputEl.value = followup;
      autoResizeFAInput(inputEl);
      setFASendButtonState({ disabled: false, loading: false });
      inputEl.focus();
      // Pergunta sugerida/retry: o usuário já demonstrou a intenção ao
      // clicar — assume e envia direto, sem exigir um segundo clique.
      sendFAMessage();
      return;
    }

    const refId = target.getAttribute("data-fa-copy");
    if (!refId) return;
    const pre = document.getElementById(refId);
    if (!pre) return;
    const code = pre.querySelector("code");
    const text = (code ? code.textContent : pre.textContent) || "";
    if (navigator.clipboard) {
      navigator.clipboard.writeText(text).catch(() => {});
    }
    const original = target.textContent;
    target.textContent = "copiado";
    setTimeout(() => {
      target.textContent = original || "copiar";
    }, 1200);
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
  input.value = (btn.dataset.prompt || btn.textContent || "").trim();
  autoResizeFAInput(input);
  setFASendButtonState({ disabled: false, loading: false });
  input.focus();

  const gerencia = btn.dataset.gerencia || "";
  if (gerencia) {
    const label = btn.querySelector(".fa-topic-label")?.textContent?.trim() || "";
    resolveFAGerencia(gerencia, label);
  }
}

// ── Gerência → dataset (aprende o catálogo via rótulo do BigQuery) ──────────
const _faGerenciaResolved = new Set();

// Lê o corpo `text/event-stream` do endpoint de gerência incrementalmente:
// eventos com `phase` atualizam a bolha de status em tempo real; o último
// evento (com `status`) é o resultado final. Eventos podem chegar picotados
// entre chunks — só processa quando acha o separador "\n\n" completo.
async function _faReadGerenciaStream(res, label) {
  if (!res.body) return null;
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let finalEvent = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let sepIdx;
    while ((sepIdx = buffer.indexOf("\n\n")) !== -1) {
      const rawEvent = buffer.slice(0, sepIdx);
      buffer = buffer.slice(sepIdx + 2);
      const dataLine = rawEvent.split("\n").find((l) => l.startsWith("data:"));
      if (!dataLine) continue;
      const payload = JSON.parse(dataLine.slice(5).trim());
      if (payload.phase) {
        faLearningHandle?.setPhase(_faLearningPhaseText(payload.phase, label));
      } else {
        finalEvent = payload;
      }
    }
  }
  return finalEvent;
}

async function resolveFAGerencia(gerencia, label = "") {
  if (_faGerenciaResolved.has(gerencia)) return;

  appendFALearning(label);
  try {
    const res = await fetch("/api/agents/finance_auditor/gerencia", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ gerencia, label }),
    });
    if (!res.ok) return;
    const data = await _faReadGerenciaStream(res, label);
    if (!data || data.status !== "ok") return;

    _faGerenciaResolved.add(gerencia);

    const suggestions = Array.isArray(data.suggestions) ? data.suggestions : [];
    const text = data.message || "Estou pronto para responder perguntas sobre esta área.";
    removeFALearning();
    await appendFAChatTextMessage(text, {
      escapeInput: false,
      data: { skipExecutiveSummary: true },
    });
    _faRenderQuickSuggestions(suggestions);
  } catch (e) {
    // Falha silenciosa — comportamento atual (apenas pré-preencher) é preservado.
  } finally {
    removeFALearning();
  }
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
      <p>Sobre o que voc\u00ea quer falar neste momento?</p>
    </div>`;

  const input = document.getElementById("fa-input");
  if (input) {
    input.value = "";
    input.placeholder = "Sobre o que voc\u00ea quer falar neste momento?";
    autoResizeFAInput(input);
  }
  setFASendButtonState({ disabled: true, loading: false });
  _faRenderQuickSuggestions([]);
}

// Garante espaço suficiente abaixo da última mensagem para a rolagem
// "pegar" no topo mesmo que ainda não haja resposta alguma — sem isso, o
// navegador limita o scroll ao que já existe (mensagem nova + indicador de
// "pensando" juntos quase nunca enchem a tela), e a pergunta não sobe de
// fato. Chamada de novo a cada nova bolha (pensando, resposta, erro) para
// continuar sendo SEMPRE o último filho — senão o espaço vazio fica
// encravado entre a pergunta e o que vem depois, empurrando o resto para
// fora da tela. Removido quando a troca termina (sucesso ou erro).
function _faEnsureScrollSpacer() {
  const area = document.getElementById("fa-messages");
  if (!area) return;
  let spacer = document.getElementById("fa-scroll-spacer");
  if (!spacer) {
    spacer = document.createElement("div");
    spacer.id = "fa-scroll-spacer";
    spacer.setAttribute("aria-hidden", "true");
    spacer.style.flexShrink = "0";
  }
  area.appendChild(spacer); // sempre realocado como último filho
  spacer.style.height = `${area.clientHeight}px`;
}

// Remover o espaçador de golpe encolhe scrollHeight; se a resposta for
// curta (chat rápido, erro, resultado breve) o conteúdo real não enche o
// espaço que o espaçador reservava, e o navegador FORÇA o scrollTop de
// volta pra baixo (clamp) — desfazendo a pergunta que tinha sido fixada no
// topo. Resposta longa (tabela, gráfico) preenche sozinha e mascarava o
// problema; daí o "nem sempre" do comportamento. Em vez de remover tudo,
// encolhe só o que sobra (o que não está "em uso" sustentando a posição
// atual) — sem isso, zero impacto quando o conteúdo já é longo o bastante.
function _faCollapseScrollSpacer() {
  const area = document.getElementById("fa-messages");
  const spacer = document.getElementById("fa-scroll-spacer");
  if (!area || !spacer) return;
  const spacerTopDelta = spacer.getBoundingClientRect().top - area.getBoundingClientRect().top;
  const neededSpacer = Math.max(0, area.clientHeight - spacerTopDelta);
  if (neededSpacer <= 0) {
    spacer.remove();
  } else {
    spacer.style.height = `${neededSpacer}px`;
  }
}

// O espaçador existe só para a ROLAGEM PROGRAMÁTICA ter espaço de sobra —
// não deve virar uma área vazia em que o usuário consiga "passear" rolando
// manualmente para baixo antes da resposta existir. Prende o scroll no fim
// do conteúdo real; rolar para cima continua sempre livre. Só relevante
// quando o scroll NÃO está travado (ver _faLockScroll) — enquanto travado
// nenhuma das duas direções se move mesmo.
function _faClampScrollBelowSpacer() {
  const area = document.getElementById("fa-messages");
  const spacer = document.getElementById("fa-scroll-spacer");
  if (!area || !spacer) return;
  const spacerTopDelta = spacer.getBoundingClientRect().top - area.getBoundingClientRect().top;
  const maxScrollTop = area.scrollTop + spacerTopDelta - area.clientHeight;
  if (maxScrollTop > 0 && area.scrollTop > maxScrollTop) {
    area.scrollTop = maxScrollTop;
  }
}

// Acompanha o crescimento da resposta durante a "digitação" — sem isso,
// uma resposta mais alta que a tela cresce abaixo da área visível e quem
// está lendo perde de vista o texto sendo gerado até rolar manualmente (só
// percebe quando já acabou). Claude/ChatGPT seguem o streaming por padrão e
// só param de seguir se o usuário rolar de propósito — é esse comportamento
// que replicamos, não um scroll fixo que ignora o que está crescendo.
let _faStickToBottom = true;

// Desconta o espaçador (reserva de rolagem, não conteúdo real) do fim da
// área rolável — sem isso "perto do fim" nunca seria verdade enquanto ele
// existir (ver _faEnsureScrollSpacer).
function _faRealContentScrollHeight() {
  const area = document.getElementById("fa-messages");
  if (!area) return 0;
  const spacer = document.getElementById("fa-scroll-spacer");
  const spacerHeight = spacer ? spacer.getBoundingClientRect().height : 0;
  return area.scrollHeight - spacerHeight;
}

// Gesto manual (roda do mouse/toque) é o único sinal confiável de "o
// usuário tomou o controle" — ao contrário do evento "scroll", que também
// dispara pra rolagem PROGRAMÁTICA nossa (_faFollowGrowingAnswer,
// _faScrollMessageToTop...) e daria falso positivo de "usuário saiu" logo
// depois de fixarmos a pergunta no topo (longe do fim, mas por desenho
// nosso, não por ação do usuário).
function _faPauseStickToBottom() {
  _faStickToBottom = false;
  _faUpdateJumpToBottomButton();
}

// "scroll" só REATIVA o acompanhamento (quando o usuário volta perto do fim
// de propósito) — nunca desativa; quem desativa é o gesto manual acima.
function _faMaybeResumeStickToBottom() {
  if (_faStickToBottom) return;
  const area = document.getElementById("fa-messages");
  if (!area) return;
  const distance = _faRealContentScrollHeight() - area.scrollTop - area.clientHeight;
  if (distance < 24) {
    _faStickToBottom = true;
    _faUpdateJumpToBottomButton();
  }
}

// Chamada a cada novo trecho revelado da resposta (ver _faTypeMarkdownInto)
// — empurra a visão só o suficiente pra borda de baixo do conteúdo real
// ficar visível, nunca mais que isso (acompanha o crescimento, não "salta
// pro fim").
function _faFollowGrowingAnswer() {
  const area = document.getElementById("fa-messages");
  if (!area) return;
  if (_faStickToBottom) {
    area.scrollTop = Math.max(0, _faRealContentScrollHeight() - area.clientHeight);
  }
  // Mesmo sem seguir (usuário rolou pra longe), o conteúdo continua
  // crescendo a cada tick — reavalia se já passou a ter overflow real, pro
  // botão "ir para o fim" aparecer no momento certo, não só em scroll manual.
  _faUpdateJumpToBottomButton();
}

// Botão flutuante "ir para o fim" (mesmo padrão Claude/ChatGPT): aparece só
// quando o usuário rolou pra longe do conteúdo que está chegando/já chegou
// E existe de fato conteúdo abaixo da área visível pra valer a pena mostrar
// — sem essa segunda checagem, um gesto de roda numa conversa curta (sem
// overflow real) mostraria um botão sem destino útil.
function _faUpdateJumpToBottomButton() {
  const btn = document.getElementById("fa-jump-bottom");
  const area = document.getElementById("fa-messages");
  if (!btn || !area) return;
  const hasOverflow = _faRealContentScrollHeight() > area.clientHeight + 24;
  btn.hidden = _faStickToBottom || !hasOverflow;
}

function _faJumpToBottomNow() {
  _faStickToBottom = true;
  _faFollowGrowingAnswer();
  _faUpdateJumpToBottomButton();
}

// Enquanto o bot está "pensando"/avaliando a resposta, ninguém deve poder
// rolar a tela em nenhuma direção — a pergunta fica fixa no topo até a
// resposta real começar a chegar. overflow:hidden bloqueia gesto do
// usuário (roda do mouse, toque, teclado, arrastar a barra) mas continua
// permitindo rolagem programática (scrollTo/scrollTop), então a animação
// de _faScrollMessageToTop não é afetada.
function _faLockScroll() {
  const area = document.getElementById("fa-messages");
  if (area) area.style.overflowY = "hidden";
}

function _faUnlockScroll() {
  const area = document.getElementById("fa-messages");
  if (area) area.style.overflowY = "";
}

// Ancora a mensagem (pergunta) no topo da área visível. Instantâneo, não
// animado: um scroll "smooth" depende de animação do navegador rodando
// junto com _faLockScroll() (overflow:hidden) — essa combinação não é
// padronizada entre engines/versões de navegador, e foi descartada depois
// de relatos repetidos de que a rolagem "nem sempre" chegava no topo (não
// reproduzia em Chromium headless, mas o salto instantâneo elimina a
// dependência de timing de animação por completo, então não tem mais como
// dar errado por isso). Garante espaço de sobra via _faEnsureScrollSpacer()
// — chamada só nos pontos em que a resposta final ainda não chegou.
function _faScrollMessageToTop(el) {
  const area = document.getElementById("fa-messages");
  if (!area || !el) return;
  _faEnsureScrollSpacer();
  // getBoundingClientRect (não offsetTop): offsetTop é relativo ao
  // ancestral posicionado mais próximo, que não é necessariamente
  // #fa-messages — usar direto jogava a rolagem pra um ponto sem relação
  // com a posição real da mensagem dentro da área rolável.
  const delta = el.getBoundingClientRect().top - area.getBoundingClientRect().top;
  area.scrollTop = Math.max(0, area.scrollTop + delta - 12);
}

// _faScrollMessageToTop (chamada lá no início, antes da resposta chegar) só
// tem efeito DURADOURO se o conteúdo final preencher o espaço que o
// espaçador reservava. Resposta curta (chat rápido, erro, resultado breve)
// não preenche, _faCollapseScrollSpacer() encolhe a área rolável, e o
// navegador FORÇA o scrollTop de volta pra baixo (clamp) — desfazendo a
// pergunta fixada no topo. Resposta longa (tabela, gráfico) preenche
// sozinha e mascarava o problema, daí o "nem sempre" do comportamento.
// Chamada por último, JÁ SEM espaçador (depois de _faCollapseScrollSpacer):
// o melhor scroll possível com o conteúdo real, SEM tocar no espaçador de
// novo (diferente de _faScrollMessageToTop) — chamar _faEnsureScrollSpacer
// aqui desfaria o encolhimento que _faCollapseScrollSpacer() acabou de calcular.
function _faScrollMessageToTopFinal(el) {
  const area = document.getElementById("fa-messages");
  if (!area || !el) return;
  const delta = el.getBoundingClientRect().top - area.getBoundingClientRect().top;
  area.scrollTop = Math.max(0, area.scrollTop + delta - 12);
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
        <div class="fa-bubble-body"></div>
      </div>
      <div class="fa-msg-time">${_faNow()}</div>
    </div>`;
  area.appendChild(el);
  _faEnsureScrollSpacer();

  const slot = el.querySelector(".fa-bubble-body");
  _faTypeUserTextInto(slot, text);
  return id;
}

const FA_THINKING_PHASES = [
  "Entendendo sua pergunta",
  "Consultando as bases de dados",
  "Cruzando as informações",
  "Validando os resultados",
  "Redigindo a resposta",
];
// Casca compartilhada de uma "bolha de fase": avatar + texto de status que
// muda conforme `setPhase(text)` é chamado. Não decide POR QUE o texto muda
// — isso é do chamador (timer simulado para o "pensando" do chat, eventos
// reais do backend para o "aprendendo" da seleção de gerência).
function _faAppendPhaseBubble(initialText) {
  const area = document.getElementById("fa-messages");
  if (!area) return null;

  const id = `fa-phase-${++faMsgCounter}`;
  const el = document.createElement("div");
  el.id = id;
  el.className = "fa-msg fa-msg-bot";
  el.innerHTML = `
    <div class="fa-msg-avatar">FV</div>
    <div class="fa-msg-main">
      <div class="fa-bubble fa-bubble--thinking">
        <div class="fa-thinking-body" role="status" aria-live="polite">
          <div class="fa-thinking-phase">
            ${initialText}<span class="fa-thinking-dots"><span></span><span></span><span></span></span>
          </div>
          <div class="fa-thinking-track"></div>
        </div>
      </div>
    </div>`;
  area.appendChild(el);
  _faEnsureScrollSpacer();

  return {
    setPhase(text) {
      const phaseEl = document.querySelector(`#${id} .fa-thinking-phase`);
      if (!phaseEl) return;
      phaseEl.innerHTML = `${text}<span class="fa-thinking-dots"><span></span><span></span><span></span></span>`;
    },
    remove() {
      document.getElementById(id)?.remove();
    },
  };
}

function appendFAThinking() {
  removeFAThinking();
  _faLockScroll();
  const handle = _faAppendPhaseBubble(FA_THINKING_PHASES[0]);
  let idx = 0;
  const interval = setInterval(() => {
    if (idx >= FA_THINKING_PHASES.length - 1) {
      // Sem eventos reais aqui (o /analyze não faz streaming) — simulamos o
      // avanço, mas sem repetir o ciclo do início ao chegar na última fase.
      clearInterval(interval);
      return;
    }
    idx += 1;
    handle?.setPhase(FA_THINKING_PHASES[idx]);
  }, 2200);

  faThinkingHandle = {
    remove() {
      clearInterval(interval);
      handle?.remove();
    },
  };
}

function removeFAThinking() {
  _faUnlockScroll();
  if (!faThinkingHandle) return;
  faThinkingHandle.remove();
  faThinkingHandle = null;
}

// Indicador exibido enquanto a gerência escolhida está sendo resolvida e o
// catálogo (tabelas/colunas/descrições) está sendo aprendido — evita a tela
// "vazia" entre o clique no cartão e a confirmação/sugestões. O texto muda
// conforme eventos reais de fase chegam do backend (ver _gerenciaSseFetch),
// não num timer simulado.
function _faLearningPhaseText(phase, label) {
  const safeLabel = label ? _escFA(label) : "";
  switch (phase) {
    case "catalog":
      return "Lendo tabelas, colunas e descrições";
    case "suggestions":
      return "Preparando sugestões de perguntas";
    default:
      return `Estou aprendendo o produto de dados${safeLabel ? ` de ${safeLabel}` : ""}, aguarde`;
  }
}

function appendFALearning(label) {
  removeFALearning();
  faLearningHandle = _faAppendPhaseBubble(_faLearningPhaseText(null, label));
}

function removeFALearning() {
  if (!faLearningHandle) return;
  faLearningHandle.remove();
  faLearningHandle = null;
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
  _faEnsureScrollSpacer();
}

async function appendFAChatTextMessage(text, opts = {}) {
  const area = document.getElementById("fa-messages");
  if (!area) return;

  const el = document.createElement("div");
  el.className = "fa-msg fa-msg-bot";
  el.innerHTML = `
    <div class="fa-msg-avatar">FV</div>
    <div class="fa-msg-main">
      <div class="fa-bubble fa-bubble--bot">
        <div class="fa-bubble-body"><div class="fa-report-slot"></div></div>
      </div>
      <div class="fa-msg-time">${_faNow()}</div>
    </div>`;
  area.appendChild(el);
  _faEnsureScrollSpacer();

  const slot = el.querySelector(".fa-report-slot");
  await _faTypeMarkdownInto(slot, text, { escapeInput: true, ...opts });
}

// Verdadeiro quando nenhuma capability "produtora de resposta" teve sucesso —
// usado tanto para o chip de status quanto para decidir entre mostrar
// "próximas perguntas" (sugestão de avanço) ou suprimi-las após uma falha.
function _faIsFailedResult(data) {
  const toolResults = Array.isArray(data.tool_results) ? data.tool_results : [];
  if (!toolResults.length) return false;
  const okCount = toolResults.filter((r) => r && r.ok).length;
  return !!data.error || okCount === 0;
}

async function appendFABotMessage(data) {
  const area = document.getElementById("fa-messages");
  if (!area) return;

  const id = `fa-bot-${++faMsgCounter}`;
  const el = document.createElement("div");
  el.id = id;
  el.className = "fa-msg fa-msg-bot";

  const persona = String(data.persona || "").trim();
  const isFailed = _faIsFailedResult(data);
  const reportSlotId = `${id}-report`;

  el.innerHTML = `
    <div class="fa-msg-avatar">FV</div>
    <div class="fa-msg-main fa-msg-main--report">
      <div class="fa-bubble fa-bubble--bot fa-bubble--report">
        <div class="fa-bubble-body">
          <div class="fa-report-slot" id="${reportSlotId}"></div>
          <div class="fa-art-slot"></div>
        </div>
      </div>
      <div class="fa-msg-time">${_faNow()}</div>
    </div>`;

  area.appendChild(el);
  _faEnsureScrollSpacer();

  // A narrativa e digitada primeiro - so depois os cartoes de dados entram em cena.
  const slot = el.querySelector(".fa-report-slot");
  const extractedSuggestions = await _faTypeMarkdownInto(slot, data.markdown_report || data.chat_answer || "", { data });

  if (isFailed) {
    _faRenderQuickSuggestions([]);
    return id;
  }

  const artifactsHtml = _faDetailsHtml(data);
  if (artifactsHtml) {
    const artSlot = el.querySelector(".fa-art-slot");
    if (artSlot) {
      artSlot.innerHTML = artifactsHtml;
      _faExecuteInlineScripts(artSlot);
      // Tabelas/gráficos chegam depois do texto e também alteram a altura —
      // sem isso, quem está seguindo o crescimento perde justamente a parte
      // com o gráfico, que via de regra é o que a pergunta pediu.
      _faFollowGrowingAnswer();
    }
  }

  // Legenda de volume/custo/tokens s\u00f3 aparece depois que a resposta (e os
  // artefatos) terminaram de renderizar \u2014 n\u00e3o durante a digita\u00e7\u00e3o.
  const metaCaption = _faMetaCaptionHtml(data);
  if (metaCaption) {
    el.querySelector(".fa-msg-time")?.insertAdjacentHTML("afterend", metaCaption);
  }

  const suggestions = extractedSuggestions && extractedSuggestions.length
    ? extractedSuggestions
    : _faSuggestedFollowups(data.original_query || data.query || "", persona);
  _faRenderQuickSuggestions(suggestions);
  return id;
}

// Legenda discreta de volume/custo/tokens sob o hor\u00e1rio \u2014 s\u00f3 aparece quando
// h\u00e1 consumo de BigQuery ou LLM a relatar.
function _faMetaCaptionHtml(data) {
  const toolResults = Array.isArray(data.tool_results) ? data.tool_results : [];

  let bytesTotal = 0;
  let costTotal = 0;
  for (const r of toolResults) {
    const p = (r && r.payload) || {};
    if (typeof p.bytes_processed === "number") bytesTotal += p.bytes_processed;
    if (typeof p.estimated_cost_usd === "number") costTotal += p.estimated_cost_usd;
  }
  const totalTokens = Number(data.token_usage?.total_tokens) || 0;

  if (!bytesTotal && !totalTokens) return "";

  const parts = [];
  if (bytesTotal) parts.push(`${fmtBytes(bytesTotal)} processados`);
  if (costTotal) parts.push(fmtUSD(costTotal));
  if (totalTokens) parts.push(`${totalTokens.toLocaleString("pt-BR")} tokens`);

  return `<div class="fa-meta-caption">${parts.join(" \u00b7 ")}</div>`;
}


// Mostra APENAS artefatos que respondem à pergunta (tabelas finais, gráficos,
// forecast, anexos). Esconde artefatos de steps preparatórios
// (bq_list_datasets, bq_list_tables, bq_get_schema) e SQL/schema técnicos.
// "stats_describe" fica de fora de propósito: a tabela de estatística
// descritiva é insumo interno do Composer, não algo a exibir como cartão —
// o relatório já traduz o que importa dela em prosa.
const _FA_ANSWER_CAPS = new Set([
  "text_to_sql",
  "bq_query",
  "metric_execute",
  "viz_spec",
  "forecast_simple",
  "attachment_analyze",
  "org_fact_recall",
]);

// `el.innerHTML = htmlComStringTag` insere o <script> no DOM mas o browser
// NUNCA o executa (proteção padrão contra injeção) — é por isso que o card
// de gráfico (vega_lite) ficava em branco, sem erro nenhum: o script que
// chama vegaEmbed simplesmente nunca rodava. Recriar cada <script> como
// elemento novo e reinseri-lo força a execução.
function _faExecuteInlineScripts(container) {
  if (!container) return;
  container.querySelectorAll("script").forEach((oldScript) => {
    const newScript = document.createElement("script");
    Array.from(oldScript.attributes).forEach((attr) => newScript.setAttribute(attr.name, attr.value));
    newScript.textContent = oldScript.textContent;
    oldScript.replaceWith(newScript);
  });
}

function _faDetailsHtml(data) {
  const toolResults = Array.isArray(data.tool_results) ? data.tool_results : [];
  const artifacts = Array.isArray(data.artifacts) ? data.artifacts : [];
  if (artifacts.length === 0) return "";

  const answerArtifacts = artifacts.filter((a) => {
    if (a && a.type === "audio") return true;
    const stepIdx = typeof a.step_index === "number" ? a.step_index : -1;
    if (stepIdx < 0 || stepIdx >= toolResults.length) return false;
    const cap = (toolResults[stepIdx] || {}).capability;
    if (!_FA_ANSWER_CAPS.has(cap)) return false;
    // SQL e schema são detalhes técnicos — escondemos por padrão.
    if (a.type === "sql" || a.type === "schema") return false;
    // Dump bruto da query: a narrativa já cobre esse dado — mostrar de novo
    // aqui só duplica informação.
    if (a.type === "table" && a.title === "Resultado da query") return false;
    return true;
  });
  if (!answerArtifacts.length) return "";

  const html = answerArtifacts
    .map((a) => _faRenderArtifact(a))
    .filter(Boolean)
    .join("");
  if (!html) return "";
  return `<div class="fa-answer-artifacts">${html}</div>`;
}

// Realce leve de SQL: keywords/strings/números/comentários em <span>.
function _faHighlightSql(sql) {
  const escaped = _escFA(sql);
  const KW =
    "(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT|JOIN|LEFT|RIGHT|INNER|OUTER|" +
    "ON|AS|AND|OR|NOT|IN|IS|NULL|WITH|UNION|ALL|DISTINCT|CASE|WHEN|THEN|ELSE|END|" +
    "COUNT|SUM|AVG|MIN|MAX|CAST|DATE|TIMESTAMP|BETWEEN|EXISTS|LIKE|ASC|DESC|OVER|" +
    "PARTITION BY)";
  return escaped
    .replace(/(--[^\n]*)/g, '<span class="com">$1</span>')
    .replace(/('[^']*')/g, '<span class="str">$1</span>')
    .replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="num">$1</span>')
    .replace(new RegExp("\\b" + KW + "\\b", "gi"), '<span class="kw">$1</span>');
}

// O resto do produto já formata número/data em pt-BR (toLocaleString); sem
// isso o Vega usava o locale en-US padrão (",1234.5" / "Jan 2026") e o
// gráfico destoava visualmente do restante da resposta. `vega.formatLocale`
// e `vega.timeFormatLocale` setam o locale padrão pra toda a lib de uma vez
// — não existe campo "locale" no spec do Vega-Lite, então isso tem que
// rodar uma vez no frontend antes do primeiro vegaEmbed.
let _faVegaLocaleReady = false;
function _faEnsureVegaLocale() {
  if (_faVegaLocaleReady || !window.vega) return;
  try {
    window.vega.formatLocale({
      decimal: ",",
      thousands: ".",
      grouping: [3],
      currency: ["R$ ", ""],
    });
    window.vega.timeFormatLocale({
      dateTime: "%A, %e de %B de %Y. %X",
      date: "%d/%m/%Y",
      time: "%H:%M:%S",
      periods: ["AM", "PM"],
      days: ["domingo", "segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado"],
      shortDays: ["dom", "seg", "ter", "qua", "qui", "sex", "sáb"],
      months: [
        "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
      ],
      shortMonths: ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"],
    });
    _faVegaLocaleReady = true;
  } catch (e) {
    // Sem locale custom, o gráfico ainda renderiza (só em en-US) — não é
    // motivo para quebrar o card.
  }
}

// Fallback de erro do gráfico Vega-Lite: escapa o dump do spec (pode conter
// strings vindas de dados de query) antes de ir pro innerHTML — sem isso um
// valor de coluna com "</pre><script>" seria executado como HTML/JS real.
function _faChartErrorHtml(message, spec) {
  const safeMsg = _escFA(message || "Erro desconhecido");
  let dump = "";
  if (spec) {
    try {
      dump = _escFA(JSON.stringify(spec, null, 2));
    } catch (e) {
      dump = "";
    }
  }
  return (
    `<div class="fa-chart-error">` +
    `<span class="fa-chart-error-icon" aria-hidden="true">${_faIcon("alert-triangle", 14)}</span>` +
    `<div class="fa-chart-error-body">` +
    `<div class="fa-chart-error-title">Não foi possível renderizar o gráfico</div>` +
    `<div class="fa-chart-error-msg">${safeMsg}</div>` +
    (dump ? `<pre class="fa-chart-error-dump">${dump}</pre>` : "") +
    `</div></div>`
  );
}

// Casca comum de um cartão de artefato: ícone + título + meta opcional no
// cabeçalho, corpo customizável. `index` alimenta o atraso do efeito de
// entrada escalonado (--i) definido em CSS.
function _faArtCard(index, { icon, title, meta = "", extraHead = "", bodyHtml, padded = false }) {
  const bodyClass = padded ? "fa-art-card-body fa-art-card-body--padded" : "fa-art-card-body";
  const metaHtml = meta ? `<span class="fa-art-card-meta">${_escFA(meta)}</span>` : "";
  return (
    `<div class="fa-art-card" style="--i:${index}">` +
    `<div class="fa-art-card-head">` +
    `<span class="fa-art-card-icon" aria-hidden="true">${icon}</span>` +
    `<span class="fa-art-card-title">${_escFA(title)}</span>` +
    metaHtml +
    extraHead +
    `</div>` +
    `<div class="${bodyClass}">${bodyHtml}</div>` +
    `</div>`
  );
}

// Renderiza um artefato individual conforme o tipo.
function _faRenderArtifact(a, index = 0) {
  if (!a || typeof a !== "object") return "";
  const type = String(a.type || "");
  switch (type) {
    case "table": {
      const cols = Array.isArray(a.columns) ? a.columns : [];
      const rows = Array.isArray(a.rows) ? a.rows : [];
      if (!cols.length || !rows.length) return "";
      const head = cols.map((c) => `<th>${_escFA(c)}</th>`).join("");
      const body = rows
        .slice(0, 25)
        .map(
          (r) =>
            "<tr>" +
            cols
              .map(
                (c) =>
                  `<td>${_escFA(r[c] == null ? "" : String(r[c]))}</td>`,
              )
              .join("") +
            "</tr>",
        )
        .join("");
      const moreNote =
        rows.length > 25
          ? `<div class="fa-art-more-note">+${rows.length - 25} linha(s) ocultas</div>`
          : "";
      return _faArtCard(index, {
        icon: _faIcon("grid", 13),
        title: a.title || "Tabela",
        meta: `${rows.length} linha${rows.length === 1 ? "" : "s"}`,
        bodyHtml:
          `<div class="fa-art-table-scroll"><table class="fa-artifact-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>` +
          moreNote,
      });
    }
    case "sql": {
      const sql = String(a.sql || "");
      if (!sql) return "";
      // Botão "copiar" sem injeção: lê do <code> irmão via DOM, não do JS inline.
      const sqlId = `fa-sql-${faMsgCounter}-${Math.random().toString(36).slice(2, 7)}`;
      return _faArtCard(index, {
        icon: _faIcon("code", 13),
        title: "SQL executado",
        extraHead: `<button class="fa-art-copy" type="button" data-fa-copy="${sqlId}">copiar</button>`,
        bodyHtml: `<pre id="${sqlId}" class="fa-sql"><code>${_faHighlightSql(sql)}</code></pre>`,
      });
    }
    case "schema": {
      const text = String(a.text || "");
      if (!text) return "";
      return _faArtCard(index, {
        icon: _faIcon("database", 13),
        title: `Schema: ${a.table_ref || ""}`,
        bodyHtml: `<pre class="fa-art-schema"><code>${_escFA(text)}</code></pre>`,
      });
    }
    case "vega_lite": {
      const spec = a.spec || {};
      const vid = `fa-vega-${faMsgCounter}-${Math.random().toString(36).slice(2, 8)}`;
      const specJson = JSON.stringify(spec).replace(/</g, "\\u003c");
      return _faArtCard(index, {
        icon: _faIcon("bar-chart", 13),
        title: a.title ? `Gráfico: ${a.title}` : "Gráfico",
        padded: true,
        bodyHtml:
          `<div class="fa-chart-frame"><div id="${vid}" class="fa-chart-canvas"></div></div>` +
          `<script>(function(){var el=document.getElementById('${vid}');try{_faEnsureVegaLocale();var s=${specJson};` +
          `if(window.vegaEmbed){window.vegaEmbed('#${vid}',s,{actions:false,renderer:'svg'})` +
          `.catch(function(e){el.innerHTML=_faChartErrorHtml(e&&e.message?e.message:String(e),s);});}` +
          `else{el.innerHTML=_faChartErrorHtml('Biblioteca de gráficos não carregada.',s);}}` +
          `catch(e){el.innerHTML=_faChartErrorHtml(e&&e.message?e.message:String(e),null);}})();<\/script>`,
      });
    }
    case "audio": {
      const assetId = String(a.audio_asset_id || "").trim();
      if (!assetId) return "";
      const domId = `fa-audio-${faMsgCounter}-${Math.random().toString(36).slice(2, 8)}`;
      const transcript = String(a.text || "").trim();
      const transcriptHtml = transcript
        ? `<details class="fa-audio-transcript"><summary>Ver roteiro</summary><pre><code>${_escFA(transcript)}</code></pre></details>`
        : "";
      return _faArtCard(index, {
        icon: _faIcon("volume-2", 13),
        title: a.title || "Podcast",
        padded: true,
        bodyHtml:
          `<audio id="${domId}" class="fa-audio-player" controls preload="none"></audio>` +
          `<div class="fa-art-more-note">Se preferir, use o menu do player para baixar o áudio.</div>` +
          transcriptHtml +
          `<script>(async function(){try{var el=document.getElementById('${domId}');if(!el)return;var resp=await fetch('/api/agents/finance_auditor/podcast/${encodeURIComponent(assetId)}',{headers:authHeaders()});if(!resp.ok)throw new Error('HTTP '+resp.status);var blob=await resp.blob();var url=URL.createObjectURL(blob);el.src=url;el.addEventListener('ended',function(){setTimeout(function(){URL.revokeObjectURL(url);},15000);},{once:true});}catch(e){var el=document.getElementById('${domId}');if(el){el.insertAdjacentHTML('afterend','<div class="data-warn-banner">Não foi possível carregar o áudio do podcast.</div>');}}})();<\/script>`,
      });
    }
    default:
      return "";
  }
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
      // separator row (align row) — só aparece logo após o cabeçalho.
      if (cells.every((c) => /^[-:]+$/.test(c))) continue;

      if (!inTable) {
        closeList();
        // Lookahead: a linha seguinte sendo separadora confirma que ESTA
        // linha é o cabeçalho — sem isso a linha de título era descartada
        // (nunca virava <th>, a tabela nascia sem nenhuma coluna nomeada).
        const nextLine = (lines[i + 1] || "").trimEnd();
        const nextCells =
          nextLine.startsWith("|") && nextLine.endsWith("|")
            ? nextLine.slice(1, -1).split("|").map((c) => c.trim())
            : [];
        const nextIsSeparator = nextCells.length > 0 && nextCells.every((c) => /^[-:]+$/.test(c));

        if (nextIsSeparator) {
          const ths = cells.map((c) => `<th>${inline(c)}</th>`).join("");
          out.push(`<table><thead><tr>${ths}</tr></thead><tbody>`);
        } else {
          // Sem separador na próxima linha: não é cabeçalho formal — trata
          // como a primeira linha de dados em vez de descartar.
          out.push("<table><thead></thead><tbody>");
          const tds = cells.map((c) => `<td>${inline(c)}</td>`).join("");
          out.push(`<tr>${tds}</tr>`);
        }
        inTable = true;
        continue;
      }
      const tds = cells.map((c) => `<td>${inline(c)}</td>`).join("");
      out.push(`<tr>${tds}</tr>`);
      continue;
    } else if (inTable) {
      closeTable();
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

  if (!text || faIsLoading) return;

  input.value = "";
  if (input) {
    input.style.height = "auto";
  }
  setFAInteractionLock(true);
  setFASendButtonState({ disabled: true, loading: true });
  _faRenderQuickSuggestions([]);

  // Nova pergunta = nova intenção de acompanhar a resposta, mesmo que o
  // usuário tenha parado de seguir a resposta anterior rolando manualmente.
  _faStickToBottom = true;
  _faUpdateJumpToBottomButton();

  const userMsgId = appendFAUserMessage(text);
  appendFAThinking();
  // Só rola depois que a pergunta E o "pensando" já estão no DOM — senão o
  // alvo da rolagem fica defasado pela mudança de layout que vem a seguir.
  _faScrollMessageToTop(document.getElementById(userMsgId));

  faIsLoading = true;

  try {
    const res = await fetch("/api/agents/finance_auditor/analyze", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        query: text,
        project_id: null,
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
    if (data && typeof data === "object") {
      data.original_query = text;
    }
    removeFAThinking();
    // Reconfirma a pergunta no topo depois que a bolha de "pensando" sai do
    // DOM. Pra respostas rápidas (ex.: response_mode "chat"), o scroll
    // suave disparado em _faScrollMessageToTop logo após enviar a pergunta
    // ainda podia estar animando quando removeFAThinking() destrava o
    // scroll e muda o layout — a mutação no meio da animação podia cortá-la
    // antes de chegar no topo, daí o "nem sempre sobe até o limite".
    _faScrollMessageToTopFinal(document.getElementById(userMsgId));

    if (data.status === "error") {
      appendFAErrorMessage(
        data.error || "Não foi possível realizar a análise.",
      );
    } else if (data.status === "awaiting_approval") {
      const msgId = await appendFABotMessage(data);
      _faAppendPodcastConfirm(msgId, data.thread_id, data.message);
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
    _faScrollMessageToTopFinal(document.getElementById(userMsgId));
    appendFAErrorMessage(prettifyErrorMessage(e.message));
  } finally {
    _faCollapseScrollSpacer();
    // Reconfirma a pergunta no topo só quando a resposta NÃO precisou de
    // acompanhamento de scroll (curta, cabe numa tela) — é o cenário do bug
    // original (espaçador encolhendo e puxando o scroll de volta). Resposta
    // longa que o auto-follow já levou pro fim (_faStickToBottom ainda true
    // + overflow real) NÃO deve ser jogada de volta pro topo aqui — isso
    // desfaria o "seguir o crescimento" bem na hora em que ele mais importa.
    const _faMsgsArea = document.getElementById("fa-messages");
    const _faHasOverflow = _faMsgsArea && _faRealContentScrollHeight() > _faMsgsArea.clientHeight + 24;
    if (_faStickToBottom && !_faHasOverflow) {
      _faScrollMessageToTopFinal(document.getElementById(userMsgId));
    }
    faIsLoading = false;
    setFAInteractionLock(false);
    setFASendButtonState({
      disabled: !input?.value.trim(),
      loading: false,
    });
    input?.focus();
  }
}

// Bloco de confirmação humana (HITL) antes de gerar o podcast — o grafo do
// Finance Voice pausa em node_podcast_builder via interrupt() aguardando
// essa decisão, então nada de TTS é gasto até o usuário clicar.
function _faAppendPodcastConfirm(msgId, threadId, message) {
  const msgEl = document.getElementById(msgId);
  const slot = msgEl?.querySelector(".fa-art-slot");
  if (!slot || !threadId) return;

  const confirmId = `${msgId}-podcast-confirm`;
  slot.insertAdjacentHTML(
    "beforeend",
    `<div class="fa-podcast-confirm" id="${confirmId}">` +
      `<span class="fa-podcast-confirm-msg">${_escFA(message || "Deseja gerar um podcast desta análise?")}</span>` +
      `<div class="fa-podcast-confirm-actions">` +
      `<button type="button" class="fa-podcast-confirm-btn fa-podcast-confirm-btn--primary" data-decision="approve">Gerar podcast</button>` +
      `<button type="button" class="fa-podcast-confirm-btn fa-podcast-confirm-btn--ghost" data-decision="skip">Agora não</button>` +
      `</div></div>`,
  );

  document.getElementById(confirmId)
    ?.querySelectorAll("[data-decision]")
    .forEach((btn) => {
      btn.addEventListener("click", () => {
        _faResumePodcast(threadId, btn.getAttribute("data-decision"), msgId);
      });
    });

  _faFollowGrowingAnswer();
}

async function _faResumePodcast(threadId, decision, msgId) {
  const confirmId = `${msgId}-podcast-confirm`;
  const block = document.getElementById(confirmId);
  const buttons = block ? Array.from(block.querySelectorAll("[data-decision]")) : [];
  const primaryBtn = block?.querySelector('[data-decision="approve"]');
  const primaryLabel = primaryBtn?.textContent;

  buttons.forEach((btn) => { btn.disabled = true; });
  if (decision === "approve" && primaryBtn) primaryBtn.textContent = "Gerando podcast...";

  try {
    const res = await fetch("/api/agents/finance_auditor/resume", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ thread_id: threadId, decision }),
    });

    if (res.status === 401) {
      doLogout();
      return;
    }

    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Erro ao processar o pedido de podcast.");

    block?.remove();

    const artSlot = document.getElementById(msgId)?.querySelector(".fa-art-slot");
    if (!artSlot) return;

    if (decision === "approve") {
      const audioArtifact = (Array.isArray(data.artifacts) ? data.artifacts : []).find(
        (a) => a && a.type === "audio" && a.kind === "analysis_podcast",
      );
      if (audioArtifact) {
        artSlot.insertAdjacentHTML("beforeend", _faRenderArtifact(audioArtifact));
        _faExecuteInlineScripts(artSlot);
      } else {
        const warning = (data.warnings || [])[0] || "Não foi possível gerar o podcast agora.";
        artSlot.insertAdjacentHTML("beforeend", `<div class="fa-art-more-note">${_escFA(warning)}</div>`);
      }
    } else {
      artSlot.insertAdjacentHTML(
        "beforeend",
        `<div class="fa-art-more-note">Ok, não vou gerar o podcast desta análise.</div>`,
      );
    }
    _faFollowGrowingAnswer();
  } catch (e) {
    buttons.forEach((btn) => { btn.disabled = false; });
    if (primaryBtn && primaryLabel) primaryBtn.textContent = primaryLabel;
    if (block) {
      let errEl = block.querySelector(".fa-podcast-confirm-error");
      if (!errEl) {
        block.insertAdjacentHTML("beforeend", `<div class="fa-podcast-confirm-error"></div>`);
        errEl = block.querySelector(".fa-podcast-confirm-error");
      }
      if (errEl) errEl.textContent = prettifyErrorMessage(e.message);
    }
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

// ── Shared project/dataset select helpers ─────────────────────────────────

async function _loadProjectsIntoSelect(selectId, onLoaded) {
  const sel = document.getElementById(selectId);
  if (!sel) return;
  sel.innerHTML = '<option value="">Carregando projetos...</option>';
  sel.disabled = true;
  try {
    const resp = await fetch("/api/schema-explorer/projects", {
      headers: { Authorization: "Bearer " + (typeof token !== "undefined" ? token : "") },
    });
    if (!resp.ok) throw new Error("fail");
    const projects = await resp.json();
    if (!projects.length) throw new Error("empty");
    sel.innerHTML =
      '<option value="">Selecione um projeto</option>' +
      projects.map((p) => `<option value="${p}">${p}</option>`).join("");
    sel.disabled = false;
    if (typeof onLoaded === "function") onLoaded(projects);
  } catch (_) {
    sel.innerHTML = '<option value="">Erro ao carregar projetos</option>';
    sel.disabled = false;
  }
}

async function _loadDatasetsIntoSelect(project, selectId, preselect) {
  const sel = document.getElementById(selectId);
  if (!sel) return;
  if (!project) {
    sel.innerHTML = '<option value="">Selecione um projeto primeiro</option>';
    sel.disabled = true;
    return;
  }
  sel.innerHTML = '<option value="">Carregando datasets...</option>';
  sel.disabled = true;
  try {
    const resp = await fetch(
      `/api/schema-explorer/datasets?project_id=${encodeURIComponent(project)}`,
      {
        headers: { Authorization: "Bearer " + (typeof token !== "undefined" ? token : "") },
      },
    );
    if (!resp.ok) throw new Error("fail");
    const datasets = await resp.json();
    sel.innerHTML =
      '<option value="">Selecione um dataset</option>' +
      datasets.map((d) => `<option value="${d}">${d}</option>`).join("");
    sel.disabled = false;
    if (preselect) {
      sel.value = preselect;
    }
  } catch (_) {
    sel.innerHTML = '<option value="">Erro ao carregar datasets</option>';
    sel.disabled = false;
  }
}

// ── Schema Explorer select handlers ───────────────────────────────────────

function neoOnProjectChange() {
  const project = document.getElementById("neo-project")?.value.trim();
  _neoSetBtn(false);
  _neoDsIndicator("idle");
  _loadDatasetsIntoSelect(project, "neo-dataset");
}

function _neoWireValidation() {
  // Selects fire "change" — validation is wired via onchange attributes in HTML
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
      _neoSelectNode(d.id, edges, false);
    })
    .on("dblclick", (ev, d) => {
      ev.stopPropagation();
      _neoSelectNode(d.id, edges, true);
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
function _neoSelectNode(id, edges, openDetail = false) {
  _neo.selectedNode = id;
  _neoApplyDim(id, edges);
  _neo.nodeG?.classed("neo-selected", (n) => n.id === id);
  if (openDetail) _neoOpenDetail(id);
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
    unknown: "SEM CLASSIFICA\u00C7\u00C3O",
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
      <span class="neo-dp-badge" style="background:${TC[node.table_type] ?? "#3d5276"}">${TL[node.table_type] ?? "SEM CLASSIFICAÇÃO"}</span>
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
    <div class="neo-dp-next">
      <div class="neo-dp-next-title">Qual ação deseja executar agora?</div>
      <div class="neo-dp-next-actions">
        <a class="neo-dp-action-link" href="#" onclick="neoGoQB('${_neo.dsRef}','${node.id}');return false;">
          <span class="neo-dp-action-arrow">→</span>Gerar insights analíticos
        </a>
        <a class="neo-dp-action-link" href="#" onclick="neoGoAudit('${_neo.dsRef}','${node.id}');return false;">
          <span class="neo-dp-action-arrow">→</span>Gerar diagnóstico operacional
        </a>
      </div>
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
  const project = parts[0] || "";
  const dataset = parts[1] || "";

  // Vem do Explorador de Esquema com projeto/dataset/tabela já definidos —
  // pula o seletor de gerência e a tela de Configuração por completo
  // (nunca mais aparecem no QB) e garante que solicitação/botão, que a
  // orientação de "sem gerência" pode ter escondido, voltem a aparecer.
  _qbPickerResolved = true;
  document.getElementById("qb-gerencia-picker")?.style.setProperty("display", "none");
  document.getElementById("qb-request-field")?.style.removeProperty("display");
  document.getElementById("qb-btn")?.style.removeProperty("display");

  navTo("qb");
  _setQBGerenciaMode(true);

  if (project) {
    const pi = document.getElementById("qb-project");
    if (pi) {
      if (![...pi.options].some((o) => o.value === project)) {
        pi.add(new Option(project, project));
      }
      pi.value = project;
    }
    _loadDatasetsIntoSelect(project, "qb-dataset", dataset).then(() => {
      validateQBDatasetHint().then(() => {
        _loadQBSuggestions(project, dataset, tableId);
      });
    });
  }
}

function neoGoAudit(dsRef, tableId) {
  const parts = dsRef.split(".");
  const dataset = parts[1] || "-";

  navTo("audit");

  const input = document.getElementById("fa-input");
  if (!input) return;

  input.value = `Gerar um diagnostico da tabela ${tableId} no dataset ${dataset}. Quero sinais de risco, anomalias potenciais, hipoteses e proximos passos de investigacao.`;
  autoResizeFAInput(input);
  setFASendButtonState({
    disabled: !input.value.trim(),
    loading: false,
  });
  input.focus();
}

function _escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

async function _loadQBSuggestions(projectId, datasetHint, tableId) {
  const block = document.getElementById("qb-suggestions-block");
  if (!block) return;

  block.hidden = false;
  block.innerHTML = `<span class="qb-sugg-loading">${_faIcon("sparkle", 13)}Gerando sugestões com IA<span class="fa-thinking-dots"><span></span><span></span><span></span></span></span>`;

  try {
    const res = await fetch("/api/agents/query_build/suggestions", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        project_id: projectId,
        dataset_hint: datasetHint,
        table_id: tableId,
      }),
    });

    if (res.status === 401) {
      doLogout();
      return;
    }
    if (!res.ok) throw new Error("Falha ao buscar sugestoes.");

    const data = await res.json();
    const suggestions = Array.isArray(data.suggestions) ? data.suggestions : [];
    _renderQBSuggestionChips(suggestions);
  } catch (e) {
    block.hidden = true;
    block.innerHTML = "";
  }
}

// Mesma barra de chips do Finance Voice (fa-quick-suggestions/fa-suggestion-chip)
// — até 6 sugestões, as 4 primeiras visíveis e as 2 últimas atrás de "Mostrar mais".
function _renderQBSuggestionChips(suggestions) {
  const bar = document.getElementById("qb-suggestions-block");
  if (!bar) return;

  const list = (Array.isArray(suggestions) ? suggestions : [])
    .map((s) => String(s || "").trim())
    .filter(Boolean)
    .slice(0, 6);

  if (!list.length) {
    bar.innerHTML = "";
    bar.hidden = true;
    return;
  }

  const chipHtml = (text) =>
    `<button type="button" class="fa-suggestion-chip" data-text="${_escapeHtml(text)}" data-followup="${_escapeHtml(text)}" onclick="_selectQBSuggestion(this)">` +
    `<span class="fa-suggestion-chip-text">${_escapeHtml(text)}</span></button>`;

  const visible = list.slice(0, 4);
  const extra = list.slice(4);
  const extraHtml = extra.length
    ? `<span class="fa-suggestions-extra" id="qb-suggestions-extra" hidden>${extra.map(chipHtml).join("")}</span>` +
      `<button type="button" class="fa-suggestions-toggle" id="qb-suggestions-toggle" aria-expanded="false">Mostrar mais ${_faIcon("chevron-down", 11)}</button>`
    : "";

  bar.innerHTML =
    `<span class="fa-suggestions-icon" aria-hidden="true">${_faIcon("sparkle", 13)}</span>` +
    visible.map(chipHtml).join("") +
    extraHtml;
  bar.hidden = false;

  const toggle = document.getElementById("qb-suggestions-toggle");
  if (!toggle) return;
  toggle.addEventListener("click", () => {
    const extraEl = document.getElementById("qb-suggestions-extra");
    if (!extraEl) return;
    const expanded = toggle.getAttribute("aria-expanded") === "true";
    extraEl.hidden = expanded;
    toggle.setAttribute("aria-expanded", String(!expanded));
    toggle.innerHTML = expanded
      ? `Mostrar mais ${_faIcon("chevron-down", 11)}`
      : `Mostrar menos ${_faIcon("chevron-up", 11)}`;
  });
}

function _selectQBSuggestion(btn) {
  const text =
    btn.dataset.text ||
    btn.querySelector(".fa-suggestion-chip-text")?.textContent.trim() ||
    btn.textContent.trim();
  const textarea = document.getElementById("qb-request");
  if (textarea) {
    textarea.value = text;
    textarea.dispatchEvent(new Event("input"));
  }
  btn.classList.add("qb-sugg-item--selected");
  btn.disabled = true;
  btn.setAttribute("aria-disabled", "true");
  if (typeof syncQBGenerateButtonState === "function")
    syncQBGenerateButtonState();
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

// ─────────────────────────────────────
// Admin — Users
// ─────────────────────────────────────
let _adminEditingUsername = null;

function _aclHasFullAccess(acl) {
  return !!(acl && Array.isArray(acl.allowed_datasets) && acl.allowed_datasets.includes("*"));
}

async function _fetchAclMap() {
  try {
    const res = await fetch("/admin/finance/acl", { headers: authHeaders() });
    if (!res.ok) return {};
    const data = await res.json();
    const map = {};
    for (const acl of data.acl || []) map[acl.user_id] = acl;
    return map;
  } catch (_) {
    return {};
  }
}

async function adminLoadUsers() {
  const tbody = document.getElementById("admin-users-tbody");
  if (!tbody) return;
  tbody.innerHTML = "<tr><td colspan='7' style='text-align:center;color:var(--ink3)'>Carregando...</td></tr>";

  try {
    const [res, aclMap] = await Promise.all([
      fetch("/admin/users", { headers: authHeaders() }),
      _fetchAclMap(),
    ]);
    if (!res.ok) throw new Error((await res.json()).detail || "Erro");
    const users = await res.json();

    tbody.innerHTML = users.map(u => `
      <tr>
        <td><code>${u.username}</code></td>
        <td>${u.name}</td>
        <td><span class="admin-badge ${u.is_admin ? 'badge-admin' : 'badge-user'}">${u.is_admin ? 'Admin' : 'Usuário'}</span></td>
        <td style="font-size:11px;color:var(--ink3)">${u.gerencia || '—'}</td>
        <td>${_aclHasFullAccess(aclMap[u.username]) ? '<span class="admin-badge badge-full-access">Total</span>' : '<span style="font-size:11px;color:var(--ink3)">—</span>'}</td>
        <td style="font-size:11px;color:var(--ink3)">${u.created_at ? u.created_at.slice(0, 10) : '—'}</td>
        <td class="admin-actions">
          <button class="btn-table-edit" onclick="adminOpenUserModal('${u.username}')">Editar</button>
          ${u.username !== currentUser?.username
            ? `<button class="btn-table-del" onclick="adminDeleteUser('${u.username}')">Excluir</button>`
            : ''}
        </td>
      </tr>
    `).join("") || "<tr><td colspan='7' style='text-align:center'>Nenhum usuário cadastrado.</td></tr>";
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan='7' style='color:#c0392b'>${e.message}</td></tr>`;
  }
}

// Opções conhecidas derivadas de _QB_GERENCIA_TOPICS. Backend pode adicionar mais via BigQuery labels.
function _buildGerenciaOptions(backendVals = []) {
  const options = [
    { value: "", label: "(nenhuma)", none: true },
    ..._QB_GERENCIA_TOPICS
      .filter((t) => t.gerencia)
      .map((t) => ({ value: t.gerencia, label: t.label })),
  ];
  for (const v of backendVals) {
    if (v && !options.some((o) => o.value === v)) {
      options.push({ value: v, label: _qbCapitalize(v.replace(/_/g, " ")) });
    }
  }
  return options;
}

function _renderGerenciaPicker(options) {
  const picker = document.getElementById("gerencia-picker");
  if (!picker) return;
  picker.innerHTML = options
    .map(
      (o) =>
        `<button type="button" class="gerencia-pill${o.none ? " gerencia-pill-none" : ""}" data-value="${_escapeHtml(o.value)}">${_escapeHtml(o.label)}</button>`,
    )
    .join("");
  picker.querySelectorAll(".gerencia-pill").forEach((pill) => {
    pill.addEventListener("click", () => {
      const val = pill.dataset.value;
      const hidden = document.getElementById("modal-gerencia");
      if (hidden) hidden.value = val;
      _syncGerenciaPickerUI(val);
    });
  });
}

function _syncGerenciaPickerUI(value) {
  const picker = document.getElementById("gerencia-picker");
  if (!picker) return;
  picker.querySelectorAll(".gerencia-pill").forEach((pill) => {
    pill.classList.toggle("selected", pill.dataset.value === value);
  });
}

async function _loadGerenciasIntoSelect() {
  let backendVals = [];
  try {
    const res = await fetch("/admin/gerencias", { headers: authHeaders() });
    if (res.ok) backendVals = await res.json();
  } catch (_) {}

  _renderGerenciaPicker(_buildGerenciaOptions(backendVals));
  const hidden = document.getElementById("modal-gerencia");
  if (hidden) hidden.value = "";
  _syncGerenciaPickerUI("");
}

async function adminOpenUserModal(username = null) {
  _adminEditingUsername = username;
  const title = document.getElementById("admin-modal-title");
  const passLabel = document.getElementById("modal-pass-label");
  const errEl = document.getElementById("admin-modal-error");

  if (errEl) errEl.style.display = "none";
  document.getElementById("modal-username").value = "";
  document.getElementById("modal-name").value = "";
  document.getElementById("modal-password").value = "";
  document.getElementById("modal-is-admin").checked = false;
  document.getElementById("modal-full-access").checked = false;
  document.getElementById("modal-username").disabled = false;

  await _loadGerenciasIntoSelect();

  if (username) {
    if (title) title.textContent = "Editar Usuário";
    if (passLabel) passLabel.textContent = "Nova senha (deixe em branco para não alterar)";
    try {
      const [res, aclRes] = await Promise.all([
        fetch("/admin/users", { headers: authHeaders() }),
        fetch(`/admin/finance/acl/${username}`, { headers: authHeaders() }),
      ]);
      const users = await res.json();
      const u = users.find(x => x.username === username);
      if (u) {
        document.getElementById("modal-username").value = u.username;
        document.getElementById("modal-username").disabled = true;
        document.getElementById("modal-name").value = u.name;
        document.getElementById("modal-is-admin").checked = !!u.is_admin;
        const gerVal = u.gerencia || "";
        document.getElementById("modal-gerencia").value = gerVal;
        _syncGerenciaPickerUI(gerVal);
      }
      if (aclRes.ok) {
        const acl = await aclRes.json();
        document.getElementById("modal-full-access").checked = _aclHasFullAccess(acl);
      }
    } catch (_) {}
  } else {
    if (title) title.textContent = "Novo Usuário";
    if (passLabel) passLabel.textContent = "Senha";
  }

  document.getElementById("admin-user-modal").style.display = "flex";
}

function adminCloseUserModal(event) {
  if (event && event.target !== document.getElementById("admin-user-modal")) return;
  document.getElementById("admin-user-modal").style.display = "none";
  _adminEditingUsername = null;
}

async function adminSaveUser() {
  const username = document.getElementById("modal-username").value.trim();
  const name = document.getElementById("modal-name").value.trim();
  const password = document.getElementById("modal-password").value;
  const is_admin = document.getElementById("modal-is-admin").checked;
  const full_access = document.getElementById("modal-full-access").checked;
  const gerencia = document.getElementById("modal-gerencia").value;
  const errEl = document.getElementById("admin-modal-error");
  const saveBtn = document.getElementById("admin-modal-save");

  if (errEl) errEl.style.display = "none";

  if (!username || !name) {
    if (errEl) { errEl.textContent = "Matrícula e nome são obrigatórios."; errEl.style.display = "block"; }
    return;
  }
  if (!_adminEditingUsername && !password) {
    if (errEl) { errEl.textContent = "Informe uma senha para o novo usuário."; errEl.style.display = "block"; }
    return;
  }

  if (saveBtn) saveBtn.disabled = true;

  try {
    let res;
    if (_adminEditingUsername) {
      const body = { name, is_admin, gerencia };
      if (password) body.password = password;
      res = await fetch(`/admin/users/${_adminEditingUsername}`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
    } else {
      res = await fetch("/admin/users", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ username, name, password, is_admin, gerencia }),
      });
    }

    if (!res.ok) throw new Error((await res.json()).detail || "Erro ao salvar");

    // Acesso total = ACL "*" (todas as gerências/datasets); desmarcado limpa
    // a ACL de volta ao padrão (permissivo ou bloqueado conforme RBAC strict).
    await fetch(`/admin/finance/acl/${username}`, {
      method: "PUT",
      headers: authHeaders(),
      body: JSON.stringify({
        allowed_datasets: full_access ? ["*"] : [],
        allowed_metrics: full_access ? ["*"] : [],
        denied_datasets: [],
      }),
    });

    document.getElementById("admin-user-modal").style.display = "none";
    _adminEditingUsername = null;
    adminLoadUsers();
  } catch (e) {
    if (errEl) { errEl.textContent = e.message; errEl.style.display = "block"; }
  } finally {
    if (saveBtn) saveBtn.disabled = false;
  }
}

async function adminDeleteUser(username) {
  if (!confirm(`Excluir o usuário "${username}"? Esta ação não pode ser desfeita.`)) return;
  try {
    const res = await fetch(`/admin/users/${username}`, {
      method: "DELETE",
      headers: authHeaders(),
    });
    if (!res.ok) throw new Error((await res.json()).detail || "Erro ao excluir");
    adminLoadUsers();
  } catch (e) {
    alert(e.message);
  }
}

// ─────────────────────────────────────
// Admin — Config
// ─────────────────────────────────────
async function adminLoadConfig() {
  const grid = document.getElementById("admin-config-grid");
  if (!grid) return;
  grid.innerHTML = '<div class="acp-loading">Carregando parâmetros...</div>';

  try {
    const res = await fetch("/admin/config", { headers: authHeaders() });
    if (!res.ok) throw new Error((await res.json()).detail || "Erro");
    const configs = await res.json();

    if (!configs.length) {
      grid.innerHTML = '<div class="acp-loading">Nenhum parâmetro encontrado.</div>';
      return;
    }

    grid.innerHTML = configs.map(c => `
      <div class="acp-card">
        <span class="acp-card-key">${escapeHtml(c.key)}</span>
        <div class="acp-card-desc">${escapeHtml(c.description || "—")}</div>
        <div class="acp-card-row">
          <input
            class="acp-card-input"
            id="cfg-${escapeHtml(c.key)}"
            type="text"
            value="${escapeHtml(c.value || "")}"
            onkeydown="if(event.key==='Enter') adminSaveConfig('${escapeHtml(c.key)}')"
          />
          <button class="acp-card-save" id="save-btn-${escapeHtml(c.key)}" onclick="adminSaveConfig('${escapeHtml(c.key)}')">Salvar</button>
        </div>
      </div>
    `).join("");
  } catch (e) {
    grid.innerHTML = `<div class="acp-loading" style="color:#c0392b">${e.message}</div>`;
  }
}

async function adminSaveConfig(key) {
  const input = document.getElementById(`cfg-${key}`);
  const btn = document.getElementById(`save-btn-${key}`);
  if (!input) return;
  const value = input.value.trim();

  try {
    const res = await fetch(`/admin/config/${key}`, {
      method: "PUT",
      headers: authHeaders(),
      body: JSON.stringify({ value }),
    });
    if (!res.ok) throw new Error((await res.json()).detail || "Erro ao salvar");
    if (btn) {
      btn.textContent = "Salvo ✓";
      btn.classList.add("saved");
      setTimeout(() => { btn.textContent = "Salvar"; btn.classList.remove("saved"); }, 1800);
    }
  } catch (e) {
    alert(e.message);
  }
}
