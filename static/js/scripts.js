// ─────────────────────────────────────
// App state
// ─────────────────────────────────────
let token = null;
let currentUser = null;
let session = { queries: 0 };
let qbDatasetValidationTimer = null;
let qbIsLoading = false;
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

async function setRuntimeModelInfo() {
  const modelNameEl = document.getElementById("qa-model-name");
  const modelSubEl = document.getElementById("qa-model-sub");
  const qbModelNameEl = document.getElementById("qb-model-name");
  const qbModelSubEl = document.getElementById("qb-model-sub");

  if (modelNameEl) modelNameEl.textContent = "Carregando modelo...";
  if (modelSubEl) modelSubEl.textContent = "LLM em uso no backend";
  if (qbModelNameEl) qbModelNameEl.textContent = "Carregando modelo...";
  if (qbModelSubEl) qbModelSubEl.textContent = "LLM em uso no backend";

  if (!modelNameEl && !qbModelNameEl) return;

  try {
    const res = await fetch("/api/runtime-llm");
    if (!res.ok) throw new Error("Falha ao buscar LLM ativa");

    const data = await res.json();
    if (modelNameEl) modelNameEl.textContent = data?.model || "não definido";
    if (modelSubEl)
      modelSubEl.textContent = data?.provider_label || "Provider desconhecido";
    if (qbModelNameEl)
      qbModelNameEl.textContent = data?.model || "não definido";
    if (qbModelSubEl)
      qbModelSubEl.textContent =
        data?.provider_label || "Provider desconhecido";
  } catch (err) {
    console.warn("Não foi possível carregar a LLM ativa:", err);
    if (modelNameEl) modelNameEl.textContent = "não definido";
    if (modelSubEl) modelSubEl.textContent = "Provider indisponível";
    if (qbModelNameEl) qbModelNameEl.textContent = "não definido";
    if (qbModelSubEl) qbModelSubEl.textContent = "Provider indisponível";
  }
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
    if (!Number.isNaN(tableCount)) {
      chips.push(
        `<span class="qb-dataset-chip">📊 ${tableCount} tabelas</span>`,
      );
    }
    if (datasetHint) {
      chips.push(`<span class="qb-dataset-chip">🗂️ ${datasetHint}</span>`);
    }
    chips.push('<span class="qb-dataset-chip">✅ Metadados OK</span>');
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
          payload.message || "Dataset nao validado para uso no Query Build.",
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

function loadExampleQuery() {
  const project = document.getElementById("qa-project");
  const query = document.getElementById("qa-query");

  if (project && !project.value.trim()) {
    project.value = "seu-projeto-gcp";
  }

  if (query) {
    query.value = `SELECT
  *
FROM \`projeto.dataset.tabela\`
WHERE data >= '2024-01-01'
ORDER BY data DESC`;
    query.focus();
  }
}

function resetQATabsDataState() {
  ["tab-antipatterns", "tab-optimized", "tab-recs"].forEach((id) => {
    document.getElementById(id)?.classList.remove("has-data");
  });

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

  if (qTiles) qTiles.style.display = "none";
  if (qSavSec) qSavSec.style.display = "none";
  if (qRecSec) qRecSec.style.display = "none";
  if (qTipsSec) qTipsSec.style.display = "none";
  if (qOptSec) qOptSec.style.display = "none";
  if (qOptEmpty) qOptEmpty.style.display = "flex";

  const qApList = document.getElementById("q-ap-list");
  const qRecList = document.getElementById("q-rec-list");
  const qTipsList = document.getElementById("q-tips-list");
  const qOptQuery = document.getElementById("q-opt-query");
  const qSummary = document.getElementById("q-summary");
  const qApCount = document.getElementById("q-ap-count");

  if (qApList) qApList.innerHTML = "";
  if (qRecList) qRecList.innerHTML = "";
  if (qTipsList) qTipsList.innerHTML = "";
  if (qOptQuery) qOptQuery.textContent = "";
  if (qSummary) qSummary.textContent = "";
  if (qApCount) qApCount.textContent = "";

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
  } else if (view === "analytics") {
    document.getElementById("nav-analytics")?.classList.add("active");
    document.querySelectorAll(".snav")[2]?.classList.add("active");
  } else if (view === "qa") {
    document.getElementById("nav-qa")?.classList.add("active");
  } else if (view === "qb") {
    document.getElementById("nav-qb")?.classList.add("active");
  }

  if (view === "qa" || view === "qb") {
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
    "Document Build": devColors.teal,
    "Query Build": devColors.violet,
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
// Query Analyzer
// ─────────────────────────────────────
async function runAnalyze() {
  const query = document.getElementById("qa-query")?.value.trim() || "";
  const project_id = document.getElementById("qa-project")?.value.trim() || "";
  const errEl = document.getElementById("qa-error");
  const qaEmpty = document.getElementById("qa-empty");
  const qaTabsArea = document.getElementById("qa-tabs-area");

  if (errEl) errEl.style.display = "none";

  if (!query) {
    showQAError("Cole uma query SQL antes de analisar.");
    return;
  }

  if (!project_id) {
    showQAError("Preencha o Project ID do GCP.");
    return;
  }

  setQALoading(true);
  setQAProgress("Validando entrada...", 12);
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
      body: JSON.stringify({ query, project_id }),
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
    text.textContent = on ? "Gerando SQL..." : "Gerar SQL com Query Build";
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

function loadExampleQBRequest() {
  const project = document.getElementById("qb-project");
  const dataset = document.getElementById("qb-dataset");
  const request = document.getElementById("qb-request");

  if (project && !project.value.trim()) project.value = "seu-projeto-gcp";
  if (dataset && !dataset.value.trim()) dataset.value = "inteligencia_negocios";
  if (request) {
    request.value =
      "Quero as 20 maiores vendas dos ultimos 30 dias por regiao, com ticket medio e variacao percentual versus mes anterior.";
    request.focus();
  }

  scheduleQBDatasetValidation();
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
    bot: "Query Analyzer",
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

  if (btn) btn.disabled = on;
  if (spinner) spinner.style.display = on ? "block" : "none";
  if (text) {
    text.textContent = on ? "Analisando..." : "Analisar com Query Analyzer";
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

document.addEventListener("click", (e) => {
  const target = e.target.closest(".btn-example-query");
  if (target) {
    loadExampleQuery();
  }
});

document.getElementById("qa-query")?.addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    runAnalyze();
  }
});

document.getElementById("qb-request")?.addEventListener("keydown", (e) => {
  if (e.ctrlKey && e.key === "Enter") {
    runQueryBuild();
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
    setRuntimeModelInfo();

    // Remover event listeners dos botões que foram removidos
    renderShowcase();
    startShowcaseAutoplay();
    console.log("✅ Inicialização concluída!");
  } catch (error) {
    console.error("❌ Erro na inicialização:", error);
  }
});

const showcaseBots = [
  {
    name: "Query Analyzer",
    description:
      "Analisa queries BigQuery, identifica anti-padrões e sugere otimizações que reduzem custo e aceleram performance.",
    tags: ["BigQuery", "Power BI", "SQL"],
    status: "Disponivel",
    action: () => navTo("qa"),
  },
  {
    name: "Document Build",
    description:
      "Gera documentação técnica automaticamente a partir de queries SQL, pipelines e modelos de dados financeiros.",
    tags: ["Docs", "Pipeline", "DataOps"],
    status: "Em Andamento",
    action: () =>
      openDev(
        "Document Build",
        "Gera documentação técnica a partir de queries SQL e pipelines.",
        [
          "Análise de queries",
          "Geração de markdown",
          "Exportar para Confluence",
        ],
        "Q2 2025",
      ),
  },
  {
    name: "Query Build",
    description:
      "Constrói queries BigQuery a partir de linguagem natural para acelerar análises e exploração de dados.",
    tags: ["NL2SQL", "BigQuery", "IA"],
    status: "Disponivel",
    action: () => navTo("qb"),
  },
  {
    name: "Finance AuditorIA",
    description:
      "Audita pipelines e queries financeiras verificando conformidade, rastreabilidade e consistência de KPIs.",
    tags: ["Auditoria", "KPIs", "Compliance"],
    status: "Disponivel",
    action: () =>
      openDev(
        "Finance AuditorIA",
        "Audita pipelines verificando conformidade e consistência de KPIs.",
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
