param(
  [Parameter(Mandatory = $true)]
  [string]$Message,

  [switch]$SkipTests
)

$ErrorActionPreference = "Stop"

function Step($text) {
  Write-Host "`n==> $text" -ForegroundColor Cyan
}

function Fail($text) {
  Write-Host "`nERRO: $text" -ForegroundColor Red
  exit 1
}

Step "Validando repositorio git"
if (-not (Test-Path ".git")) {
  Fail "Este comando deve ser executado na raiz de um repositorio git."
}

$branch = git rev-parse --abbrev-ref HEAD 2>$null
if (-not $branch) {
  Fail "Nao foi possivel identificar a branch atual."
}

$status = git status --porcelain
if (-not $status) {
  Write-Host "Nada para commitar. Repositorio sem alteracoes." -ForegroundColor Yellow
  exit 0
}

if (-not $SkipTests) {
  Step "Executando testes (pytest -q)"
  try {
    pytest -q
    if ($LASTEXITCODE -ne 0) {
      Fail "Testes falharam. Commit/push cancelados."
    }
  }
  catch {
    Fail "Nao foi possivel executar pytest. Ative o .venv e confirme as dependencias."
  }
}
else {
  Write-Host "Pulando testes por solicitacao do usuario." -ForegroundColor Yellow
}

Step "Adicionando alteracoes"
git add -A
if ($LASTEXITCODE -ne 0) {
  Fail "Falha ao executar git add -A."
}

Step "Criando commit"
git commit -m "$Message"
if ($LASTEXITCODE -ne 0) {
  Fail "Falha ao criar commit. Verifique hooks ou configuracao do git."
}

Step "Enviando para origin/$branch"
git push origin $branch
if ($LASTEXITCODE -ne 0) {
  Fail "Falha ao enviar para o GitHub."
}

Write-Host "`nPublicacao concluida com sucesso em origin/$branch." -ForegroundColor Green
