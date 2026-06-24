"""Eval harness do Finance Voice IA.

Roda cada caso (definido em ``tests/evals/cases/*.py``) através do grafo
real do agente, com LLM e BigQuery completamente stubados, e avalia uma
lista declarativa de assertions.

Dois modos:
- ``pytest tests/evals/`` — usado pela CI para detectar regressão.
- ``python -m tests.evals.runner`` — CLI interativa para debug.

Cases são Python puro (sem yaml/json) com o dict ``CASE`` no topo:

    CASE = {
        "id": "...",
        "question": "...",
        "project_id": "...",
        "user": {"username": "...", "is_admin": False},
        "user_profile": {},
        "script": {        # respostas pré-gravadas do LLM por chamada
            "plan": {"rationale": "...", "steps": [...]},
            "picker_table_ids": [...],
            "sql": "SELECT ...",
            "reflect": {"is_valid": True, "suggested_steps": []},
            "composer": "Markdown da resposta final",
        },
        "bq": {            # dados pré-gravados das tools BigQuery
            "datasets": [...],
            "tables_by_dataset": {...},
            "schema_text": "...",
            "rows": [...],
            "bytes_processed": 512,
        },
        "expect": {
            "status": "ok",
            "plan": {"must_include": [...], "must_not_include": [...]},
            "steps": {"text_to_sql": {"ok": True}},
            "answer": {"must_mention_any": [...], "must_not_mention": [...]},
        },
    }
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

CASES_DIR = Path(__file__).parent / "cases"


# ---------------------------------------------------------------------------
# Scripted LLM — substitui completamente as chamadas reais ao Gemini
# ---------------------------------------------------------------------------


class _ScriptedStructured:
    """Mock de ``llm.with_structured_output(Schema)``.

    Dispatch é por campo do schema (robusto a classes privadas definidas
    dentro de funções, como ``_SqlOutput`` e ``_Picked``).
    """

    def __init__(self, parent: "ScriptedLLM", schema: Any) -> None:
        self.parent = parent
        self.schema = schema

    def invoke(self, messages: Any) -> Any:
        name = getattr(self.schema, "__name__", "")
        fields = list(getattr(self.schema, "model_fields", {}).keys())
        self.parent.calls.append(
            {"kind": "structured", "schema": name, "fields": fields,
             "system_head": _head(_first_system_text(messages), 200)}
        )

        if name == "PlanResponse":
            from src.agents.finance_auditor.supervisor_schemas import (
                PlanResponse,
                PlanStep,
            )
            plan = self.parent.script.get("plan") or {}
            steps = [PlanStep(**s) for s in plan.get("steps", [])]
            return PlanResponse(
                rationale=plan.get("rationale", ""),
                steps=steps,
            )

        if name == "ReflectVerdict":
            from src.agents.finance_auditor.supervisor_schemas import (
                PlanStep,
                ReflectVerdict,
            )
            r = self.parent.script.get("reflect") or {}
            steps = [PlanStep(**s) for s in r.get("suggested_steps", [])]
            return ReflectVerdict(
                is_valid=r.get("is_valid", True),
                confidence=r.get("confidence", 0.8),
                issues=r.get("issues", []),
                suggested_steps=steps,
            )

        # _SqlOutput (privado em cap_text_to_sql) — campo único `sql`.
        if fields == ["sql"]:
            return self.schema.model_validate(
                {"sql": self.parent.script.get("sql", "")}
            )

        # _Picked (privado em _pick_relevant_tables) — table_ids + rationale.
        if "table_ids" in fields:
            return self.schema.model_validate(
                {
                    "table_ids": self.parent.script.get("picker_table_ids", []),
                    "rationale": "scripted",
                }
            )

        raise RuntimeError(
            f"ScriptedLLM: sem script para schema={name} fields={fields}"
        )


class ScriptedLLM:
    """LLM determinístico orientado por script.

    Registra cada chamada em ``self.calls`` para que asserts possam
    inspecionar prompts/sequência.
    """

    def __init__(self, script: dict[str, Any]) -> None:
        self.script = script
        self.calls: list[dict[str, Any]] = []

    def with_structured_output(self, schema: Any) -> _ScriptedStructured:
        return _ScriptedStructured(self, schema)

    def invoke(self, messages: Any) -> Any:
        sys_text = _first_system_text(messages)
        self.calls.append(
            {"kind": "invoke_plain", "system_head": _head(sys_text, 200)}
        )
        sys_lower = sys_text.lower()
        if "compositor" in sys_lower or "composer" in sys_lower:
            return MagicMock(content=self.script.get("composer", ""))
        # Fallback usado em cap_text_to_sql quando structured_output falha.
        if "gerador de sql" in sys_lower:
            sql = self.script.get("sql", "")
            return MagicMock(content=json.dumps({"sql": sql}))
        # multimodal / catch-all
        return MagicMock(content=self.script.get("composer", ""))

    async def ainvoke(self, messages: Any) -> Any:
        return self.invoke(messages)


def _first_system_text(messages: Any) -> str:
    try:
        for m in messages or []:
            content = getattr(m, "content", "")
            # langchain_core.SystemMessage
            if "SystemMessage" in str(type(m)):
                return str(content)
        # fallback: primeira mensagem
        if messages:
            return str(getattr(messages[0], "content", ""))
    except Exception:  # noqa: BLE001
        pass
    return ""


def _head(text: str, n: int) -> str:
    text = (text or "").replace("\n", " ").strip()
    return text[:n]


# ---------------------------------------------------------------------------
# Stubs para BigQuery e SQLite
# ---------------------------------------------------------------------------


def _make_bq_stubs(bq_cfg: dict[str, Any]) -> dict[str, Any]:
    """Devolve dicionário de funções stub usadas em ``patch.multiple``."""
    datasets = list(bq_cfg.get("datasets") or [])
    tables_by_dataset = bq_cfg.get("tables_by_dataset") or {}
    schema_text = bq_cfg.get("schema_text") or "schema-stub"
    rows = list(bq_cfg.get("rows") or [])
    bytes_processed = int(bq_cfg.get("bytes_processed", 1024))
    estimated_cost = float(bq_cfg.get("estimated_cost_usd", 0.0001))

    def fake_list_project_datasets(project_id: str) -> list[str]:
        return datasets

    def fake_get_dataset_tables_metadata(
        project_id: str, dataset_hint: str, **kw: Any
    ) -> dict[str, Any]:
        if dataset_hint not in tables_by_dataset:
            raise RuntimeError(
                f"404 Not found: Dataset {project_id}:{dataset_hint}"
            )
        return {
            "project_id": project_id,
            "dataset_id": dataset_hint,
            "dataset_ref": f"{project_id}.{dataset_hint}",
            "tables": tables_by_dataset[dataset_hint],
        }

    def fake_get_table_schema(table_ref: str, project_id: str | None, **kw: Any) -> str:
        return schema_text

    fake_dry = MagicMock(error=None, bytes_processed=bytes_processed,
                         estimated_cost_usd=estimated_cost)

    def fake_dry_run_query(sql: str, project_id: str | None, **kw: Any):
        return fake_dry

    def fake_execute_query_rows(sql: str, project_id: str | None, **kw: Any):
        return rows

    return {
        "_list_project_datasets": fake_list_project_datasets,
        "get_dataset_tables_metadata": fake_get_dataset_tables_metadata,
        "get_table_schema": fake_get_table_schema,
        "dry_run_query": fake_dry_run_query,
        "execute_query_rows": fake_execute_query_rows,
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


@dataclass
class RunResult:
    case_id: str
    response: dict[str, Any]
    llm_calls: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


def run_case(case: dict[str, Any]) -> RunResult:
    """Executa um único case e devolve a resposta do agente.

    Tudo é stubado: LLM, BigQuery, RBAC, audit log, semantic layer.
    """
    from src.agents.finance_auditor import (
        FinanceAuditorAgent,
        audit as audit_mod,
        capabilities as cap_mod,
    )

    script = case.get("script") or {}
    bq_cfg = case.get("bq") or {}

    scripted_llm = ScriptedLLM(script)
    bq_stubs = _make_bq_stubs(bq_cfg)

    # Stubs:
    # - _create_llm devolve o ScriptedLLM tanto no LLM analítico quanto no criativo.
    # - capabilities.get_runtime_config sempre devolve "5 GiB" para o budget.
    # - RBAC libera tudo (o eval não testa governança aqui).
    # - audit_log e org_memory: write-only via mock para não tocar DB.
    # - PII mode "off" para resposta não ser mutada.
    runtime_overrides = {
        "VERTEXAI_TEMPERATURE_CREATIVE": "0.3",
        "FINANCE_AUDITOR_QUERY_BUDGET_BYTES": str(5 * 1024 ** 3),
        "FINANCE_AUDITOR_PII_MODE": "off",
        "FINANCE_AUDITOR_RBAC_STRICT": "0",
        "FINANCE_AUDITOR_DEFAULT_DATASET": "",
    }

    def fake_runtime_cfg(key: str, default: str = "") -> str:
        return runtime_overrides.get(key, default)

    response: dict[str, Any] = {}
    error: str | None = None

    try:
        with patch("src.agents.finance_auditor._create_llm",
                   side_effect=lambda *a, **kw: scripted_llm), \
             patch("src.shared.tools.llm.create_llm",
                   side_effect=lambda *a, **kw: scripted_llm), \
             patch.object(cap_mod, "get_runtime_config", side_effect=fake_runtime_cfg), \
             patch("src.shared.guardrails.pii_guard.get_runtime_config",
                   side_effect=fake_runtime_cfg), \
             patch("src.shared.config.get_runtime_config",
                   side_effect=fake_runtime_cfg), \
             patch.object(cap_mod.rbac, "check_dataset", return_value=(True, "")), \
             patch.object(cap_mod.rbac, "check_metric", return_value=(True, "")), \
             patch.object(audit_mod, "append_finance_audit", return_value=1), \
             patch.object(cap_mod, "_list_project_datasets",
                          side_effect=bq_stubs["_list_project_datasets"]), \
             patch.object(cap_mod, "get_dataset_tables_metadata",
                          side_effect=bq_stubs["get_dataset_tables_metadata"]), \
             patch.object(cap_mod, "get_table_schema",
                          side_effect=bq_stubs["get_table_schema"]), \
             patch.object(cap_mod, "dry_run_query",
                          side_effect=bq_stubs["dry_run_query"]), \
             patch.object(cap_mod, "execute_query_rows",
                          side_effect=bq_stubs["execute_query_rows"]):
            agent = FinanceAuditorAgent()
            response = agent.analyze(
                query=case["question"],
                project_id=case.get("project_id") or "test_project",
                dataset_hint=case.get("dataset_hint"),
                user_profile=case.get("user_profile") or {},
                user=case.get("user") or {"username": "tester"},
                attachments=case.get("attachments") or [],
            )
    except Exception as exc:  # noqa: BLE001
        import traceback
        error = f"{exc}\n{traceback.format_exc()}"

    return RunResult(
        case_id=case["id"],
        response=response,
        llm_calls=scripted_llm.calls,
        error=error,
    )


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------


def _captured_text(response: dict[str, Any]) -> str:
    return str(
        response.get("chat_answer")
        or response.get("markdown_report")
        or response.get("error")
        or ""
    )


def _plan_capabilities(response: dict[str, Any]) -> list[str]:
    return [
        str((s or {}).get("capability") or "").lower()
        for s in (response.get("plan") or [])
    ]


def _tool_results_by_cap(response: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for r in response.get("tool_results") or []:
        cap = str((r or {}).get("capability") or "").lower()
        out.setdefault(cap, r)
    return out


def _artifact_types(response: dict[str, Any]) -> set[str]:
    return {str((a or {}).get("type") or "") for a in response.get("artifacts") or []}


def evaluate(result: RunResult, expect: dict[str, Any]) -> list[dict[str, Any]]:
    """Avalia cada bloco de ``expect`` contra a resposta. Devolve findings."""
    findings: list[dict[str, Any]] = []

    def add(name: str, ok: bool, expected: Any = None, actual: Any = None) -> None:
        findings.append({"name": name, "ok": ok, "expected": expected, "actual": actual})

    if result.error:
        add("runner.no_error", False, "no exception", result.error[:300])
        return findings

    resp = result.response

    # status
    if "status" in expect:
        add(
            "status",
            resp.get("status") == expect["status"],
            expect["status"],
            resp.get("status"),
        )

    # persona
    if "persona" in expect:
        add("persona", resp.get("persona") == expect["persona"], expect["persona"], resp.get("persona"))

    # plan
    plan_expect = expect.get("plan") or {}
    caps = _plan_capabilities(resp)
    if "must_include" in plan_expect:
        missing = [c for c in plan_expect["must_include"] if c not in caps]
        add("plan.must_include", not missing, plan_expect["must_include"], caps)
    if "must_not_include" in plan_expect:
        bad = [c for c in plan_expect["must_not_include"] if c in caps]
        add("plan.must_not_include", not bad,
            f"none of {plan_expect['must_not_include']}", caps)
    if "max_steps" in plan_expect:
        add("plan.max_steps", len(caps) <= plan_expect["max_steps"],
            f"<= {plan_expect['max_steps']}", len(caps))
    if "min_steps" in plan_expect:
        add("plan.min_steps", len(caps) >= plan_expect["min_steps"],
            f">= {plan_expect['min_steps']}", len(caps))

    # steps
    steps_expect = expect.get("steps") or {}
    by_cap = _tool_results_by_cap(resp)
    for cap, cond in steps_expect.items():
        cap_l = cap.lower()
        if cap_l not in by_cap:
            add(f"steps.{cap}.executed", False, "executed", "missing")
            continue
        if "ok" in cond:
            add(f"steps.{cap}.ok", bool(by_cap[cap_l].get("ok")) == bool(cond["ok"]),
                cond["ok"], by_cap[cap_l].get("ok"))

    # answer
    text = _captured_text(resp)
    text_lower = text.lower()
    answer_expect = expect.get("answer") or {}
    if "must_mention_any" in answer_expect:
        terms = [t.lower() for t in answer_expect["must_mention_any"]]
        hit = any(t in text_lower for t in terms)
        add("answer.must_mention_any", hit, answer_expect["must_mention_any"], text[:240])
    if "must_mention_all" in answer_expect:
        missing = [t for t in answer_expect["must_mention_all"] if t.lower() not in text_lower]
        add("answer.must_mention_all", not missing, answer_expect["must_mention_all"], missing or "ok")
    if "must_not_mention" in answer_expect:
        bad = [t for t in answer_expect["must_not_mention"] if t.lower() in text_lower]
        add("answer.must_not_mention", not bad,
            f"none of {answer_expect['must_not_mention']}", bad or "ok")
    if "min_length" in answer_expect:
        add("answer.min_length", len(text) >= answer_expect["min_length"],
            f">= {answer_expect['min_length']}", len(text))

    # artifacts
    art_expect = expect.get("artifacts") or {}
    types = _artifact_types(resp)
    if "must_include_types" in art_expect:
        missing = [t for t in art_expect["must_include_types"] if t not in types]
        add("artifacts.must_include_types", not missing,
            art_expect["must_include_types"], sorted(types))
    if "must_not_include_types" in art_expect:
        bad = [t for t in art_expect["must_not_include_types"] if t in types]
        add("artifacts.must_not_include_types", not bad,
            f"none of {art_expect['must_not_include_types']}", sorted(types))

    return findings


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_cases(cases_dir: Path | None = None) -> list[dict[str, Any]]:
    """Importa cada ``cases/case_*.py`` e coleta o dict ``CASE`` exportado."""
    cases_dir = cases_dir or CASES_DIR
    out: list[dict[str, Any]] = []
    if not cases_dir.exists():
        return out
    for path in sorted(cases_dir.glob("case_*.py")):
        mod_name = f"tests.evals.cases.{path.stem}"
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        try:
            mod = importlib.import_module(mod_name)
        except Exception as exc:  # noqa: BLE001
            print(f"[skip] {path.name}: import error: {exc}", file=sys.stderr)
            continue
        case = getattr(mod, "CASE", None)
        if isinstance(case, dict) and "id" in case:
            out.append(case)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_finding(f: dict[str, Any]) -> None:
    mark = "✓" if f["ok"] else "✗"
    color_start = "\033[32m" if f["ok"] else "\033[31m"
    color_end = "\033[0m"
    print(f"   {color_start}{mark}{color_end} {f['name']}")
    if not f["ok"]:
        print(f"      expected: {f['expected']!r}")
        actual = f["actual"]
        if isinstance(actual, str) and len(actual) > 160:
            actual = actual[:160] + "..."
        print(f"      actual:   {actual!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tests.evals.runner")
    parser.add_argument("--case", help="ID do case específico (substring match).")
    parser.add_argument("--json", dest="json_out", help="Caminho para salvar relatório JSON.")
    parser.add_argument("--quiet", action="store_true", help="Suprime detalhes; só sumário.")
    args = parser.parse_args(argv)

    cases = discover_cases()
    if args.case:
        cases = [c for c in cases if args.case in c["id"]]
    if not cases:
        print("Nenhum case encontrado.")
        return 1

    report: list[dict[str, Any]] = []
    total = 0
    passed = 0
    for case in cases:
        total += 1
        if not args.quiet:
            print(f"\n■ {case['id']}: {case.get('question', '')[:80]}")
        result = run_case(case)
        findings = evaluate(result, case.get("expect") or {})
        case_pass = all(f["ok"] for f in findings)
        passed += int(case_pass)
        if not args.quiet:
            for f in findings:
                _print_finding(f)
        else:
            print(f"  {'✓' if case_pass else '✗'} {case['id']}")
        report.append({
            "id": case["id"],
            "passed": case_pass,
            "findings": findings,
            "response_summary": {
                "status": result.response.get("status"),
                "persona": result.response.get("persona"),
                "plan_caps": _plan_capabilities(result.response),
                "answer_head": _captured_text(result.response)[:160],
            },
            "llm_calls": [c.get("schema") or c.get("kind") for c in result.llm_calls],
            "error": result.error,
        })

    print(f"\n{'='*60}")
    print(f"{passed}/{total} cases passaram")
    print("=" * 60)

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Relatório JSON: {args.json_out}")

    return 0 if passed == total else 2


if __name__ == "__main__":
    sys.exit(main())
