"""Pytest harness — roda cada case do eval como um teste parametrizado.

Em CI rápida: ``pytest tests/evals``. Para debug individual com prompts
e detalhes, ``python -m tests.evals.runner --case 01``.
"""

from __future__ import annotations

import pytest

from tests.evals.runner import discover_cases, evaluate, run_case


def _ids(case):
    return case["id"]


_CASES = discover_cases()
if not _CASES:
    pytest.skip("Nenhum case em tests/evals/cases/", allow_module_level=True)


@pytest.mark.parametrize("case", _CASES, ids=_ids)
def test_eval(case):
    result = run_case(case)
    findings = evaluate(result, case.get("expect") or {})
    failed = [f for f in findings if not f["ok"]]
    if failed:
        lines = [f"Eval case '{case['id']}' falhou:"]
        for f in failed:
            actual = f["actual"]
            if isinstance(actual, str) and len(actual) > 160:
                actual = actual[:160] + "..."
            lines.append(
                f"  • {f['name']}: expected={f['expected']!r}  actual={actual!r}"
            )
        if result.error:
            lines.append(f"  • runner error: {result.error[:240]}")
        pytest.fail("\n".join(lines))
