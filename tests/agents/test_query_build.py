from src.agents.query_build import QueryBuildAgent
from src.agents.query_build.nodes import _fix_invalid_string_aggregates


def test_query_build_agent_metadata():
    agent = QueryBuildAgent()
    assert agent.agent_id == "query_build"
    assert agent.display_name == "Query Build"


def test_fix_invalid_string_aggregate_avg():
    sql = "SELECT AVG(CAST(p.score_credito AS STRING)) AS engajamento FROM t"

    fixed, changed = _fix_invalid_string_aggregates(sql)

    assert changed is True
    assert "AVG(SAFE_CAST(p.score_credito AS FLOAT64))" in fixed


def test_fix_invalid_string_aggregate_sum():
    sql = "SELECT SUM(CAST(f.valor_liquido AS STRING)) AS receita_total FROM t"

    fixed, changed = _fix_invalid_string_aggregates(sql)

    assert changed is True
    assert "SUM(SAFE_CAST(f.valor_liquido AS NUMERIC))" in fixed


def test_fix_invalid_string_aggregate_no_change_for_valid_avg():
    sql = "SELECT AVG(p.score_credito) AS engajamento FROM t"

    fixed, changed = _fix_invalid_string_aggregates(sql)

    assert changed is False
    assert fixed == sql
