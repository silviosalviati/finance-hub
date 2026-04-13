from src.agents.query_analyzer import QueryAnalyzerAgent
from src.agents.query_analyzer.nodes import _calculate_score, _inspect_query_structure, _score_to_grade
from src.agents.query_analyzer.state import AgentState
from src.shared.tools.schemas import DryRunResult, QueryAntiPattern


def test_query_analyzer_agent_metadata():
    agent = QueryAnalyzerAgent()
    assert agent.agent_id == "query_analyzer"
    assert agent.display_name == "Query Analyzer"


def test_query_analyzer_score_rewards_real_optimization():
    state = AgentState(
        original_query="SELECT * FROM `p.d.t` ORDER BY 1",
        project_id="p",
        query_structure={
            "has_star": True,
            "has_order_without_limit": True,
            "has_cross_join": False,
            "has_distinct": False,
            "has_union_without_all": False,
            "join_count": 0,
            "subquery_count": 0,
            "cte_count": 0,
            "tables": ["p.d.t"],
        },
        antipatterns=[
            QueryAntiPattern(
                pattern="SELECT *",
                description="uso de select star",
                severity="HIGH",
                suggestion="projetar colunas",
            ),
            QueryAntiPattern(
                pattern="ORDER BY sem LIMIT",
                description="ordenacao global",
                severity="HIGH",
                suggestion="adicionar limit",
            ),
        ],
        optimized_query="SELECT col1, col2 FROM `p.d.t`",
        dry_run_original=DryRunResult(
            bytes_processed=100 * 1024**3,
            bytes_billed=100 * 1024**3,
            estimated_cost_usd=0.5,
            referenced_tables=["p.d.t"],
            error=None,
        ),
        dry_run_optimized=DryRunResult(
            bytes_processed=40 * 1024**3,
            bytes_billed=40 * 1024**3,
            estimated_cost_usd=0.2,
            referenced_tables=["p.d.t"],
            error=None,
        ),
    )

    score = _calculate_score(state)
    grade = _score_to_grade(score)

    assert score >= 75
    assert grade in {"A", "B"}


def test_query_analyzer_score_penalizes_poor_final_query():
    state = AgentState(
        original_query="SELECT * FROM `p.d.t`",
        project_id="p",
        query_structure={
            "has_star": True,
            "has_order_without_limit": False,
            "has_cross_join": True,
            "has_distinct": True,
            "has_union_without_all": True,
            "join_count": 1,
            "subquery_count": 0,
            "cte_count": 0,
            "tables": ["p.d.t"],
        },
        antipatterns=[
            QueryAntiPattern(
                pattern="SELECT *",
                description="uso de select star",
                severity="HIGH",
                suggestion="projetar colunas",
            ),
            QueryAntiPattern(
                pattern="CROSS JOIN sem filtro",
                description="cross join",
                severity="HIGH",
                suggestion="evitar cross join",
            ),
        ],
        optimized_query="SELECT DISTINCT * FROM `p.d.t` CROSS JOIN `p.d.t2` UNION SELECT * FROM `p.d.t3`",
        dry_run_original=DryRunResult(
            bytes_processed=10 * 1024**3,
            bytes_billed=10 * 1024**3,
            estimated_cost_usd=0.05,
            referenced_tables=["p.d.t"],
            error=None,
        ),
        dry_run_optimized=DryRunResult(
            bytes_processed=12 * 1024**3,
            bytes_billed=12 * 1024**3,
            estimated_cost_usd=0.06,
            referenced_tables=["p.d.t", "p.d.t2", "p.d.t3"],
            error=None,
        ),
    )

    score = _calculate_score(state)
    grade = _score_to_grade(score)

    assert score < 75
    assert grade in {"C", "D", "F"}


def test_inspect_query_structure_detects_order_by_rand():
    structure = _inspect_query_structure("SELECT col FROM `p.d.t` ORDER BY RAND() LIMIT 100")

    assert structure["has_order_by_rand"] is True
