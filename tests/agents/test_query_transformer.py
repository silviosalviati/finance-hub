from unittest.mock import patch

from src.agents.query_transformer import QueryTransformerAgent
from src.agents.query_transformer.nodes import (
    _deterministic_quality_checks,
    _resolve_refs_to_literal_sql,
    await_quality_approval,
    check_access,
    node_guardrails_in,
    validate_equivalence,
)
from src.agents.query_transformer.sql_analysis import analyze_sql, architecture_questions
from src.agents.query_transformer.state import QueryTransformerState
from src.shared.tools.schemas import DryRunResult


def test_query_transformer_agent_metadata():
    agent = QueryTransformerAgent()
    assert agent.agent_id == "query_transformer"
    assert agent.display_name == "Query Transformer"


def test_format_result_preserves_dry_run_error_detail():
    agent = QueryTransformerAgent()
    result = agent._format_result(
        {
            "dry_run_generated": DryRunResult(
                error="Unrecognized name: missing_column at [1:8]",
                bytes_processed=0,
                bytes_billed=0,
                estimated_cost_usd=0.0,
            ),
            "error": None,
            "error_category": "bigquery_syntax",
        }
    )

    assert result["status"] == "error"
    assert "Unrecognized name: missing_column" in result["error"]


def test_sql_analysis_extracts_bigquery_dependencies():
    analysis = analyze_sql("SELECT id FROM `proj.ds.clientes` WHERE updated_at >= '2026-01-01'")
    assert analysis.sql_kind == "single_select"
    assert analysis.tables[0]["full_name"] == "proj.ds.clientes"
    assert "updated_at" in analysis.partition_candidates


def test_sql_analysis_blocks_dynamic_sql_and_procedure_scope():
    analysis = analyze_sql('EXECUTE IMMEDIATE "SELECT 1"')
    assert analysis.sql_kind == "dynamic_sql"
    assert any(finding.code == "DYNAMIC_SQL" for finding in analysis.blocking_findings)

    procedure = analyze_sql("CALL `proj.ds.rebuild_daily`()")
    assert procedure.sql_kind == "procedure_call"
    assert not procedure.blocking_findings
    assert any(finding.code == "PROCEDURE" for finding in procedure.security_findings)


def test_sql_analysis_requires_architecture_answers_for_aggregations():
    analysis = analyze_sql("SELECT account_id, SUM(amount) FROM `proj.ds.events` GROUP BY account_id")
    questions = architecture_questions(analysis)
    assert any(question["id"] == "refresh_strategy" for question in questions)
    assert any(question["id"] == "unique_key" for question in questions)


class TestGuardrailsIn:
    def test_blocks_non_select(self):
        state = QueryTransformerState(
            request_sql="DELETE FROM proj.ds.tabela WHERE 1=1", project_id="proj"
        )
        result = node_guardrails_in(state)
        assert result.get("error_category") == "sql_not_select"

    def test_blocks_missing_from(self):
        state = QueryTransformerState(request_sql="SELECT 1", project_id="proj")
        result = node_guardrails_in(state)
        assert result.get("error_category") == "sql_not_select"

    def test_allows_valid_select(self):
        state = QueryTransformerState(
            request_sql="SELECT id, nome FROM `proj.ds.clientes`", project_id="proj"
        )
        assert node_guardrails_in(state) == {}


class TestCheckAccess:
    def test_blocks_when_rbac_denies(self):
        state = QueryTransformerState(
            request_sql="SELECT id FROM `proj.ds_restrito.clientes`", project_id="proj"
        )
        with patch(
            "src.agents.query_transformer.nodes.rbac.check_dataset",
            return_value=(False, "sem acesso"),
        ):
            result = check_access(state)
        assert result.get("error_category") == "rbac"

    def test_allows_when_rbac_permits(self):
        state = QueryTransformerState(
            request_sql="SELECT id FROM `proj.ds.clientes`", project_id="proj"
        )
        with patch(
            "src.agents.query_transformer.nodes.rbac.check_dataset",
            return_value=(True, ""),
        ):
            result = check_access(state)
        assert result == {}


class TestResolveRefsToLiteralSql:
    def test_resolves_ref(self):
        sql = 'SELECT * FROM ${ref("clientes")}'
        resolved = _resolve_refs_to_literal_sql(sql, "proj")
        assert resolved == "SELECT * FROM `proj.clientes`"

    def test_resolves_source(self):
        sql = 'SELECT * FROM ${source("raw", "clientes")}'
        resolved = _resolve_refs_to_literal_sql(sql, "proj")
        assert resolved == "SELECT * FROM `proj.raw.clientes`"

    def test_no_placeholder_unchanged(self):
        sql = "SELECT * FROM `proj.ds.clientes`"
        assert _resolve_refs_to_literal_sql(sql, "proj") == sql

    def test_ref_preserves_original_dataset(self):
        sql = 'SELECT id FROM ${ref("clientes")}'
        original = "SELECT id FROM `proj.ds.clientes`"
        assert _resolve_refs_to_literal_sql(sql, "proj", original) == "SELECT id FROM `proj.ds.clientes`"


class TestValidateEquivalence:
    def test_matching_schema_is_equivalent(self):
        schema = [{"name": "id", "type": "INTEGER"}, {"name": "nome", "type": "STRING"}]
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            dry_run_original=DryRunResult(
                bytes_processed=100, bytes_billed=100, estimated_cost_usd=0.0, result_schema=schema
            ),
            dry_run_generated=DryRunResult(
                bytes_processed=100, bytes_billed=100, estimated_cost_usd=0.0, result_schema=schema
            ),
        )
        result = validate_equivalence(state)
        assert result["equivalence_ok"] is True
        assert result["equivalence_diff"] == ""

    def test_mismatched_schema_is_not_equivalent(self):
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            dry_run_original=DryRunResult(
                bytes_processed=100,
                bytes_billed=100,
                estimated_cost_usd=0.0,
                result_schema=[{"name": "id", "type": "INTEGER"}],
            ),
            dry_run_generated=DryRunResult(
                bytes_processed=100,
                bytes_billed=100,
                estimated_cost_usd=0.0,
                result_schema=[{"name": "id", "type": "STRING"}],
            ),
        )
        result = validate_equivalence(state)
        assert result["equivalence_ok"] is False
        assert "id" in result["equivalence_diff"]

    def test_missing_dry_runs_is_not_equivalent(self):
        state = QueryTransformerState(request_sql="x", project_id="proj")
        result = validate_equivalence(state)
        assert result["equivalence_ok"] is False


class TestDeterministicQualityChecks:
    def test_missing_config_block_penalized(self):
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            config_block="",
            query_body='SELECT id FROM ${ref("clientes")}',
        )
        score, issues = _deterministic_quality_checks(state)
        assert score < 100
        assert any("config()" in i for i in issues)

    def test_select_star_penalized(self):
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            config_block='config { type: "table" }',
            query_body='SELECT * FROM ${ref("clientes")}',
        )
        score, issues = _deterministic_quality_checks(state)
        assert any("SELECT *" in i for i in issues)
        assert score < 100

    def test_raw_table_reference_without_ref_penalized(self):
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            config_block='config { type: "table" }',
            query_body="SELECT id FROM `proj.ds.clientes`",
        )
        score, issues = _deterministic_quality_checks(state)
        assert any("ref()/source()" in i for i in issues)

    def test_incremental_without_unique_key_penalized(self):
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            config_block='config { type: "incremental" }',
            query_body='SELECT id FROM ${ref("clientes")}',
            materialization_type="incremental",
        )
        score, issues = _deterministic_quality_checks(state)
        assert any("uniqueKey" in i for i in issues)

    def test_clean_sqlx_scores_100(self):
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            config_block='config { type: "table", uniqueKey: ["id"] }',
            query_body='SELECT id, nome FROM ${ref("clientes")}',
            materialization_type="table",
        )
        score, issues = _deterministic_quality_checks(state)
        assert score == 100
        assert issues == []


class TestAwaitQualityApproval:
    def test_skips_when_score_high_and_equivalence_ok(self):
        state = QueryTransformerState(
            request_sql="x", project_id="proj", quality_score=90, equivalence_ok=True
        )
        result = await_quality_approval(state)
        assert result == {"human_decision": "skip"}

    def test_skips_after_two_retry_cycles_even_with_low_score(self):
        state = QueryTransformerState(
            request_sql="x",
            project_id="proj",
            quality_score=50,
            equivalence_ok=True,
            quality_retry_count=2,
        )
        result = await_quality_approval(state)
        assert result["human_decision"] == "skip"
        assert any("2 tentativas" in w for w in result["warnings"])
