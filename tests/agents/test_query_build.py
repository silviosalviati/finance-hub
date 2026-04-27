from src.agents.query_build import QueryBuildAgent
from src.agents.query_build.nodes import (
    _find_invalid_column_references,
    _fix_invalid_string_aggregates,
    validate_generated_sql_consistency,
)
from src.agents.query_build.state import QueryBuildState


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


def test_find_invalid_column_references_detects_unknown_qualified_column():
    sql = """
    SELECT t1.valor_limite_liquido, t2.score_credito
    FROM `proj.ds.fatos_vendas` AS t1
    JOIN `proj.ds.perfil_clientes` AS t2
      ON t1.cliente_id = t2.cliente_id
    """
    table_columns = {
        "proj.ds.fatos_vendas": ["cliente_id", "valor_liquido"],
        "proj.ds.perfil_clientes": ["cliente_id", "score_credito"],
    }

    invalid = _find_invalid_column_references(sql, table_columns)

    assert "t1.valor_limite_liquido" in invalid
    assert "t2.score_credito" not in invalid


def test_validate_generated_sql_consistency_blocks_unknown_columns():
    state = QueryBuildState(
        request_text="teste",
        project_id="proj",
        dataset_hint="ds",
        generated_sql=(
            "SELECT t1.valor_limite_liquido FROM `proj.ds.fatos_vendas` AS t1"
        ),
        dataset_table_columns={
            "proj.ds.fatos_vendas": ["cliente_id", "valor_liquido"],
        },
    )

    result = validate_generated_sql_consistency(state)

    assert "error" in result
    assert "inexistente(s)" in result["error"]
    assert result.get("sample_error") == "Amostra bloqueada: SQL com coluna inexistente no schema."


def test_validate_generated_sql_consistency_blocks_named_params_before_schema_check():
    state = QueryBuildState(
        request_text="teste",
        project_id="proj",
        dataset_hint="ds",
        generated_sql=(
            "SELECT t1.valor_liquido FROM `proj.ds.fatos_vendas` AS t1 WHERE t1.valor_liquido > @limite"
        ),
        dataset_table_columns={
            "proj.ds.fatos_vendas": ["cliente_id", "valor_liquido"],
        },
    )

    result = validate_generated_sql_consistency(state)

    assert "error" in result
    assert "parametro(s) nomeado(s)" in result["error"]
    assert result.get("sample_error") == "Amostra bloqueada: SQL com parametro sem valor."


def test_validate_generated_sql_consistency_blocks_mustache_template_placeholders():
    state = QueryBuildState(
        request_text="teste",
        project_id="proj",
        dataset_hint="ds",
        generated_sql=(
            "SELECT * FROM `proj.ds.fatos_vendas` WHERE data_vigencia_inicio <= '{{DATA_FIM_PERIODO}}'"
        ),
        dataset_table_columns={
            "proj.ds.fatos_vendas": ["data_vigencia_inicio", "valor_liquido"],
        },
    )

    result = validate_generated_sql_consistency(state)

    assert "error" in result
    assert "template" in result["error"].lower()
    assert "DATA_FIM_PERIODO" in result["error"]
    assert result.get("sample_error") == "Amostra bloqueada: SQL com placeholder de template sem valor."
