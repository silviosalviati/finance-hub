"""Testes do CRUD de métricas do Semantic Layer (Gold Metric Catalog).

Cobre o round-trip dos campos `domain`/`is_official` adicionados em
`finance_semantic_metrics` — usados pelo Planner (REGRA #11) para eleger a
métrica oficial de um domínio quando o usuário pede gráfico/dashboard sem
citar uma métrica específica.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def db(tmp_path, monkeypatch):
    from src.core import database

    monkeypatch.setattr(database, "_DB_PATH", tmp_path / "test_app.db")
    database.init_db()
    return database


class TestFinanceMetricsGoldCatalog:
    def test_upsert_persiste_domain_e_is_official(self, db):
        db.upsert_finance_metric(
            "cobranca_oficial",
            name="Inadimplência oficial",
            description="Métrica governada de cobrança",
            source_table="p.d.t",
            sql_template="SELECT 1",
            domain="cobranca",
            is_official=True,
        )

        metric = db.get_finance_metric("cobranca_oficial")
        assert metric is not None
        assert metric["domain"] == "cobranca"
        assert metric["is_official"] is True

    def test_default_is_official_false_e_domain_vazio(self, db):
        db.upsert_finance_metric(
            "ticket_medio",
            name="Ticket médio",
            description="",
            source_table="p.d.t",
            sql_template="SELECT 1",
        )

        metric = db.get_finance_metric("ticket_medio")
        assert metric["is_official"] is False
        assert metric["domain"] == ""

    def test_list_finance_metrics_inclui_os_novos_campos(self, db):
        db.upsert_finance_metric(
            "vendas_oficial",
            name="Receita oficial",
            description="",
            source_table="p.d.t",
            sql_template="SELECT 1",
            domain="vendas",
            is_official=True,
        )

        metrics = db.list_finance_metrics()
        assert len(metrics) == 1
        assert metrics[0]["domain"] == "vendas"
        assert metrics[0]["is_official"] is True

    def test_update_troca_is_official_de_true_para_false(self, db):
        db.upsert_finance_metric(
            "cobranca_oficial",
            name="Inadimplência oficial",
            description="",
            source_table="p.d.t",
            sql_template="SELECT 1",
            domain="cobranca",
            is_official=True,
        )
        db.upsert_finance_metric(
            "cobranca_oficial",
            name="Inadimplência oficial",
            description="",
            source_table="p.d.t",
            sql_template="SELECT 1",
            domain="cobranca",
            is_official=False,
        )

        metric = db.get_finance_metric("cobranca_oficial")
        assert metric["is_official"] is False
