"""Testes do CRUD de tags de agentes e das tags de acesso do usuário —
suporte ao controle de acesso a agentes por tag de categoria (OR logic,
admin sempre passa, agente sem tag fica visível para todos)."""

from __future__ import annotations

import pytest


@pytest.fixture()
def db(tmp_path, monkeypatch):
    from src.core import database

    monkeypatch.setattr(database, "_DB_PATH", tmp_path / "test_app.db")
    database.init_db()
    return database


class TestAgentTagAssignments:
    def test_agent_sem_tags_retorna_lista_vazia(self, db):
        assert db.get_agent_tags("query_analyzer") == []

    def test_upsert_e_leitura_de_tags(self, db):
        result = db.upsert_agent_tags("finance_auditor", ["finops", "financeiro"])
        assert result["agent_id"] == "finance_auditor"
        assert set(result["tags"]) == {"finops", "financeiro"}
        assert set(db.get_agent_tags("finance_auditor")) == {"finops", "financeiro"}

    def test_upsert_sobrescreve_tags_anteriores(self, db):
        db.upsert_agent_tags("document_build", ["documentacao"])
        db.upsert_agent_tags("document_build", ["docs"])
        assert db.get_agent_tags("document_build") == ["docs"]

    def test_list_agent_tag_assignments_so_lista_agentes_com_tag(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        assignments = db.list_agent_tag_assignments()
        assert assignments == {"finance_auditor": ["finops"]}


class TestUserAgentTags:
    def test_create_user_persiste_agent_tags(self, db):
        db.create_user("u1", "senha123", "Usuário 1", False, agent_tags="documentacao,finops")
        u = db.get_user("u1")
        assert u["agent_tags"] == "documentacao,finops"

    def test_update_user_altera_agent_tags(self, db):
        db.create_user("u2", "senha123", "Usuário 2", False)
        db.update_user("u2", agent_tags="finops")
        assert db.get_user("u2")["agent_tags"] == "finops"

    def test_list_users_inclui_agent_tags(self, db):
        db.create_user("u3", "senha123", "Usuário 3", False, agent_tags="finops")
        users = db.list_users()
        u3 = next(u for u in users if u["username"] == "u3")
        assert u3["agent_tags"] == "finops"


class TestListDistinctTags:
    def test_uniao_de_tags_de_agentes_e_usuarios(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        db.create_user("u4", "senha123", "Usuário 4", False, agent_tags="documentacao,finops")
        assert db.list_distinct_tags() == ["documentacao", "finops"]


class TestDeleteTagEverywhere:
    def test_remove_tag_de_agentes_e_usuarios(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops", "financeiro"])
        db.upsert_agent_tags("document_build", ["finops"])
        db.create_user("u5", "senha123", "Usuário 5", False, agent_tags="finops,documentacao")

        result = db.delete_tag_everywhere("finops")

        assert result == {"agents_updated": 2, "users_updated": 1}
        assert db.get_agent_tags("finance_auditor") == ["financeiro"]
        assert db.get_agent_tags("document_build") == []
        assert db.get_user("u5")["agent_tags"] == "documentacao"

    def test_tag_inexistente_nao_altera_nada(self, db):
        db.upsert_agent_tags("finance_auditor", ["financeiro"])
        result = db.delete_tag_everywhere("nao_existe")
        assert result == {"agents_updated": 0, "users_updated": 0}
        assert db.get_agent_tags("finance_auditor") == ["financeiro"]


class TestUserCanAccessAgent:
    def test_agente_sem_tag_e_visivel_para_todos(self):
        from src.shared.guardrails.agent_access import user_can_access_agent

        assert user_can_access_agent([], set()) is True
        assert user_can_access_agent([], {"qualquer"}) is True

    def test_agente_com_tag_exige_intersecao(self):
        from src.shared.guardrails.agent_access import user_can_access_agent

        assert user_can_access_agent(["finops"], {"finops"}) is True
        assert user_can_access_agent(["finops"], {"documentacao"}) is False

    def test_or_logic_basta_uma_tag_em_comum(self):
        from src.shared.guardrails.agent_access import user_can_access_agent

        assert user_can_access_agent(["finops", "financeiro"], {"documentacao", "financeiro"}) is True
