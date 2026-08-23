"""Controle de acesso a agentes por tag de categoria (lógica OR).

Um agente sem tag atribuída é visível para todos (default seguro — evita que
usuários percam acesso a agentes ainda não classificados pelo admin). Quando o
agente tem ao menos uma tag, o usuário precisa compartilhar pelo menos uma
delas para ter acesso.

O bypass de admin é feito pelo chamador (rotas em `src/api/routes/agents.py`),
não aqui, para manter esta função pura e testável isoladamente sem precisar
simular uma sessão de admin.
"""
from __future__ import annotations


def user_can_access_agent(agent_tags: list[str], user_tags: set[str]) -> bool:
    if not agent_tags:
        return True
    return bool(set(agent_tags) & user_tags)
