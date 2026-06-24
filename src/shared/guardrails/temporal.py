"""Ancoragem de data real em prompts de LLM.

Sem isso, qualquer etapa de um pipeline que precise traduzir um período
relativo ("últimos 12 meses") para algo concreto tende a confabular "hoje"
a partir da própria suposição do modelo — já produziu bug real e
contraditório no Finance Auditor (Composer narrando um período do passado
errado, Planner calculando `date_start`/`date_end` literais errados pro
`metric_execute`). Qualquer agente nessa mesma situação deve usar isto em
vez de reescrever a lógica.
"""

from __future__ import annotations

from datetime import date

_MESES_PT = (
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
)


def _format_date_extenso(today: date) -> str:
    return f"{today.day} de {_MESES_PT[today.month - 1]} de {today.year}"


def get_date_block(today: date) -> str:
    """Ancora a data atual em prompts que só NARRAM um resultado já obtido
    (ex.: o Composer do Finance Auditor) — não calculam filtro nenhum.
    """
    extenso = _format_date_extenso(today)
    return (
        "CONTEXTO TEMPORAL:\n"
        f"Hoje é {extenso} ({today.isoformat()}). Ao mencionar QUALQUER "
        "período relativo (\"últimos N meses\", \"este ano\", \"ano "
        "passado\" etc.), calcule a partir desta data — nunca assuma ou "
        "estime \"hoje\" por conta própria. Para informar o período EXATO "
        "de um resultado, use as datas mínima/máxima que de fato aparecem "
        "nas linhas (`rows`) retornadas pelas capabilities, nunca o período "
        "que você imagina ter sido pedido. Se os dados não cobrirem o "
        "intervalo solicitado, diga isso explicitamente (com as datas reais "
        "que você encontrou) em vez de inventar um intervalo plausível."
    )


def get_planner_date_block(today: date) -> str:
    """Ancora a data atual em prompts que DECIDEM o que buscar (ex.: o
    Planner do Finance Auditor, que calcula `date_start`/`date_end` \
literais para `metric_execute`) — mais crítico que `get_date_block`, \
porque um erro aqui não tem como ser corrigido por uma etapa posterior \
que só narra o que já foi buscado.
    """
    extenso = _format_date_extenso(today)
    return (
        "CONTEXTO TEMPORAL:\n"
        f"Hoje é {extenso} ({today.isoformat()}). Use esta data como base \
SEMPRE que precisar calcular um período relativo (\"últimos N meses/dias/\
anos\", \"este ano\", \"ano passado\" etc.) — nunca assuma ou estime \"hoje\" \
por conta própria. Isso vale especialmente para `metric_execute`: seus args \
exigem `date_start`/`date_end` literais (YYYY-MM-DD) que VOCÊ calcula — um \
erro aqui faz a busca inteira rodar no período errado, e nem uma etapa \
posterior que só narra o resultado consegue corrigir depois. Para gerar SQL \
livre, ao contrário, NÃO calcule datas absolutas — passe a referência \
relativa como o usuário disse (ex.: \"últimos 12 meses\") direto pro gerador \
de SQL; a resolução fica a cargo do SQL gerado, que usa CURRENT_DATE() do \
próprio BigQuery (mais confiável que qualquer cálculo seu)."
    )


__all__ = ["get_date_block", "get_planner_date_block"]
