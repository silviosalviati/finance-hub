"""Analise deterministica e politicas de seguranca do Query Transformer.

A SQL do usuario e tratada como codigo nao confiavel. A LLM pode explicar e
propor uma transformacao, mas nunca decide sozinha se a entrada e segura nem
se uma operacao mutavel pode virar um modelo Dataform.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError


@dataclass(frozen=True)
class SecurityFinding:
    code: str
    severity: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
        }


@dataclass
class SQLAnalysis:
    normalized_sql: str
    sql_kind: str
    statement_count: int
    tables: list[dict[str, str]] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    has_aggregation: bool = False
    has_window_function: bool = False
    has_order_by: bool = False
    has_nondeterministic_function: bool = False
    partition_candidates: list[str] = field(default_factory=list)
    security_findings: list[SecurityFinding] = field(default_factory=list)
    parse_error: str | None = None

    @property
    def blocking_findings(self) -> list[SecurityFinding]:
        return [
            finding
            for finding in self.security_findings
            if finding.severity == "critical"
        ]

    def as_dict(self) -> dict[str, Any]:
        return {
            "normalized_sql": self.normalized_sql,
            "sql_kind": self.sql_kind,
            "statement_count": self.statement_count,
            "tables": self.tables,
            "columns": self.columns,
            "has_aggregation": self.has_aggregation,
            "has_window_function": self.has_window_function,
            "has_order_by": self.has_order_by,
            "has_nondeterministic_function": self.has_nondeterministic_function,
            "partition_candidates": self.partition_candidates,
            "security_findings": [finding.as_dict() for finding in self.security_findings],
            "parse_error": self.parse_error,
        }


def _table_dict(table: exp.Table) -> dict[str, str]:
    return {
        "project": str(table.catalog or ""),
        "dataset": str(table.db or ""),
        "table": str(table.name or ""),
        "full_name": ".".join(
            part for part in (str(table.catalog or ""), str(table.db or ""), str(table.name or "")) if part
        ),
    }


def _security_findings(statements: list[exp.Expression], statement_count: int) -> list[SecurityFinding]:
    findings: list[SecurityFinding] = []
    forbidden = {
        exp.Insert: ("DML_INSERT", "INSERT nao pode ser convertido automaticamente."),
        exp.Update: ("DML_UPDATE", "UPDATE nao pode ser convertido automaticamente."),
        exp.Delete: ("DML_DELETE", "DELETE nao pode ser convertido automaticamente."),
        exp.Merge: ("DML_MERGE", "MERGE exige contrato incremental confirmado pelo usuario."),
        exp.Create: ("DDL_CREATE", "CREATE nao pode ser convertido automaticamente."),
        exp.Drop: ("DDL_DROP", "DROP nao pode ser convertido automaticamente."),
        exp.Alter: ("DDL_ALTER", "ALTER nao pode ser convertido automaticamente."),
        exp.TruncateTable: ("DDL_TRUNCATE", "TRUNCATE nao pode ser convertido automaticamente."),
        exp.Command: ("COMMAND", "Comandos BigQuery nao sao convertidos automaticamente."),
    }
    for statement in statements:
        is_root_command = isinstance(statement, exp.Command)
        command_sql = statement.sql(dialect="bigquery").lstrip().upper() if is_root_command else ""
        for node_type, (code, message) in forbidden.items():
            if node_type is exp.Command and command_sql.startswith("CALL "):
                continue
            if isinstance(statement, node_type) or statement.find(node_type):
                findings.append(SecurityFinding(code, "critical", message))
        if is_root_command and command_sql.startswith("CALL "):
            findings.append(SecurityFinding("PROCEDURE", "high", "Procedures precisam de um contrato de conversao explicito."))
        elif is_root_command or statement.find(exp.Execute):
            findings.append(SecurityFinding("DYNAMIC_SQL", "critical", "EXECUTE IMMEDIATE e SQL dinamica sao bloqueados."))
        if statement.find(exp.Anonymous):
            findings.append(SecurityFinding("SCRIPT", "high", "Scripts anonimos multi-statement precisam ser decompostos antes da conversao."))
        if statement.find(exp.Table):
            for table in statement.find_all(exp.Table):
                if not table.catalog or not table.db:
                    findings.append(SecurityFinding(
                        "UNQUALIFIED_TABLE",
                        "medium",
                        f"A tabela '{table.name}' nao esta totalmente qualificada.",
                    ))
    if statement_count > 1:
        findings.append(SecurityFinding("MULTI_STATEMENT", "high", "A entrada possui mais de um statement."))
    return findings


def analyze_sql(sql: str) -> SQLAnalysis:
    normalized = (sql or "").strip()
    if not normalized:
        return SQLAnalysis(normalized_sql="", sql_kind="empty", statement_count=0)

    try:
        statements = sqlglot.parse(normalized, read="bigquery")
    except ParseError as exc:
        return SQLAnalysis(
            normalized_sql=normalized,
            sql_kind="parse_error",
            statement_count=0,
            parse_error=str(exc),
            security_findings=[SecurityFinding("PARSE_ERROR", "high", "A SQL nao pode ser analisada com o dialeto BigQuery.")],
        )

    statement_count = len(statements)
    root = statements[0] if statements else None
    if root is None:
        kind = "empty"
    elif normalized.lstrip().upper().startswith("CALL "):
        kind = "procedure_call"
    elif isinstance(root, exp.Command) or root.find(exp.Execute):
        kind = "dynamic_sql"
    elif isinstance(root, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        kind = "dml"
    elif isinstance(root, (exp.Create, exp.Drop, exp.Alter, exp.TruncateTable)):
        kind = "ddl"
    elif statement_count > 1:
        kind = "multi_statement"
    elif isinstance(root, (exp.Select, exp.Union)) or root.find(exp.Select):
        kind = "single_select"
    else:
        kind = "unsupported"

    tables = []
    seen_tables: set[str] = set()
    for statement in statements:
        for table in statement.find_all(exp.Table):
            item = _table_dict(table)
            key = item["full_name"] or item["table"]
            if key and key not in seen_tables:
                tables.append(item)
                seen_tables.add(key)

    columns = sorted({str(column.name) for statement in statements for column in statement.find_all(exp.Column) if column.name})
    has_aggregation = any(statement.find(exp.AggFunc) for statement in statements)
    has_window = any(statement.find(exp.Window) for statement in statements)
    has_order = any(statement.find(exp.Order) for statement in statements)
    nondeterministic_names = {"current_date", "current_datetime", "current_timestamp", "rand", "random"}
    has_nondeterministic = any(
        str(node.name or node.sql_name()).lower() in nondeterministic_names
        for statement in statements
        for node in statement.find_all(exp.Func)
    )

    partition_candidates = sorted({
        str(column.name)
        for statement in statements
        for predicate in statement.find_all(exp.Predicate)
        for column in predicate.find_all(exp.Column)
        if column.name and any(token in column.name.lower() for token in ("date", "dt", "timestamp", "updated", "created"))
    })

    return SQLAnalysis(
        normalized_sql=normalized,
        sql_kind=kind,
        statement_count=statement_count,
        tables=tables,
        columns=columns,
        has_aggregation=has_aggregation,
        has_window_function=has_window,
        has_order_by=has_order,
        has_nondeterministic_function=has_nondeterministic,
        partition_candidates=partition_candidates,
        security_findings=_security_findings(statements, statement_count),
    )


def architecture_questions(analysis: SQLAnalysis) -> list[dict[str, Any]]:
    """Gera somente perguntas que alteram a semantica do modelo Dataform."""
    if analysis.sql_kind in {"procedure_call", "multi_statement", "dml", "ddl", "dynamic_sql", "unsupported", "parse_error"}:
        return [{
            "id": "conversion_scope",
            "label": "Qual parte da entrada deve virar um modelo Dataform?",
            "type": "select",
            "options": [
                "Extrair somente o SELECT final",
                "Separar as etapas em modelos intermediarios",
                "Converter como modelo incremental",
                "Nao converter automaticamente",
            ],
            "required": True,
        }]

    questions: list[dict[str, Any]] = []
    if analysis.has_aggregation or analysis.partition_candidates:
        questions.append({
            "id": "refresh_strategy",
            "label": "Como os dados de origem sao atualizados?",
            "type": "select",
            "options": [
                "Recebe apenas novos registros",
                "Recebe correcoes em registros existentes",
                "E apagada e recriada com snapshot completo",
                "Nao sei",
            ],
            "required": True,
        })
    if analysis.partition_candidates:
        questions.append({
            "id": "watermark_column",
            "label": "Qual coluna indica a data ou hora de atualizacao?",
            "type": "column_picker",
            "options": analysis.partition_candidates,
            "required": False,
        })
        questions.append({
            "id": "late_arrival_policy",
            "label": "Existem registros atrasados ou correcoes retroativas?",
            "type": "select",
            "options": ["Nao", "Sim, reprocessar uma janela", "Sim, recalcular o historico", "Nao sei"],
            "required": True,
        })
    if analysis.has_aggregation:
        questions.append({
            "id": "unique_key",
            "label": "Quais campos identificam unicamente cada linha do resultado?",
            "type": "text",
            "options": [],
            "required": False,
        })
    return questions


def assess_architecture(analysis: SQLAnalysis) -> dict[str, Any]:
    questions = architecture_questions(analysis)
    if analysis.sql_kind != "single_select":
        recommendation = "manual_review"
        confidence = 0.0
    elif analysis.has_aggregation and analysis.partition_candidates:
        recommendation = "incremental_pending_confirmation"
        confidence = 0.55
    elif analysis.has_aggregation:
        recommendation = "table"
        confidence = 0.7
    else:
        recommendation = "view"
        confidence = 0.65
    return {
        "recommendation": recommendation,
        "confidence": confidence,
        "required_questions": questions,
    }
