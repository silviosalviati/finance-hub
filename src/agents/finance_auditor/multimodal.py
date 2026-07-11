"""Suporte multi-modal — CSV (stdlib) e imagem (Gemini multimodal)."""

from __future__ import annotations

import base64
import binascii
import csv
import io
from typing import Any

from langchain_core.messages import HumanMessage

from src.shared.tools.llm import invoke_with_retry


KIND_CSV = "csv"
KIND_IMAGE = "image"
VALID_KINDS = {KIND_CSV, KIND_IMAGE}

_MAX_CSV_BYTES = 2 * 1024 * 1024     # 2 MiB
_MAX_IMAGE_BYTES = 4 * 1024 * 1024   # 4 MiB
_MAX_CSV_ROWS = 1000


def _decode_base64(data: str) -> bytes:
    try:
        return base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"base64 inválido: {exc}") from exc


def parse_csv(b64: str, delimiter: str | None = None) -> dict[str, Any]:
    raw = _decode_base64(b64)
    if len(raw) > _MAX_CSV_BYTES:
        raise ValueError(f"CSV excede limite de {_MAX_CSV_BYTES} bytes.")
    text = raw.decode("utf-8", errors="replace")
    sample = text[:4096]
    if delimiter is None:
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows: list[dict[str, Any]] = []
    for i, row in enumerate(reader):
        if i >= _MAX_CSV_ROWS:
            break
        rows.append({k: (v if v != "" else None) for k, v in row.items()})
    columns = list(rows[0].keys()) if rows else (reader.fieldnames or [])
    return {
        "delimiter": delimiter,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": bool(len(rows) >= _MAX_CSV_ROWS),
    }


def describe_image_with_llm(
    b64: str,
    prompt: str,
    llm: Any,
    mime_type: str = "image/png",
    usage_sink: list[dict[str, Any]] | None = None,
    run_config: dict[str, Any] | None = None,
) -> str:
    """Envia a imagem ao LLM (Gemini multimodal) e devolve a descrição/análise."""
    raw = _decode_base64(b64)
    if len(raw) > _MAX_IMAGE_BYTES:
        raise ValueError(f"Imagem excede limite de {_MAX_IMAGE_BYTES} bytes.")
    if llm is None:
        raise ValueError("LLM indisponível para análise multimodal.")

    message = HumanMessage(
        content=[
            {"type": "text", "text": prompt or "Descreva o conteúdo desta imagem em PT-BR."},
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{b64}"},
            },
        ]
    )
    response = invoke_with_retry(
        llm, [message], max_attempts=2, label="attachment_analyze_image",
        usage_sink=usage_sink, run_config=run_config,
    )
    return str(getattr(response, "content", response) or "")


__all__ = [
    "KIND_CSV", "KIND_IMAGE", "VALID_KINDS",
    "parse_csv", "describe_image_with_llm",
]
