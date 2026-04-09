from __future__ import annotations


def ensure_not_empty(value: str, field_name: str) -> str:
    clean = value.strip()
    if not clean:
        raise ValueError(f"{field_name} nao pode ser vazio.")
    return clean
