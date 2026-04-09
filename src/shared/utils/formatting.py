from __future__ import annotations


def format_bytes(value: int | float | None) -> str:
    if value is None:
        return "0.00 B"

    n = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024

    return f"{n:.2f} EB"
