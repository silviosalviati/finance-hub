from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from src.shared.tools.bigquery import (
    _json_safe_row,
    _json_safe_value,
    _resolve_credentials_path,
)
from src.shared.utils.formatting import format_bytes


def test_format_bytes_human_readable():
    assert format_bytes(1024) == "1.00 KB"


def test_json_safe_value_converts_decimal_and_dates():
    assert _json_safe_value(Decimal("10.50")) == "10.50"
    assert _json_safe_value(date(2026, 4, 26)) == "2026-04-26"
    assert _json_safe_value(datetime(2026, 4, 26, 10, 30, 0)) == "2026-04-26T10:30:00"


def test_json_safe_row_converts_nested_values():
    row = {
        "total": Decimal("99.99"),
        "periodo": date(2026, 4, 1),
        "meta": {
            "updated_at": datetime(2026, 4, 26, 8, 0, 0),
            "ratios": [Decimal("1.10"), Decimal("2.20")],
        },
    }

    safe = _json_safe_row(row)

    assert safe["total"] == "99.99"
    assert safe["periodo"] == "2026-04-01"
    assert safe["meta"]["updated_at"] == "2026-04-26T08:00:00"
    assert safe["meta"]["ratios"] == ["1.10", "2.20"]


def test_resolve_credentials_path_keeps_absolute_path(tmp_path):
    creds_path = tmp_path / "credentials.json"
    resolved = _resolve_credentials_path(str(creds_path))

    assert Path(resolved) == creds_path


def test_resolve_credentials_path_resolves_relative_to_project_root():
    resolved = Path(_resolve_credentials_path("secrets/credentials.json"))

    assert resolved.name == "credentials.json"
    assert resolved.parts[-2] == "secrets"
    assert resolved.is_absolute()
