from src.shared.utils.formatting import format_bytes


def test_format_bytes_human_readable():
    assert format_bytes(1024) == "1.00 KB"
