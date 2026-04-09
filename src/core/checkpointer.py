from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass
class CheckpointConfig:
    base_dir: Path
    ttl_hours: int = 24


class FileCheckpointer:
    def __init__(self, config: CheckpointConfig) -> None:
        self.config = config
        self.config.base_dir.mkdir(parents=True, exist_ok=True)

    def _file_path(self, key: str) -> Path:
        safe_key = key.replace("/", "_").replace("\\", "_")
        return self.config.base_dir / f"{safe_key}.json"

    def save(self, key: str, payload: dict[str, Any]) -> None:
        data = {
            "saved_at": datetime.utcnow().isoformat(),
            "payload": payload,
        }
        self._file_path(key).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def load(self, key: str) -> dict[str, Any] | None:
        file_path = self._file_path(key)
        if not file_path.exists():
            return None

        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
            saved_at = datetime.fromisoformat(data["saved_at"])
            if datetime.utcnow() - saved_at > timedelta(hours=self.config.ttl_hours):
                file_path.unlink(missing_ok=True)
                return None
            return data.get("payload")
        except Exception:
            file_path.unlink(missing_ok=True)
            return None
