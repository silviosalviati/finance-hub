from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAgent(ABC):
    @property
    @abstractmethod
    def agent_id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def display_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def analyze(self, query: str, project_id: str, dataset_hint: str | None = None) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def runtime_info(self) -> dict[str, str]:
        raise NotImplementedError
