from __future__ import annotations

from src.core.base_agent import BaseAgent


class AgentRegistry:
    def __init__(self) -> None:
        self._agents: dict[str, BaseAgent] = {}

    def register(self, agent: BaseAgent) -> None:
        self._agents[agent.agent_id] = agent

    def get(self, agent_id: str) -> BaseAgent:
        if agent_id not in self._agents:
            raise KeyError(f"Agent nao registrado: {agent_id}")
        return self._agents[agent_id]

    def list_ids(self) -> list[str]:
        return sorted(self._agents.keys())
