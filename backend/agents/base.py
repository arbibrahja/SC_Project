"""
Base class for all OLAP agents.
Each agent encapsulates a specific OLAP capability.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd


@dataclass
class AgentInput:
    """Structured input passed to an agent."""
    operation: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    context: Optional[str] = None


@dataclass
class AgentOutput:
    """Structured output returned by an agent."""
    agent: str
    operation: str
    sql: str
    data: Optional[pd.DataFrame] = None
    summary: str = ""
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "agent": self.agent,
            "operation": self.operation,
            "sql": self.sql,
            "data": [{k: (None if isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")) else v) for k, v in row.items()} for row in self.data.to_dict("records")] if self.data is not None else [],
            "columns": list(self.data.columns) if self.data is not None else [],
            "row_count": len(self.data) if self.data is not None else 0,
            "summary": self.summary,
            "error": self.error,
            "metadata": self.metadata,
        }


class BaseAgent(ABC):
    """Abstract base for all OLAP agents."""

    name: str = "base"
    description: str = ""

    def __init__(self, db_path: Optional[str] = None):
        from backend.core.database import query_df, ensure_db
        self._query_df = query_df
        self._db_path = db_path

    def execute_sql(self, sql: str, params=None) -> pd.DataFrame:
        return self._query_df(sql, params, self._db_path)

    @abstractmethod
    def run(self, agent_input: AgentInput) -> AgentOutput:
        """Execute the agent's operation and return structured output."""
        ...

    def _fmt_currency(self, val: float) -> str:
        return f"${val:,.2f}"

    def _fmt_pct(self, val: float) -> str:
        return f"{val:.2f}%"
