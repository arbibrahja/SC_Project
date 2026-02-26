"""
Planner / Orchestrator
Parses natural language business queries, selects appropriate agents,
coordinates execution, and assembles the final response.

Uses the Anthropic API for query understanding.
"""

import json
import os
import re
from typing import Optional
from dataclasses import dataclass, field

from backend.agents.base import AgentInput, AgentOutput
from backend.agents.dimension_navigator import DimensionNavigatorAgent
from backend.agents.cube_operations import CubeOperationsAgent
from backend.agents.kpi_calculator import KPICalculatorAgent
from backend.agents.report_generator import ReportGeneratorAgent
from backend.agents.visualization import VisualizationAgent
from backend.agents.anomaly_detection import AnomalyDetectionAgent


SYSTEM_PROMPT = """You are an OLAP query planner for a Business Intelligence system.
Your job is to parse natural language business questions and output a JSON execution plan.

Available agents and their operations:
1. DimensionNavigator: drill_down, roll_up, group
2. CubeOperations: slice, dice, pivot, drill_through
3. KPICalculator: yoy_growth, mom_change, compare_periods, top_n, profit_margins, summary
4. ReportGenerator: executive_summary, trend_report, format_table
5. Visualization: visualize
6. AnomalyDetection: monthly_anomaly, product_anomaly

Dimension values (use EXACT values):
- year: 2022, 2023, 2024
- quarter: "Q1", "Q2", "Q3", "Q4"
- month_name: "January", "February", ..., "December"
- region: "North America", "Europe", "Asia Pacific", "Latin America"
- category: "Electronics", "Furniture", "Office Supplies", "Clothing"
- customer_segment: "Consumer", "Corporate", "Home Office"

Output a JSON object with this structure:
{
  "intent": "one sentence description of what the user wants",
  "steps": [
    {
      "agent": "AgentName",
      "operation": "operation_name",
      "parameters": { ... }
    }
  ],
  "always_include_report": true,
  "suggested_followups": ["follow-up question 1", "follow-up question 2"]
}

Rules:
- Always include a ReportGenerator step at the end unless the user only asks for raw data.
- Always include a Visualization step for data that would benefit from a chart.
- For comparisons, always use KPICalculator with compare_periods.
- For drill-down requests, use DimensionNavigator with drill_down.
- For "top N" questions, use KPICalculator with top_n.
- Infer year = 2024 when user says "this year" or "current year".
- Infer year = 2023 when user says "last year".
- For "overall summary" or vague questions, use KPICalculator summary + ReportGenerator executive_summary.
- Output ONLY the JSON, no explanation, no markdown code fences.
"""


@dataclass
class OrchestratorResult:
    intent: str
    steps_executed: list[dict]
    outputs: list[AgentOutput]
    narrative: str
    suggested_followups: list[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "intent": self.intent,
            "steps_executed": self.steps_executed,
            "outputs": [o.to_dict() for o in self.outputs],
            "narrative": self.narrative,
            "suggested_followups": self.suggested_followups,
            "error": self.error,
        }


class OLAPOrchestrator:
    def __init__(self, db_path: Optional[str] = None, api_key: Optional[str] = None):
        self._db = db_path
        self._api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._agents = {
            "DimensionNavigator": DimensionNavigatorAgent(db_path),
            "CubeOperations": CubeOperationsAgent(db_path),
            "KPICalculator": KPICalculatorAgent(db_path),
            "ReportGenerator": ReportGeneratorAgent(db_path),
            "Visualization": VisualizationAgent(db_path),
            "AnomalyDetection": AnomalyDetectionAgent(db_path),
        }
        self._history: list[dict] = []

    def _call_llm(self, user_query: str) -> dict:
        """Call Claude API to get the execution plan."""
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self._api_key)

            messages = self._history[-6:] + [{"role": "user", "content": user_query}]

            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1500,
                system=SYSTEM_PROMPT,
                messages=messages,
            )
            raw = response.content[0].text.strip()
            # Strip markdown code fences if present
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
            return json.loads(raw)

        except ImportError:
            return self._rule_based_plan(user_query)
        except Exception as e:
            return self._rule_based_plan(user_query, fallback_error=str(e))

    def _rule_based_plan(self, query: str, fallback_error: str = "") -> dict:
        """
        Fallback rule-based planner when Claude API is unavailable.
        Handles common OLAP patterns with keyword matching.
        """
        q = query.lower()
        steps = []

        # Detect year mentions
        years = [y for y in [2022, 2023, 2024] if str(y) in q]
        quarters = [qtr for qtr in ["Q1", "Q2", "Q3", "Q4"] if qtr.lower() in q]
        regions = [r for r in ["north america", "europe", "asia pacific", "latin america"] if r in q]
        categories = [c for c in ["electronics", "furniture", "office supplies", "clothing"] if c in q]

        filters = {}
        if len(years) == 1:
            filters["year"] = years[0]
        if quarters:
            filters["quarter"] = quarters[0]
        if regions:
            filters["region"] = regions[0].title()
        if categories:
            filters["category"] = categories[0].title()

        if "compare" in q or "vs" in q or "versus" in q or "growth" in q:
            if len(years) >= 2:
                steps.append({
                    "agent": "KPICalculator", "operation": "compare_periods",
                    "parameters": {
                        "period_a": {"year": min(years)},
                        "period_b": {"year": max(years)},
                        "group_by": "region" if "region" in q else ("category" if "categor" in q else None)
                    }
                })
            else:
                steps.append({"agent": "KPICalculator", "operation": "yoy_growth",
                               "parameters": {"dimension": "region" if "region" in q else None}})

        elif "drill" in q and ("down" in q or "into" in q or "break" in q):
            hierarchy = "time"
            to_level = "quarter"
            if "month" in q:
                to_level = "month"
            if "country" in q or "countr" in q:
                hierarchy = "geography"
                to_level = "country"
            if "subcategor" in q:
                hierarchy = "product"
                to_level = "subcategory"
            steps.append({
                "agent": "DimensionNavigator", "operation": "drill_down",
                "parameters": {"hierarchy": hierarchy, "to_level": to_level, "filters": filters}
            })

        elif "top" in q:
            n = 5
            for word in q.split():
                if word.isdigit():
                    n = int(word)
                    break
            dimension = "country" if "countr" in q else ("category" if "categor" in q else "subcategory" if "sub" in q else "region")
            measure = "profit" if "profit" in q else "revenue"
            steps.append({
                "agent": "KPICalculator", "operation": "top_n",
                "parameters": {"n": n, "dimension": dimension, "measure": measure, "filters": filters}
            })

        elif "trend" in q or "month" in q or "monthly" in q:
            year = years[0] if years else 2024
            steps.append({
                "agent": "ReportGenerator", "operation": "trend_report",
                "parameters": {"year": year}
            })

        elif "slice" in q or len(filters) == 1:
            group_by = "region"
            if "category" in q or "product" in q:
                group_by = "category"
            steps.append({
                "agent": "CubeOperations", "operation": "slice",
                "parameters": {"filter": filters, "group_by": [group_by]}
            })

        elif "dice" in q or len(filters) >= 2:
            group_by = "country" if "countr" in q else "subcategory" if "sub" in q else "region"
            steps.append({
                "agent": "CubeOperations", "operation": "dice",
                "parameters": {"filters": filters, "group_by": [group_by]}
            })

        elif "pivot" in q:
            steps.append({
                "agent": "CubeOperations", "operation": "pivot",
                "parameters": {"row_dim": "region", "col_dim": "year", "measure": "revenue"}
            })

        elif "anomal" in q or "unusual" in q or "outlier" in q:
            steps.append({
                "agent": "AnomalyDetection", "operation": "monthly_anomaly",
                "parameters": {}
            })

        elif "margin" in q or "profit" in q:
            steps.append({
                "agent": "KPICalculator", "operation": "profit_margins",
                "parameters": {"dimension": "category" if "categor" in q else "region", "filters": filters}
            })

        elif "region" in q or "revenue by" in q:
            steps.append({
                "agent": "DimensionNavigator", "operation": "group",
                "parameters": {"dimensions": ["region"], "filters": filters}
            })

        if not steps:
            steps.append({
                "agent": "KPICalculator", "operation": "summary",
                "parameters": {}
            })

        # Always add summary report
        steps.append({
            "agent": "ReportGenerator", "operation": "executive_summary",
            "parameters": {}
        })

        return {
            "intent": f"Answering: '{query}'" + (f" [rule-based fallback: {fallback_error}]" if fallback_error else ""),
            "steps": steps,
            "always_include_report": True,
            "suggested_followups": [
                "Which region has the highest growth?",
                "Show me the top 5 subcategories by profit",
                "Compare 2023 vs 2024 by category",
            ]
        }

    def process(self, user_query: str) -> OrchestratorResult:
        """Main entry point: parse → plan → execute → report."""
        plan = self._call_llm(user_query)
        intent = plan.get("intent", "")
        steps = plan.get("steps", [])
        followups = plan.get("suggested_followups", [])

        outputs: list[AgentOutput] = []
        steps_executed = []

        for step in steps:
            agent_name = step.get("agent", "")
            operation = step.get("operation", "")
            parameters = step.get("parameters", {})

            agent = self._agents.get(agent_name)
            if not agent:
                continue

            agent_input = AgentInput(
                operation=operation,
                parameters=parameters,
                context=user_query
            )

            try:
                output = agent.run(agent_input)
            except Exception as e:
                output = AgentOutput(
                    agent=agent_name, operation=operation, sql="",
                    error=str(e)
                )

            outputs.append(output)
            steps_executed.append({
                "agent": agent_name,
                "operation": operation,
                "success": output.error is None,
                "row_count": len(output.data) if output.data is not None else 0,
            })

        # Build narrative from summaries
        summaries = [o.summary for o in outputs if o.summary and not o.error]
        narrative = "\n\n".join(summaries) if summaries else "No results generated."

        # Update conversation history
        self._history.append({"role": "user", "content": user_query})
        self._history.append({"role": "assistant", "content": narrative[:500]})

        return OrchestratorResult(
            intent=intent,
            steps_executed=steps_executed,
            outputs=outputs,
            narrative=narrative,
            suggested_followups=followups,
        )

    def reset_context(self):
        """Clear conversation history."""
        self._history = []
