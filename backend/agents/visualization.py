"""
Optional Agent 5: Visualization Agent
Selects the appropriate chart type for a given data shape and OLAP operation,
then returns chart configuration (JSON) compatible with Plotly/Chart.js.
"""

from backend.agents.base import BaseAgent, AgentInput, AgentOutput
import pandas as pd
import json


CHART_RULES = {
    "trend": "line",
    "mom_change": "line",
    "yoy_growth": "bar",
    "drill_down_time": "bar",
    "top_n": "bar",
    "ranking": "bar",
    "compare_periods": "grouped_bar",
    "slice": "bar",
    "dice": "bar",
    "profit_margins": "bar",
    "pivot": "heatmap",
    "group": "bar",
    "distribution": "pie",
}


def _recommend_chart(operation: str, data: pd.DataFrame) -> str:
    if data is None or data.empty:
        return "bar"
    n_rows, n_cols = data.shape
    numeric_cols = data.select_dtypes(include="number").columns.tolist()

    # Heuristic rules
    if operation in CHART_RULES:
        return CHART_RULES[operation]
    if n_rows <= 5 and len(numeric_cols) == 1:
        return "pie"
    if n_rows > 12 and len(numeric_cols) == 1:
        return "line"
    if len(numeric_cols) > 3:
        return "heatmap"
    return "bar"


def _build_plotly_config(chart_type: str, data: pd.DataFrame,
                          title: str = "", x_col: str = None,
                          y_col: str = None) -> dict:
    if data is None or data.empty:
        return {}

    cols = data.columns.tolist()
    numeric_cols = data.select_dtypes(include="number").columns.tolist()
    str_cols = [c for c in cols if c not in numeric_cols]

    x = x_col or (str_cols[0] if str_cols else cols[0])
    y = y_col or (numeric_cols[0] if numeric_cols else cols[-1])

    config = {
        "chart_type": chart_type,
        "title": title,
        "x_axis": x,
        "y_axis": y,
        "plotly": {}
    }

    x_vals = data[x].astype(str).tolist() if x in data.columns else []
    y_vals = data[y].tolist() if y in data.columns else []

    if chart_type == "bar":
        config["plotly"] = {
            "data": [{"type": "bar", "x": x_vals, "y": y_vals, "name": y}],
            "layout": {
                "title": title,
                "xaxis": {"title": x, "tickangle": -30},
                "yaxis": {"title": y},
                "template": "plotly_white"
            }
        }
    elif chart_type == "line":
        config["plotly"] = {
            "data": [{"type": "scatter", "mode": "lines+markers",
                      "x": x_vals, "y": y_vals, "name": y}],
            "layout": {
                "title": title,
                "xaxis": {"title": x},
                "yaxis": {"title": y},
                "template": "plotly_white"
            }
        }
    elif chart_type == "pie":
        config["plotly"] = {
            "data": [{"type": "pie", "labels": x_vals, "values": y_vals}],
            "layout": {"title": title, "template": "plotly_white"}
        }
    elif chart_type == "grouped_bar":
        traces = []
        for col in numeric_cols[:4]:  # max 4 series
            traces.append({
                "type": "bar",
                "name": col,
                "x": x_vals,
                "y": data[col].tolist()
            })
        config["plotly"] = {
            "data": traces,
            "layout": {
                "barmode": "group", "title": title,
                "xaxis": {"title": x}, "template": "plotly_white"
            }
        }
    elif chart_type == "heatmap":
        # For pivot tables: rows = index, cols = numeric columns
        numeric_data = data.select_dtypes(include="number")
        z_vals = numeric_data.values.tolist()
        y_labels = data[str_cols[0]].astype(str).tolist() if str_cols else list(range(len(data)))
        x_labels = numeric_data.columns.astype(str).tolist()
        config["plotly"] = {
            "data": [{"type": "heatmap", "z": z_vals, "x": x_labels,
                      "y": y_labels, "colorscale": "Blues"}],
            "layout": {"title": title, "template": "plotly_white"}
        }
    else:
        config["plotly"] = {}

    return config


class VisualizationAgent(BaseAgent):
    name = "Visualization"
    description = (
        "Selects appropriate chart types and builds Plotly-compatible chart "
        "configurations for OLAP query results."
    )

    def run(self, agent_input: AgentInput) -> AgentOutput:
        p = agent_input.parameters
        data_records = p.get("data", [])
        columns = p.get("columns", [])
        operation = p.get("source_operation", "bar")
        title = p.get("title", "OLAP Analysis")
        x_col = p.get("x_col")
        y_col = p.get("y_col")

        if data_records:
            df = pd.DataFrame(data_records, columns=columns or None)
        else:
            df = pd.DataFrame()

        chart_type = _recommend_chart(operation, df)
        chart_config = _build_plotly_config(chart_type, df, title, x_col, y_col)

        summary = f"Recommended chart: {chart_type} for operation '{operation}'."

        return AgentOutput(
            agent=self.name, operation="visualize",
            sql="-- No SQL (visualization only)",
            data=df, summary=summary,
            metadata={"chart_type": chart_type, "chart_config": chart_config}
        )
