"""
Agent 1: Dimension Navigator
Handles hierarchical navigation: drill-down and roll-up across
Time (Year → Quarter → Month), Geography (Region → Country),
and Product (Category → Subcategory).
"""

from backend.agents.base import BaseAgent, AgentInput, AgentOutput

# Hierarchy definitions
HIERARCHIES = {
    "time": {
        "levels": ["year", "quarter", "month"],
        "labels": ["Year", "Quarter", "Month"],
        "columns": ["d.year", "d.quarter", "d.month_name"],
    },
    "geography": {
        "levels": ["region", "country"],
        "labels": ["Region", "Country"],
        "columns": ["g.region", "g.country"],
    },
    "product": {
        "levels": ["category", "subcategory"],
        "labels": ["Category", "Subcategory"],
        "columns": ["p.category", "p.subcategory"],
    },
}


def _build_filter_clause(filters: dict) -> tuple[str, list]:
    """Build WHERE clause from filter dict."""
    conditions, params = [], []
    col_map = {
        "year": "d.year", "quarter": "d.quarter", "month": "d.month_name",
        "region": "g.region", "country": "g.country",
        "category": "p.category", "subcategory": "p.subcategory",
        "customer_segment": "c.customer_segment",
    }
    for key, val in filters.items():
        if key in col_map:
            if isinstance(val, list):
                placeholders = ",".join("?" * len(val))
                conditions.append(f"{col_map[key]} IN ({placeholders})")
                params.extend(val)
            else:
                conditions.append(f"{col_map[key]} = ?")
                params.append(val)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params


def _base_query(group_cols: list[str], where: str, params: list) -> tuple[str, list]:
    select_cols = ", ".join(group_cols)
    sql = f"""
        SELECT {select_cols},
               ROUND(SUM(f.revenue), 2)       AS total_revenue,
               ROUND(SUM(f.profit), 2)        AS total_profit,
               ROUND(AVG(f.profit_margin), 2) AS avg_margin,
               SUM(f.quantity)                AS total_qty,
               COUNT(*)                       AS transactions
        FROM fact_sales f
        JOIN dim_date      d ON f.date_key     = d.date_key
        JOIN dim_geography g ON f.geo_key      = g.geo_key
        JOIN dim_product   p ON f.product_key  = p.product_key
        JOIN dim_customer  c ON f.customer_key = c.customer_key
        {where}
        GROUP BY {select_cols}
        ORDER BY total_revenue DESC
    """
    return sql, params


class DimensionNavigatorAgent(BaseAgent):
    name = "DimensionNavigator"
    description = (
        "Navigates OLAP hierarchies. Supports drill-down (go deeper) and "
        "roll-up (aggregate higher) across Time, Geography, and Product dimensions."
    )

    def run(self, agent_input: AgentInput) -> AgentOutput:
        op = agent_input.operation.lower()
        params = agent_input.parameters

        if op in ("drill_down", "drill-down"):
            return self._drill_down(params, agent_input.context)
        elif op in ("roll_up", "roll-up"):
            return self._roll_up(params, agent_input.context)
        elif op == "group":
            return self._group_by(params, agent_input.context)
        else:
            return AgentOutput(
                agent=self.name, operation=op, sql="",
                error=f"Unknown operation '{op}'. Supported: drill_down, roll_up, group"
            )

    def _drill_down(self, params: dict, context: str) -> AgentOutput:
        """
        Drill into a specific dimension level.
        params: {
            "hierarchy": "time" | "geography" | "product",
            "from_level": "year" | "region" | "category"   (optional),
            "to_level": "quarter" | "country" | "subcategory",
            "filters": {...}
        }
        """
        hierarchy = params.get("hierarchy", "time")
        h = HIERARCHIES.get(hierarchy, HIERARCHIES["time"])
        to_level = params.get("to_level", h["levels"][-1])
        filters = params.get("filters", {})

        # Determine which columns to include up to and including to_level
        try:
            to_idx = h["levels"].index(to_level)
        except ValueError:
            to_idx = len(h["levels"]) - 1

        group_cols = h["columns"][: to_idx + 1]
        where, sql_params = _build_filter_clause(filters)
        sql, sql_params = _base_query(group_cols, where, sql_params)

        df = self.execute_sql(sql, sql_params)
        label = " → ".join(h["labels"][: to_idx + 1])
        summary = (
            f"Drill-down on {hierarchy} hierarchy to '{to_level}' level "
            f"({label}). {len(df)} rows returned."
        )
        if not df.empty:
            top = df.iloc[0]
            summary += (
                f" Top performer: {top.iloc[0]} "
                f"with ${top['total_revenue']:,.2f} revenue."
            )

        return AgentOutput(
            agent=self.name, operation="drill_down",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"hierarchy": hierarchy, "level": to_level}
        )

    def _roll_up(self, params: dict, context: str) -> AgentOutput:
        """
        Aggregate up to a higher level.
        params: {
            "hierarchy": "time" | "geography" | "product",
            "to_level": "year" | "region" | "category",
            "filters": {...}
        }
        """
        hierarchy = params.get("hierarchy", "time")
        h = HIERARCHIES.get(hierarchy, HIERARCHIES["time"])
        to_level = params.get("to_level", h["levels"][0])
        filters = params.get("filters", {})

        try:
            to_idx = h["levels"].index(to_level)
        except ValueError:
            to_idx = 0

        group_cols = h["columns"][: to_idx + 1]
        where, sql_params = _build_filter_clause(filters)
        sql, sql_params = _base_query(group_cols, where, sql_params)

        df = self.execute_sql(sql, sql_params)
        label = " → ".join(h["labels"][: to_idx + 1])
        summary = (
            f"Roll-up on {hierarchy} to '{to_level}' level ({label}). "
            f"{len(df)} rows. Total revenue: ${df['total_revenue'].sum():,.2f}"
        )

        return AgentOutput(
            agent=self.name, operation="roll_up",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"hierarchy": hierarchy, "level": to_level}
        )

    def _group_by(self, params: dict, context: str) -> AgentOutput:
        """Generic group-by on any combination of dimensions."""
        dimensions = params.get("dimensions", ["region"])
        filters = params.get("filters", {})

        col_map = {
            "year": "d.year", "quarter": "d.quarter", "month": "d.month_name",
            "region": "g.region", "country": "g.country",
            "category": "p.category", "subcategory": "p.subcategory",
            "customer_segment": "c.customer_segment",
        }
        group_cols = [col_map.get(d, f"d.{d}") for d in dimensions]
        where, sql_params = _build_filter_clause(filters)
        sql, sql_params = _base_query(group_cols, where, sql_params)

        df = self.execute_sql(sql, sql_params)
        dims_str = ", ".join(dimensions)
        summary = f"Grouped by [{dims_str}]. {len(df)} groups. Total revenue: ${df['total_revenue'].sum():,.2f}"

        return AgentOutput(
            agent=self.name, operation="group",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"dimensions": dimensions}
        )
