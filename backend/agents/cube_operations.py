"""
Agent 2: Cube Operations
Handles OLAP cube manipulations: Slice (single filter), Dice (multi-filter), Pivot.
"""

from backend.agents.base import BaseAgent, AgentInput, AgentOutput
import pandas as pd


COL_MAP = {
    "year": "d.year",
    "quarter": "d.quarter",
    "month": "d.month_name",
    "month_num": "d.month",
    "region": "g.region",
    "country": "g.country",
    "category": "p.category",
    "subcategory": "p.subcategory",
    "customer_segment": "c.customer_segment",
}

BASE_FROM = """
    FROM fact_sales f
    JOIN dim_date      d ON f.date_key     = d.date_key
    JOIN dim_geography g ON f.geo_key      = g.geo_key
    JOIN dim_product   p ON f.product_key  = p.product_key
    JOIN dim_customer  c ON f.customer_key = c.customer_key
"""


def _build_where(filters: dict) -> tuple[str, list]:
    conditions, params = [], []
    for key, val in filters.items():
        col = COL_MAP.get(key)
        if not col:
            continue
        if isinstance(val, list):
            placeholders = ",".join("?" * len(val))
            conditions.append(f"{col} IN ({placeholders})")
            params.extend(val)
        elif isinstance(val, dict) and "gte" in val:
            conditions.append(f"{col} >= ?")
            params.append(val["gte"])
        elif isinstance(val, dict) and "lte" in val:
            conditions.append(f"{col} <= ?")
            params.append(val["lte"])
        else:
            conditions.append(f"{col} = ?")
            params.append(val)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


class CubeOperationsAgent(BaseAgent):
    name = "CubeOperations"
    description = (
        "Performs OLAP cube manipulations: "
        "Slice (filter on one dimension), "
        "Dice (filter on multiple dimensions), "
        "Pivot (reshape result by a column dimension)."
    )

    def run(self, agent_input: AgentInput) -> AgentOutput:
        op = agent_input.operation.lower().replace("-", "_")
        p = agent_input.parameters

        if op == "slice":
            return self._slice(p, agent_input.context)
        elif op == "dice":
            return self._dice(p, agent_input.context)
        elif op == "pivot":
            return self._pivot(p, agent_input.context)
        elif op == "drill_through":
            return self._drill_through(p, agent_input.context)
        else:
            return AgentOutput(
                agent=self.name, operation=op, sql="",
                error=f"Unknown op '{op}'. Supported: slice, dice, pivot, drill_through"
            )

    # ------------------------------------------------------------------
    def _slice(self, params: dict, context: str) -> AgentOutput:
        """
        Slice: fix one dimension value, group by another.
        params: {
            "filter": {"year": 2024},          # single-dimension filter
            "group_by": ["region"],            # what to aggregate by
        }
        """
        filters = params.get("filter", {})
        group_dims = params.get("group_by", ["region"])
        group_cols = [COL_MAP.get(d, d) for d in group_dims]
        select_str = ", ".join(group_cols)
        where, sql_params = _build_where(filters)

        sql = f"""
            SELECT {select_str},
                   ROUND(SUM(f.revenue), 2)       AS total_revenue,
                   ROUND(SUM(f.profit), 2)        AS total_profit,
                   ROUND(AVG(f.profit_margin), 2) AS avg_margin,
                   SUM(f.quantity)                AS total_qty,
                   COUNT(*)                       AS transactions
            {BASE_FROM}
            {where}
            GROUP BY {select_str}
            ORDER BY total_revenue DESC
        """
        df = self.execute_sql(sql, sql_params)
        filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items())
        summary = (
            f"Slice on [{filter_desc}], grouped by {group_dims}. "
            f"{len(df)} results. Revenue total: ${df['total_revenue'].sum():,.2f}"
        )
        return AgentOutput(
            agent=self.name, operation="slice",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"filters": filters, "group_by": group_dims}
        )

    # ------------------------------------------------------------------
    def _dice(self, params: dict, context: str) -> AgentOutput:
        """
        Dice: filter on multiple dimensions and group by one or more.
        params: {
            "filters": {"year": 2024, "region": "Europe", "category": "Electronics"},
            "group_by": ["country"],
        }
        """
        filters = params.get("filters", {})
        group_dims = params.get("group_by", ["country"])
        group_cols = [COL_MAP.get(d, d) for d in group_dims]
        select_str = ", ".join(group_cols)
        where, sql_params = _build_where(filters)

        sql = f"""
            SELECT {select_str},
                   ROUND(SUM(f.revenue), 2)       AS total_revenue,
                   ROUND(SUM(f.profit), 2)        AS total_profit,
                   ROUND(AVG(f.profit_margin), 2) AS avg_margin,
                   SUM(f.quantity)                AS total_qty,
                   COUNT(*)                       AS transactions
            {BASE_FROM}
            {where}
            GROUP BY {select_str}
            ORDER BY total_revenue DESC
        """
        df = self.execute_sql(sql, sql_params)
        filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items())
        summary = (
            f"Dice with filters [{filter_desc}], grouped by {group_dims}. "
            f"{len(df)} results."
        )
        if not df.empty:
            top = df.iloc[0]
            summary += f" Leader: {top.iloc[0]} (${top['total_revenue']:,.2f})"

        return AgentOutput(
            agent=self.name, operation="dice",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"filters": filters, "group_by": group_dims}
        )

    # ------------------------------------------------------------------
    def _pivot(self, params: dict, context: str) -> AgentOutput:
        """
        Pivot: reshape data so column_dim values become column headers.
        params: {
            "row_dim": "region",
            "col_dim": "year",
            "measure": "revenue",
            "filters": {}
        }
        """
        row_dim = params.get("row_dim", "region")
        col_dim = params.get("col_dim", "year")
        measure = params.get("measure", "revenue")
        filters = params.get("filters", {})

        row_col = COL_MAP.get(row_dim, row_dim)
        col_col = COL_MAP.get(col_dim, col_dim)
        agg = "SUM" if measure in ("revenue", "profit", "quantity", "cost") else "AVG"
        where, sql_params = _build_where(filters)

        sql = f"""
            SELECT {row_col} AS row_label,
                   {col_col} AS col_label,
                   ROUND({agg}(f.{measure}), 2) AS metric
            {BASE_FROM}
            {where}
            GROUP BY {row_col}, {col_col}
            ORDER BY {row_col}, {col_col}
        """
        raw = self.execute_sql(sql, sql_params)

        # Build pivot table
        if not raw.empty:
            df = raw.pivot_table(
                index="row_label", columns="col_label", values="metric", aggfunc="sum"
            ).reset_index()
            df.columns.name = None
        else:
            df = raw

        summary = (
            f"Pivot: {row_dim} (rows) × {col_dim} (columns) measuring {measure}. "
            f"{len(df)} row-groups."
        )
        return AgentOutput(
            agent=self.name, operation="pivot",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"row_dim": row_dim, "col_dim": col_dim, "measure": measure}
        )

    # ------------------------------------------------------------------
    def _drill_through(self, params: dict, context: str) -> AgentOutput:
        """Return actual transaction records (not aggregated)."""
        filters = params.get("filters", {})
        limit = params.get("limit", 50)
        where, sql_params = _build_where(filters)

        sql = f"""
            SELECT f.order_id, d.full_date AS order_date,
                   g.region, g.country,
                   p.category, p.subcategory,
                   c.customer_segment,
                   f.quantity, f.unit_price,
                   ROUND(f.revenue, 2) AS revenue,
                   ROUND(f.profit, 2) AS profit,
                   ROUND(f.profit_margin, 2) AS profit_margin
            {BASE_FROM}
            {where}
            ORDER BY d.full_date DESC
            LIMIT {limit}
        """
        df = self.execute_sql(sql, sql_params)
        filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items())
        summary = f"Drill-through on [{filter_desc}]. Showing {len(df)} transactions."

        return AgentOutput(
            agent=self.name, operation="drill_through",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"filters": filters, "limit": limit}
        )
