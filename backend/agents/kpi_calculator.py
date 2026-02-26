"""
Agent 3: KPI Calculator
Computes business KPIs: YoY growth, MoM change, profit margins, Top-N rankings,
period-over-period comparisons.
"""

from backend.agents.base import BaseAgent, AgentInput, AgentOutput
import pandas as pd

BASE_FROM = """
    FROM fact_sales f
    JOIN dim_date      d ON f.date_key     = d.date_key
    JOIN dim_geography g ON f.geo_key      = g.geo_key
    JOIN dim_product   p ON f.product_key  = p.product_key
    JOIN dim_customer  c ON f.customer_key = c.customer_key
"""

COL_MAP = {
    "year": "d.year",
    "quarter": "d.quarter",
    "month": "d.month_name",
    "region": "g.region",
    "country": "g.country",
    "category": "p.category",
    "subcategory": "p.subcategory",
    "customer_segment": "c.customer_segment",
}


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
        else:
            conditions.append(f"{col} = ?")
            params.append(val)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


class KPICalculatorAgent(BaseAgent):
    name = "KPICalculator"
    description = (
        "Calculates business KPIs: year-over-year growth, month-over-month change, "
        "profit margins, period comparisons, and Top-N rankings."
    )

    def run(self, agent_input: AgentInput) -> AgentOutput:
        op = agent_input.operation.lower().replace("-", "_")
        p = agent_input.parameters

        dispatch = {
            "yoy": self._yoy_growth,
            "yoy_growth": self._yoy_growth,
            "mom": self._mom_change,
            "mom_change": self._mom_change,
            "compare": self._compare_periods,
            "compare_periods": self._compare_periods,
            "top_n": self._top_n,
            "ranking": self._top_n,
            "margins": self._profit_margins,
            "profit_margins": self._profit_margins,
            "summary": self._overall_summary,
        }
        fn = dispatch.get(op, self._overall_summary)
        return fn(p, agent_input.context)

    # ------------------------------------------------------------------
    def _yoy_growth(self, params: dict, context: str) -> AgentOutput:
        """
        Year-over-year revenue growth by a dimension.
        params: {"dimension": "region" | "category" | None, "filters": {}}
        """
        dimension = params.get("dimension")
        filters = params.get("filters", {})

        dim_col = COL_MAP.get(dimension, "g.region") if dimension else None

        if dim_col:
            select_extra = f"{dim_col} AS dim_label,"
            group_extra = f"{dim_col},"
        else:
            select_extra = ""
            group_extra = ""

        where, sql_params = _build_where(filters)

        sql = f"""
            SELECT {select_extra}
                   d.year,
                   ROUND(SUM(f.revenue), 2) AS total_revenue
            {BASE_FROM}
            {where}
            GROUP BY {group_extra} d.year
            ORDER BY {group_extra} d.year
        """
        raw = self.execute_sql(sql, sql_params)

        if raw.empty:
            return AgentOutput(agent=self.name, operation="yoy_growth", sql=sql.strip(),
                               data=raw, summary="No data returned.")

        # Calculate growth %
        records = []
        if "dim_label" in raw.columns:
            for label, grp in raw.groupby("dim_label"):
                grp = grp.sort_values("year").reset_index(drop=True)
                for i, row in grp.iterrows():
                    prev = grp.iloc[i - 1]["total_revenue"] if i > 0 else None
                    growth = ((row["total_revenue"] - prev) / prev * 100) if prev else None
                    records.append({
                        "dimension": label,
                        "year": row["year"],
                        "total_revenue": row["total_revenue"],
                        "yoy_growth_pct": round(growth, 2) if growth is not None else None,
                    })
        else:
            raw = raw.sort_values("year").reset_index(drop=True)
            for i, row in raw.iterrows():
                prev = raw.iloc[i - 1]["total_revenue"] if i > 0 else None
                growth = ((row["total_revenue"] - prev) / prev * 100) if prev else None
                records.append({
                    "year": row["year"],
                    "total_revenue": row["total_revenue"],
                    "yoy_growth_pct": round(growth, 2) if growth is not None else None,
                })

        df = pd.DataFrame(records)
        last_growth = df["yoy_growth_pct"].dropna().iloc[-1] if not df["yoy_growth_pct"].dropna().empty else None
        summary = (
            f"Year-over-year growth"
            + (f" by {dimension}" if dimension else "")
            + f". Most recent YoY: {last_growth:+.2f}%" if last_growth is not None else "."
        )

        return AgentOutput(
            agent=self.name, operation="yoy_growth",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"dimension": dimension}
        )

    # ------------------------------------------------------------------
    def _mom_change(self, params: dict, context: str) -> AgentOutput:
        """Month-over-month revenue change for a given year."""
        year = params.get("year")
        filters = {"year": year} if year else {}

        where, sql_params = _build_where(filters)
        sql = f"""
            SELECT d.year, d.month, d.month_name,
                   ROUND(SUM(f.revenue), 2) AS total_revenue
            {BASE_FROM}
            {where}
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
        """
        raw = self.execute_sql(sql, sql_params)
        if raw.empty:
            return AgentOutput(agent=self.name, operation="mom_change",
                               sql=sql.strip(), data=raw, summary="No data.")

        raw["prev_revenue"] = raw["total_revenue"].shift(1)
        raw["mom_change_pct"] = ((raw["total_revenue"] - raw["prev_revenue"]) /
                                  raw["prev_revenue"] * 100).round(2)
        raw = raw.drop(columns=["prev_revenue"])

        summary = f"Month-over-month change ({year or 'all years'}). {len(raw)} months."
        return AgentOutput(
            agent=self.name, operation="mom_change",
            sql=sql.strip(), data=raw, summary=summary
        )

    # ------------------------------------------------------------------
    def _compare_periods(self, params: dict, context: str) -> AgentOutput:
        """
        Compare two time periods.
        params: {
            "period_a": {"year": 2023} | {"year": 2024, "quarter": "Q1"},
            "period_b": {"year": 2024},
            "group_by": "region" | "category" | None
        }
        """
        period_a = params.get("period_a", {"year": 2023})
        period_b = params.get("period_b", {"year": 2024})
        group_by = params.get("group_by")

        def get_period_data(period_filter: dict, label: str) -> pd.DataFrame:
            where, sql_params = _build_where(period_filter)
            dim_col = COL_MAP.get(group_by) if group_by else None
            select_dim = f"{dim_col} AS dimension," if dim_col else ""
            group_dim = f"{dim_col}," if dim_col else ""
            sql = f"""
                SELECT {select_dim}
                       ROUND(SUM(f.revenue), 2) AS revenue,
                       ROUND(SUM(f.profit), 2)  AS profit,
                       SUM(f.quantity)          AS quantity
                {BASE_FROM}
                {where}
                GROUP BY {group_dim}1
            """
            df = self.execute_sql(sql, sql_params)
            df["period"] = label
            return df, sql

        label_a = "_".join(f"{k}{v}" for k, v in period_a.items())
        label_b = "_".join(f"{k}{v}" for k, v in period_b.items())

        df_a, sql_a = get_period_data(period_a, label_a)
        df_b, _ = get_period_data(period_b, label_b)

        # Merge for comparison
        if group_by and "dimension" in df_a.columns:
            merged = df_a.merge(df_b, on="dimension", suffixes=(f"_{label_a}", f"_{label_b}"))
            rev_a = f"revenue_{label_a}"
            rev_b = f"revenue_{label_b}"
            merged["revenue_change"] = (merged[rev_b] - merged[rev_a]).round(2)
            merged["revenue_change_pct"] = (
                (merged[rev_b] - merged[rev_a]) / merged[rev_a].replace(0, float("nan")) * 100
            ).round(2)
            df = merged
        else:
            row_a = df_a.iloc[0] if not df_a.empty else {}
            row_b = df_b.iloc[0] if not df_b.empty else {}
            rev_a = float(row_a.get("revenue", 0))
            rev_b = float(row_b.get("revenue", 0))
            change = rev_b - rev_a
            change_pct = (change / rev_a * 100) if rev_a else 0
            df = pd.DataFrame([{
                "period_a": label_a, "revenue_a": rev_a,
                "period_b": label_b, "revenue_b": rev_b,
                "change": round(change, 2), "change_pct": round(change_pct, 2),
            }])

        summary = (
            f"Comparison: {label_a} vs {label_b}"
            + (f" by {group_by}" if group_by else "")
            + f". {len(df)} rows."
        )
        return AgentOutput(
            agent=self.name, operation="compare_periods",
            sql=sql_a.strip(), data=df, summary=summary,
            metadata={"period_a": period_a, "period_b": period_b, "group_by": group_by}
        )

    # ------------------------------------------------------------------
    def _top_n(self, params: dict, context: str) -> AgentOutput:
        """
        Top-N performers by any dimension and measure.
        params: {
            "n": 5,
            "dimension": "country",
            "measure": "profit" | "revenue" | "profit_margin",
            "filters": {}
        }
        """
        n = params.get("n", 5)
        dimension = params.get("dimension", "country")
        measure = params.get("measure", "revenue")
        filters = params.get("filters", {})

        dim_col = COL_MAP.get(dimension, "g.country")
        agg = "SUM" if measure in ("revenue", "profit", "quantity", "cost") else "AVG"
        where, sql_params = _build_where(filters)

        sql = f"""
            SELECT {dim_col} AS dimension,
                   ROUND({agg}(f.{measure}), 2) AS metric,
                   ROUND(SUM(f.revenue), 2) AS total_revenue,
                   ROUND(SUM(f.profit), 2)  AS total_profit,
                   COUNT(*)                 AS transactions
            {BASE_FROM}
            {where}
            GROUP BY {dim_col}
            ORDER BY metric DESC
            LIMIT {n}
        """
        df = self.execute_sql(sql, sql_params)
        df.insert(0, "rank", range(1, len(df) + 1))
        summary = f"Top {n} {dimension}s by {measure}. #1: {df.iloc[0]['dimension']} ({df.iloc[0]['metric']:,.2f})" if not df.empty else "No data."

        return AgentOutput(
            agent=self.name, operation="top_n",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"n": n, "dimension": dimension, "measure": measure}
        )

    # ------------------------------------------------------------------
    def _profit_margins(self, params: dict, context: str) -> AgentOutput:
        """Profit margin analysis by dimension."""
        dimension = params.get("dimension", "category")
        filters = params.get("filters", {})
        dim_col = COL_MAP.get(dimension, "p.category")
        where, sql_params = _build_where(filters)

        sql = f"""
            SELECT {dim_col} AS dimension,
                   ROUND(SUM(f.revenue), 2)       AS total_revenue,
                   ROUND(SUM(f.profit), 2)        AS total_profit,
                   ROUND(AVG(f.profit_margin), 2) AS avg_margin_pct,
                   ROUND(SUM(f.profit)/SUM(f.revenue)*100, 2) AS blended_margin_pct
            {BASE_FROM}
            {where}
            GROUP BY {dim_col}
            ORDER BY avg_margin_pct DESC
        """
        df = self.execute_sql(sql, sql_params)
        summary = f"Profit margins by {dimension}. Best: {df.iloc[0]['dimension']} at {df.iloc[0]['avg_margin_pct']}%" if not df.empty else ""
        return AgentOutput(
            agent=self.name, operation="profit_margins",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"dimension": dimension}
        )

    # ------------------------------------------------------------------
    def _overall_summary(self, params: dict, context: str) -> AgentOutput:
        """High-level KPI dashboard summary."""
        sql = """
            SELECT
                COUNT(*)                                AS total_transactions,
                ROUND(SUM(f.revenue), 2)               AS total_revenue,
                ROUND(SUM(f.profit), 2)                AS total_profit,
                ROUND(AVG(f.profit_margin), 2)         AS avg_margin_pct,
                ROUND(AVG(f.revenue), 2)               AS avg_order_value,
                MIN(d.full_date)                       AS earliest_date,
                MAX(d.full_date)                       AS latest_date
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
        """
        df = self.execute_sql(sql)
        r = df.iloc[0]
        summary = (
            f"Overall: {int(r['total_transactions']):,} transactions, "
            f"${r['total_revenue']:,.2f} revenue, "
            f"${r['total_profit']:,.2f} profit, "
            f"{r['avg_margin_pct']}% avg margin."
        )
        return AgentOutput(
            agent=self.name, operation="summary",
            sql=sql.strip(), data=df, summary=summary
        )
