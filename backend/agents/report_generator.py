"""
Agent 4: Report Generator
Formats agent outputs into polished reports with totals rows,
conditional formatting hints, and executive narrative summaries.
"""

from backend.agents.base import BaseAgent, AgentInput, AgentOutput
import pandas as pd
from typing import Optional


class ReportGeneratorAgent(BaseAgent):
    name = "ReportGenerator"
    description = (
        "Formats OLAP results into polished reports: formatted tables with totals, "
        "ranking callouts, and executive summaries."
    )

    def run(self, agent_input: AgentInput) -> AgentOutput:
        op = agent_input.operation.lower().replace("-", "_")
        p = agent_input.parameters

        if op in ("format_table", "table"):
            return self._format_table(p, agent_input.context)
        elif op in ("executive_summary", "summary", "narrative"):
            return self._executive_summary(p, agent_input.context)
        elif op in ("trend_report", "trend"):
            return self._trend_report(p, agent_input.context)
        else:
            # Default: generate an executive summary from raw data if provided
            return self._executive_summary(p, agent_input.context)

    # ------------------------------------------------------------------
    def _format_table(self, params: dict, context: str) -> AgentOutput:
        """
        Take a list of records and produce a formatted DataFrame with:
        - Currency formatting on revenue/profit columns
        - A TOTAL row appended
        - Rank column if requested
        """
        data = params.get("data", [])
        columns = params.get("columns")
        add_total = params.get("add_total", True)
        add_rank = params.get("add_rank", False)
        currency_cols = params.get("currency_cols", ["total_revenue", "total_profit",
                                                      "revenue", "profit"])

        if not data:
            # Pull a default grouped report from the database
            sql = """
                SELECT g.region,
                       ROUND(SUM(f.revenue),2)       AS total_revenue,
                       ROUND(SUM(f.profit),2)        AS total_profit,
                       ROUND(AVG(f.profit_margin),2) AS avg_margin_pct,
                       SUM(f.quantity)               AS total_qty,
                       COUNT(*)                      AS transactions
                FROM fact_sales f
                JOIN dim_geography g ON f.geo_key = g.geo_key
                GROUP BY g.region
                ORDER BY total_revenue DESC
            """
            df = self.execute_sql(sql)
        else:
            df = pd.DataFrame(data, columns=columns)

        if add_rank and "rank" not in df.columns:
            df.insert(0, "rank", range(1, len(df) + 1))

        # Add totals row
        if add_total and not df.empty:
            totals = {"rank": "—"} if "rank" in df.columns else {}
            for col in df.columns:
                if col == "rank":
                    continue
                if pd.api.types.is_numeric_dtype(df[col]):
                    if "pct" in col.lower() or "margin" in col.lower():
                        totals[col] = round(df[col].mean(), 2)
                    else:
                        totals[col] = round(df[col].sum(), 2)
                else:
                    totals[col] = "TOTAL" if list(df.columns).index(col) == (1 if "rank" in df.columns else 0) else "—"
            df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)

        # Formatting hints (metadata — frontend can use these for conditional styling)
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        summary = f"Formatted table with {len(df)-1 if add_total else len(df)} rows + totals."

        sql_placeholder = "-- Data provided externally (formatted from prior agent output)"
        return AgentOutput(
            agent=self.name, operation="format_table",
            sql=sql_placeholder, data=df, summary=summary,
            metadata={
                "currency_cols": currency_cols,
                "numeric_cols": numeric_cols,
                "has_totals_row": add_total,
            }
        )

    # ------------------------------------------------------------------
    def _executive_summary(self, params: dict, context: str) -> AgentOutput:
        """
        Generate a natural-language executive summary from database.
        params: {
            "focus": "overall" | "regional" | "product" | "time",
            "year": 2024   (optional)
        }
        """
        focus = params.get("focus", "overall")
        year_filter = params.get("year")

        where = f"WHERE d.year = {year_filter}" if year_filter else ""
        year_label = str(year_filter) if year_filter else "all years"

        # Top-line metrics
        sql_topline = f"""
            SELECT ROUND(SUM(f.revenue),2)       AS total_revenue,
                   ROUND(SUM(f.profit),2)        AS total_profit,
                   ROUND(AVG(f.profit_margin),2) AS avg_margin,
                   COUNT(*)                      AS transactions,
                   ROUND(AVG(f.revenue),2)       AS avg_order
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            {where}
        """
        topline = self.execute_sql(sql_topline).iloc[0]

        # Best region
        sql_region = f"""
            SELECT g.region, ROUND(SUM(f.revenue),2) AS rev
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_geography g ON f.geo_key = g.geo_key
            {where}
            GROUP BY g.region ORDER BY rev DESC LIMIT 1
        """
        top_region = self.execute_sql(sql_region)

        # Best category
        sql_cat = f"""
            SELECT p.category, ROUND(SUM(f.revenue),2) AS rev,
                   ROUND(AVG(f.profit_margin),2) AS margin
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_product p ON f.product_key = p.product_key
            {where}
            GROUP BY p.category ORDER BY rev DESC LIMIT 1
        """
        top_cat = self.execute_sql(sql_cat)

        # YoY comparison (if no year filter)
        yoy_insight = ""
        if not year_filter:
            sql_yoy = """
                SELECT d.year, ROUND(SUM(f.revenue),2) AS rev
                FROM fact_sales f
                JOIN dim_date d ON f.date_key = d.date_key
                GROUP BY d.year ORDER BY d.year
            """
            yoy_df = self.execute_sql(sql_yoy)
            if len(yoy_df) >= 2:
                last = yoy_df.iloc[-1]
                prev = yoy_df.iloc[-2]
                growth = (last["rev"] - prev["rev"]) / prev["rev"] * 100
                yoy_insight = (
                    f" Revenue grew {growth:+.1f}% from "
                    f"{int(prev['year'])} (${prev['rev']:,.0f}) to "
                    f"{int(last['year'])} (${last['rev']:,.0f})."
                )

        # Build narrative
        narrative = f"""## Executive Summary — {year_label.title()}

**Revenue:** ${topline['total_revenue']:,.2f} across {int(topline['transactions']):,} transactions.
**Profit:** ${topline['total_profit']:,.2f} | **Avg Margin:** {topline['avg_margin']}% | **Avg Order Value:** ${topline['avg_order']:,.2f}
{yoy_insight}

**Top Region:** {top_region.iloc[0]['region'] if not top_region.empty else 'N/A'} (${top_region.iloc[0]['rev']:,.2f})
**Top Category:** {top_cat.iloc[0]['category'] if not top_cat.empty else 'N/A'} (${top_cat.iloc[0]['rev']:,.2f}, {top_cat.iloc[0]['margin']}% margin)
"""

        summary_df = pd.DataFrame([{
            "metric": k, "value": v
        } for k, v in {
            "Total Revenue": f"${topline['total_revenue']:,.2f}",
            "Total Profit": f"${topline['total_profit']:,.2f}",
            "Avg Margin": f"{topline['avg_margin']}%",
            "Total Transactions": f"{int(topline['transactions']):,}",
            "Avg Order Value": f"${topline['avg_order']:,.2f}",
            "Top Region": top_region.iloc[0]['region'] if not top_region.empty else "N/A",
            "Top Category": top_cat.iloc[0]['category'] if not top_cat.empty else "N/A",
        }.items()])

        return AgentOutput(
            agent=self.name, operation="executive_summary",
            sql=sql_topline.strip(), data=summary_df, summary=narrative,
            metadata={"focus": focus, "year": year_filter, "narrative": narrative}
        )

    # ------------------------------------------------------------------
    def _trend_report(self, params: dict, context: str) -> AgentOutput:
        """Monthly revenue trend report for a given year."""
        year = params.get("year", 2024)
        dimension = params.get("dimension")  # optional breakdown dimension

        dim_col = {
            "region": "g.region", "category": "p.category",
            "customer_segment": "c.customer_segment"
        }.get(dimension)

        if dim_col:
            select_extra = f"{dim_col} AS dimension,"
            group_extra = f"{dim_col},"
        else:
            select_extra = ""
            group_extra = ""

        sql = f"""
            SELECT {select_extra}
                   d.month, d.month_name,
                   ROUND(SUM(f.revenue),2)  AS total_revenue,
                   ROUND(SUM(f.profit),2)   AS total_profit,
                   COUNT(*)                 AS transactions
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_geography g ON f.geo_key = g.geo_key
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE d.year = {year}
            GROUP BY {group_extra} d.month, d.month_name
            ORDER BY {group_extra} d.month
        """
        df = self.execute_sql(sql)

        if not df.empty and "dimension" not in df.columns:
            peak_month = df.loc[df["total_revenue"].idxmax()]
            low_month = df.loc[df["total_revenue"].idxmin()]
            summary = (
                f"Monthly trend for {year}. "
                f"Peak: {peak_month['month_name']} (${peak_month['total_revenue']:,.2f}). "
                f"Slowest: {low_month['month_name']} (${low_month['total_revenue']:,.2f})."
            )
        else:
            summary = f"Monthly trend for {year}" + (f" by {dimension}" if dimension else "") + f". {len(df)} rows."

        return AgentOutput(
            agent=self.name, operation="trend_report",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"year": year, "dimension": dimension}
        )
