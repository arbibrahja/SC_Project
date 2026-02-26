"""
Optional Agent 6: Anomaly Detection Agent
Identifies statistical anomalies in sales metrics using Z-score and IQR methods.
"""

from backend.agents.base import BaseAgent, AgentInput, AgentOutput
import pandas as pd


class AnomalyDetectionAgent(BaseAgent):
    name = "AnomalyDetection"
    description = (
        "Identifies unusual patterns in sales data using statistical methods "
        "(Z-score, IQR). Flags outlier time periods, regions, or products."
    )

    def run(self, agent_input: AgentInput) -> AgentOutput:
        op = agent_input.operation.lower()
        p = agent_input.parameters

        if op in ("time_anomaly", "monthly"):
            return self._monthly_anomaly(p, agent_input.context)
        elif op in ("product_anomaly", "product"):
            return self._product_anomaly(p, agent_input.context)
        else:
            return self._monthly_anomaly(p, agent_input.context)

    def _monthly_anomaly(self, params: dict, context: str) -> AgentOutput:
        """Detect months with anomalously high or low revenue."""
        sql = """
            SELECT d.year, d.month, d.month_name,
                   ROUND(SUM(f.revenue),2) AS monthly_revenue
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
        """
        df = self.execute_sql(sql)

        # Z-score
        mean = df["monthly_revenue"].mean()
        std = df["monthly_revenue"].std()
        df["z_score"] = ((df["monthly_revenue"] - mean) / std).round(3)
        df["anomaly"] = df["z_score"].abs() > 2.0
        df["anomaly_type"] = df.apply(
            lambda r: "High" if r["z_score"] > 2 else ("Low" if r["z_score"] < -2 else "Normal"),
            axis=1
        )

        anomalies = df[df["anomaly"]]
        summary = (
            f"Detected {len(anomalies)} monthly revenue anomalies "
            f"(Z-score threshold: ±2.0). "
            f"Mean: ${mean:,.2f}, Std: ${std:,.2f}."
        )
        if not anomalies.empty:
            worst = anomalies.iloc[0]
            summary += f" Most extreme: {worst['month_name']} {int(worst['year'])} (Z={worst['z_score']})"

        return AgentOutput(
            agent=self.name, operation="monthly_anomaly",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"threshold_z": 2.0, "anomaly_count": len(anomalies)}
        )

    def _product_anomaly(self, params: dict, context: str) -> AgentOutput:
        """Detect subcategories with anomalous profit margins."""
        sql = """
            SELECT p.category, p.subcategory,
                   ROUND(AVG(f.profit_margin),2) AS avg_margin,
                   ROUND(SUM(f.revenue),2) AS total_revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.category, p.subcategory
        """
        df = self.execute_sql(sql)

        # IQR method
        q1 = df["avg_margin"].quantile(0.25)
        q3 = df["avg_margin"].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        df["anomaly"] = (df["avg_margin"] < lower) | (df["avg_margin"] > upper)
        df["anomaly_type"] = df.apply(
            lambda r: "High margin" if r["avg_margin"] > upper
            else ("Low margin" if r["avg_margin"] < lower else "Normal"),
            axis=1
        )

        anomalies = df[df["anomaly"]]
        summary = (
            f"Product margin anomaly detection via IQR. "
            f"Normal range: {lower:.1f}%–{upper:.1f}%. "
            f"{len(anomalies)} subcategories flagged."
        )
        return AgentOutput(
            agent=self.name, operation="product_anomaly",
            sql=sql.strip(), data=df, summary=summary,
            metadata={"iqr_lower": round(lower, 2), "iqr_upper": round(upper, 2)}
        )
