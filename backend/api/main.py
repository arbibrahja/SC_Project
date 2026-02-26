"""
FastAPI Backend — OLAP Multi-Agent BI Platform
Exposes REST endpoints consumed by the React frontend.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from typing import Optional
import os
from pathlib import Path
import sys

# Project root = two levels up from backend/api/main.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.core.database import ensure_db, query_df
from backend.core.orchestrator import OLAPOrchestrator

# ---------------------------------------------------------------------------
app = FastAPI(
    title="OLAP Multi-Agent BI Platform",
    description="Business Intelligence API powered by specialized AI agents",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths — always relative to project root, works on any machine
DB_PATH  = str(PROJECT_ROOT / "olap.db")
CSV_PATH = str(PROJECT_ROOT / "data" / "global_retail_sales.csv")

ensure_db(DB_PATH, CSV_PATH)

orchestrator = OLAPOrchestrator(
    db_path=DB_PATH,
    api_key=os.environ.get("ANTHROPIC_API_KEY", "")
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    query: str
    reset_context: bool = False

class AgentRequest(BaseModel):
    agent: str
    operation: str
    parameters: dict = {}

# ---------------------------------------------------------------------------
# Frontend serving — open http://localhost:8000 and the app loads directly
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def serve_frontend():
    index_path = PROJECT_ROOT / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h2>index.html not found</h2>")
# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    try:
        df = query_df("SELECT COUNT(*) AS cnt FROM fact_sales", db_path=DB_PATH)
        return {"status": "healthy", "records": int(df.iloc[0]["cnt"])}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/chat")
def chat(req: ChatRequest):
    if req.reset_context:
        orchestrator.reset_context()
    try:
        result = orchestrator.process(req.query)
        return result.to_dict()
    except Exception as e:
        raise HTTPException(500, f"Orchestrator error: {e}")


@app.post("/api/agent")
def run_agent(req: AgentRequest):
    from backend.agents.base import AgentInput
    from backend.agents.dimension_navigator import DimensionNavigatorAgent
    from backend.agents.cube_operations import CubeOperationsAgent
    from backend.agents.kpi_calculator import KPICalculatorAgent
    from backend.agents.report_generator import ReportGeneratorAgent
    from backend.agents.visualization import VisualizationAgent
    from backend.agents.anomaly_detection import AnomalyDetectionAgent

    agents = {
        "DimensionNavigator": DimensionNavigatorAgent(DB_PATH),
        "CubeOperations":     CubeOperationsAgent(DB_PATH),
        "KPICalculator":      KPICalculatorAgent(DB_PATH),
        "ReportGenerator":    ReportGeneratorAgent(DB_PATH),
        "Visualization":      VisualizationAgent(DB_PATH),
        "AnomalyDetection":   AnomalyDetectionAgent(DB_PATH),
    }

    agent = agents.get(req.agent)
    if not agent:
        raise HTTPException(400, f"Unknown agent: {req.agent}. Options: {list(agents.keys())}")

    try:
        output = agent.run(AgentInput(operation=req.operation, parameters=req.parameters))
        return output.to_dict()
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/schema")
def get_schema():
    return {
        "dimensions": {
            "time":      {"hierarchy": ["year", "quarter", "month"],
                          "values": {"year": [2022, 2023, 2024], "quarter": ["Q1","Q2","Q3","Q4"]}},
            "geography": {"hierarchy": ["region", "country"],
                          "values": {"region": ["North America","Europe","Asia Pacific","Latin America"]}},
            "product":   {"hierarchy": ["category", "subcategory"],
                          "values": {"category": ["Electronics","Furniture","Office Supplies","Clothing"]}},
            "customer":  {"hierarchy": ["customer_segment"],
                          "values": {"customer_segment": ["Consumer","Corporate","Home Office"]}},
        },
        "measures": ["quantity", "unit_price", "revenue", "cost", "profit", "profit_margin"],
        "fact_table": "fact_sales",
    }


@app.get("/api/quick-stats")
def quick_stats():
    sql = """
        SELECT COUNT(*)                      AS total_transactions,
               ROUND(SUM(f.revenue),2)       AS total_revenue,
               ROUND(SUM(f.profit),2)        AS total_profit,
               ROUND(AVG(f.profit_margin),2) AS avg_margin,
               ROUND(AVG(f.revenue),2)       AS avg_order_value
        FROM fact_sales f
    """
    df = query_df(sql, db_path=DB_PATH)
    return df.to_dict("records")[0]


@app.get("/api/revenue-by-region")
def revenue_by_region():
    sql = """
        SELECT g.region,
               ROUND(SUM(f.revenue),2)       AS total_revenue,
               ROUND(SUM(f.profit),2)        AS total_profit,
               ROUND(AVG(f.profit_margin),2) AS avg_margin
        FROM fact_sales f
        JOIN dim_geography g ON f.geo_key = g.geo_key
        GROUP BY g.region ORDER BY total_revenue DESC
    """
    df = query_df(sql, db_path=DB_PATH)
    return df.to_dict("records")


@app.get("/api/revenue-by-year")
def revenue_by_year():
    sql = """
        SELECT d.year,
               ROUND(SUM(f.revenue),2) AS total_revenue,
               ROUND(SUM(f.profit),2)  AS total_profit
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year ORDER BY d.year
    """
    df = query_df(sql, db_path=DB_PATH)
    return df.to_dict("records")


@app.get("/api/revenue-by-category")
def revenue_by_category():
    sql = """
        SELECT p.category,
               ROUND(SUM(f.revenue),2)       AS total_revenue,
               ROUND(AVG(f.profit_margin),2) AS avg_margin
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category ORDER BY total_revenue DESC
    """
    df = query_df(sql, db_path=DB_PATH)
    return df.to_dict("records")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.api.main:app", host="0.0.0.0", port=8000, reload=True)
