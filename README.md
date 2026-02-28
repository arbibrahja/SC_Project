# OLAP Intelligence Platform — Tier 3: Architect

A production-grade **multi-agent Business Intelligence platform** that lets business users explore 10,000 retail transactions through natural language, powered by 6 specialized AI agents.

---

## Architecture

```
FRONTEND (HTML/React)
        │
   API LAYER (FastAPI)
        │
 PLANNER / ORCHESTRATOR  ◄── Claude (claude-sonnet-4-6)
        │
   ┌────┴────────────────────────────────┐
   │         │           │               │
DimensionNav  CubeOps  KPICalc  ReportGen  [Viz] [Anomaly]
   │                                     │
   └───────────── DATA ACCESS ───────────┘
                     │
              SQLite Star Schema
          (dim_date · dim_geo · dim_product · dim_customer · fact_sales)
```

---

## Quick Start on how to run the project
## Use

```bash
python .\launch.py
```
## After that run the index.html file, if you run just the python file and add to the URL /docs you can find the APIs docs

## The first run will take a while to run

## The extra folder contains some HTML pages with extra information about the project

## Longer steps to open the project step by step

### 1. Clone / open the project

### 2. Install dependencies
```bash
pip install -r requirements.txt
```
### 3. Generate the dataset
```bash
cd data
python generate_dataset.py
cd ..
```

### 4. Initialize the database
```bash
python -m backend.core.database
```
This creates `olap.db` and loads 10,000 records into the star schema.

### 5. Set your API key
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```
Or create a `.env` file:
```
ANTHROPIC_API_KEY=sk-ant-...
```

### 6. Start the backend
```bash
uvicorn backend.api.main:app --reload --port 8000
```
If that does not work try this
``` bash
py -3.11 -m uvicorn backend.api.main:app --reload
```

### 7. Open the frontend
Open `index.html` in your browser (or serve it):
```bash
python -m http.server 3000 --directory frontend
```
Then visit `http://localhost:3000`.

---
## Project Structure

```
olap-platform/
├── data/
│   ├── generate_dataset.py        # Generates 10,000 retail transactions
│   └── global_retail_sales.csv    # Generated dataset
├── database/
│   └── schema.sql                 # Star schema DDL
├── backend/
│   ├── core/
│   │   ├── database.py            # SQLite star schema + ETL
│   │   └── orchestrator.py        # Planner / multi-agent coordinator
│   ├── agents/
│   │   ├── base.py                # AgentInput / AgentOutput / BaseAgent
│   │   ├── dimension_navigator.py # Agent 1: drill_down, roll_up, group
│   │   ├── cube_operations.py     # Agent 2: slice, dice, pivot, drill_through
│   │   ├── kpi_calculator.py      # Agent 3: yoy, mom, compare, top_n, margins
│   │   ├── report_generator.py    # Agent 4: executive_summary, trend_report
│   │   ├── visualization.py       # Optional: chart type selection + Plotly config
│   │   └── anomaly_detection.py   # Optional: Z-score + IQR anomaly detection
│   └── api/
│       └── main.py                # FastAPI REST API
├── frontend/
│   └── index.html                 # Single-file React BI dashboard
├── docs/
│   └── architecture.md            # System design document
├── requirements.txt
└── README.md
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check + record count |
| POST | `/api/chat` | Natural language → multi-agent analysis |
| POST | `/api/agent` | Direct agent invocation |
| GET | `/api/schema` | Star schema metadata |
| GET | `/api/quick-stats` | Headline KPIs |
| GET | `/api/revenue-by-region` | Regional breakdown |
| GET | `/api/revenue-by-year` | YoY revenue |
| GET | `/api/revenue-by-category` | Category breakdown |

### Example: Chat Request
```bash
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "Compare 2023 vs 2024 by region"}'
```

### Example: Direct Agent Call
```bash
curl -X POST http://localhost:8000/api/agent \
  -H "Content-Type: application/json" \
  -d '{
    "agent": "KPICalculator",
    "operation": "top_n",
    "parameters": {"n": 5, "dimension": "country", "measure": "profit"}
  }'
```

---

## The Six Agents

### Agent 1 — DimensionNavigator (Required)
Navigates OLAP hierarchies across Time (Year→Quarter→Month), Geography (Region→Country), and Product (Category→Subcategory).

**Operations:** `drill_down`, `roll_up`, `group`

### Agent 2 — CubeOperations (Required)
Implements the classic OLAP cube manipulations.

**Operations:** `slice`, `dice`, `pivot`, `drill_through`

### Agent 3 — KPICalculator (Required)
Computes business KPIs with statistical rigor.

**Operations:** `yoy_growth`, `mom_change`, `compare_periods`, `top_n`, `profit_margins`, `summary`

### Agent 4 — ReportGenerator (Required)
Formats agent outputs into polished reports with totals rows and executive narratives.

**Operations:** `executive_summary`, `trend_report`, `format_table`

### Agent 5 — Visualization (Optional, higher grade)
Selects optimal chart types and generates Plotly-compatible chart configs.

**Operations:** `visualize`

### Agent 6 — AnomalyDetection (Optional, higher grade)
Detects statistical outliers using Z-score and IQR methods.

**Operations:** `monthly_anomaly`, `product_anomaly`

---

## The Planner / Orchestrator

The `OLAPOrchestrator` in `backend/core/orchestrator.py` coordinates all agents:

1. **Query Understanding** — Calls Claude to parse natural language into a structured JSON execution plan
2. **Agent Selection** — Routes to the appropriate agent(s) based on intent
3. **Execution** — Runs each agent step in sequence, passing context forward
4. **Aggregation** — Assembles summaries into a final narrative
5. **Context Management** — Maintains conversation history for multi-turn sessions

**Fallback:** If the Anthropic API is unavailable, the orchestrator switches to a rule-based keyword planner so the system continues working.

---

## Dataset

| Attribute | Value |
|-----------|-------|
| Records | 10,000 transactions |
| Period | January 2022 – December 2024 |
| Regions | North America, Europe, Asia Pacific, Latin America |
| Categories | Electronics, Furniture, Office Supplies, Clothing |

### Dimensions
- **Time:** order_date, year, quarter, month, month_name
- **Geography:** region, country
- **Product:** category, subcategory
- **Customer:** customer_segment (Consumer, Corporate, Home Office)

### Measures
- quantity, unit_price, revenue, cost, profit, profit_margin

---


