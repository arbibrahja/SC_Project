# Architecture Document — OLAP Intelligence Platform

## System Overview

The OLAP Intelligence Platform is a multi-tier AI system that enables business users to perform
enterprise-grade data analysis through natural language. It combines classical OLAP techniques
(star schemas, cube operations, hierarchical navigation) with modern AI orchestration.

---

## Architectural Layers

### Layer 1: Presentation (React Frontend)

**Technology:** Single-file HTML/React application (no build step required)

The frontend provides:
- **Executive Dashboard** — Headline KPIs, regional bar charts, category breakdown
- **AI Chat Interface** — Conversational analysis with result tables and SQL visibility
- **Agent Registry** — Live view of all 6 agents and their operations
- **Schema Viewer** — Star schema ER diagram with table columns

Key design decision: The frontend runs in both *online mode* (connected to FastAPI) and *demo mode*
(rule-based mock responses) so it functions without an API key.

---

### Layer 2: API (FastAPI)

**Technology:** FastAPI with CORS middleware

REST endpoints expose the orchestrator and individual agents. The API follows a clean contract:
- `POST /api/chat` → natural language → JSON result
- `POST /api/agent` → direct agent invocation for testing
- `GET /api/*` → pre-computed dashboard data

The API initializes the database on startup and shares a single orchestrator instance across
requests. For production deployment, this would be extended with connection pooling and
request queuing.

---

### Layer 3: Orchestration (Planner)

**Technology:** Python + Anthropic Claude API

The `OLAPOrchestrator` is the system's "brain". Its pipeline:

```
User Query
    ↓
Claude API Call (claude-sonnet-4-6)
    ↓
JSON Execution Plan: [{agent, operation, parameters}, ...]
    ↓
Sequential Agent Execution
    ↓
Output Assembly (narrative + data + SQL + metadata)
    ↓
Response
```

The orchestrator maintains a sliding window of conversation history (last 6 turns) enabling
multi-turn analysis like: "Now drill into the top performer by month."

**Fallback Planning:** If the Claude API is unavailable (no key, network issue), the orchestrator
switches to a deterministic rule-based planner that keyword-matches common OLAP patterns.

---

### Layer 4: Agents

Each agent is a focused module with a single responsibility:

| Agent | Responsibility | Key SQL Patterns |
|-------|---------------|-----------------|
| DimensionNavigator | Hierarchical navigation | GROUP BY with hierarchy levels |
| CubeOperations | Cube slicing and pivoting | WHERE + GROUP BY + PIVOT |
| KPICalculator | Business metric computation | LAG(), window functions, CASE |
| ReportGenerator | Result formatting + narrative | Multi-query aggregation |
| Visualization | Chart configuration | Data shape analysis |
| AnomalyDetection | Statistical outlier detection | Z-score, IQR computation |

All agents extend `BaseAgent` and implement the `run(AgentInput) → AgentOutput` interface,
making them individually testable and easily replaceable.

---

### Layer 5: Data (Star Schema)

**Technology:** SQLite (stdlib, zero dependencies) with ANSI-compatible SQL

The flat CSV is transformed into a normalized star schema via ETL:

```
                 dim_date (date_key PK)
                      │
dim_geography ── fact_sales ── dim_product
  (geo_key PK)   (sale_id PK)   (product_key PK)
                      │
                dim_customer (customer_key PK)
```

**Why SQLite?** Zero installation, works on any OS, included in Python stdlib. The SQL is ANSI-
compatible and will run unchanged on PostgreSQL or DuckDB for production workloads.

**View:** `v_sales_full` denormalizes the star schema for simple ad-hoc queries.

---

## Data Flow: End-to-End Example

**User query:** *"Compare 2023 vs 2024 by region, then drill into the top performer"*

1. Frontend sends `POST /api/chat` with `{query: "..."}` 
2. FastAPI passes to `OLAPOrchestrator.process()`
3. Orchestrator sends query + conversation history to Claude API
4. Claude returns JSON plan:
   ```json
   {
     "steps": [
       {"agent": "KPICalculator", "operation": "compare_periods",
        "parameters": {"period_a": {"year": 2023}, "period_b": {"year": 2024}, "group_by": "region"}},
       {"agent": "DimensionNavigator", "operation": "drill_down",
        "parameters": {"hierarchy": "geography", "to_level": "country",
                       "filters": {"region": "North America"}}},
       {"agent": "ReportGenerator", "operation": "executive_summary", "parameters": {}}
     ]
   }
   ```
5. Each agent executes SQL against the star schema and returns structured `AgentOutput`
6. Orchestrator assembles narrative from summaries
7. API returns JSON with data tables, SQL, narrative, and follow-up suggestions
8. Frontend renders results with collapsible SQL viewer and follow-up buttons

---

## Design Decisions

### Why not LangChain/DSPy?
The agents are implemented in pure Python for transparency and simplicity. The project guide
lists DSPy/LangChain as options, but raw API calls with structured prompts are more
maintainable for a learning project and avoid framework version complexity.

### Why SQLite over PostgreSQL?
SQLite requires zero setup and runs everywhere. All SQL is ANSI-compatible. Switching to
PostgreSQL requires only changing the connection string.

### Why a single-file React frontend?
No build step means the frontend works immediately — open the HTML file in any browser.
This matches the "deployed demo" requirement without requiring Node.js.

### Conversation Context
The orchestrator maintains a 6-turn sliding window. This balances contextual awareness
(enabling follow-up questions) with prompt efficiency (avoiding token bloat over long sessions).

---

## Production Considerations

For a real enterprise deployment, the following would be added:
- **Authentication:** JWT tokens on all API endpoints
- **Connection Pooling:** Replace single SQLite connection with pool (or PostgreSQL)
- **Caching:** Redis cache for common OLAP query patterns
- **Rate Limiting:** Per-user API call limits
- **Monitoring:** Prometheus metrics for agent latency and error rates
- **Horizontal Scaling:** Stateless agents behind a load balancer
- **Database:** Migrate to PostgreSQL or DuckDB for concurrent write support
