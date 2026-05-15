"""
CockroachDB REST API Proxy for Atlassian Forge Apps
=====================================================
Lightweight FastAPI proxy exposing 10 financial databases as JSON endpoints.

Endpoints:
  GET  /health                     — health check
  GET  /api/databases              — list all databases + tables + row counts
  GET  /api/{database}/tables      — list tables in a specific database
  GET  /api/{database}/{table}     — rows from table (paginated)
  GET  /api/{database}/{table}/{id} — single row by primary key
  POST /api/{database}/{table}     — insert a row
  GET  /api/market-radar           — aggregated market data (Forge)
  GET  /api/finance-cockpit        — aggregated finance data (Forge)
  GET  /api/decision-brief/{id}    — aggregated decision brief (Forge)
  GET  /api/battery-erp-dashboard  — aggregated battery ERP data
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import dotenv
dotenv.load_dotenv()

from fastapi import FastAPI, Request, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("db-proxy")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_KEY = os.getenv("API_KEY", "")
COCKROACH_HOST = os.getenv(
    "COCKROACH_HOST",
    "vortex-giraffe-15678.jxf.gcp-us-east1.cockroachlabs.cloud",
)
COCKROACH_PORT = os.getenv("COCKROACH_PORT", "26257")
COCKROACH_USER = os.getenv("COCKROACH_USER", "cubiczan")
COCKROACH_PASSWORD = os.getenv("COCKROACH_PASSWORD", "oY-hPkgXtZjc6kGqY67Gyg")
COCKROACH_SSL = os.getenv("COCKROACH_SSL", "require")

DATABASES = [
    "closed_loop_finance",
    "sec_earnings_workbench",
    "hedge_fund_13f_radar",
    "market_sentiment_fedgpt",
    "autonomous_business_os",
    "multi_agent_cfo_os",
    "stratifi_core",
    "battery_erp",
    "working_capital_optimizer",
    "cash_flow_optimizer",
]

def _base_url() -> str:
    return f"postgresql+psycopg2://{COCKROACH_USER}:{COCKROACH_PASSWORD}@{COCKROACH_HOST}:{COCKROACH_PORT}"

def _db_url(database: str) -> str:
    # CockroachDB v25+ requires sslmode=verify-full for cloud connections
    ssl = COCKROACH_SSL if COCKROACH_SSL in ("verify-full", "verify-ca", "disable") else "require"
    return f"{_base_url()}/{database}?sslmode={ssl}"

# ---------------------------------------------------------------------------
# Connection pool — one engine per database (lazily created)
# ---------------------------------------------------------------------------
_engines: dict[str, Engine] = {}

def get_engine(database: str) -> Engine:
    """Return a cached SQLAlchemy engine for the given database."""
    if database not in _engines:
        url = _db_url(database)
        _engines[database] = create_engine(
            url,
            poolclass=QueuePool,
            pool_pre_ping=True,
            pool_size=3,
            max_overflow=5,
            pool_recycle=300,
            connect_args={"connect_timeout": 10},
        )
        logger.info("Created engine for database: %s", database)
    return _engines[database]

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="CockroachDB REST Proxy",
    description="REST API proxy for Atlassian Forge apps to access CockroachDB financial databases.",
    version="1.0.0",
)

# CORS — allow Forge origins
FORGE_ORIGINS = [
    "https://*.atlassian.net",
    "https://*.atlassian.com",
    "http://localhost:*",
    "http://127.0.0.1:*",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=FORGE_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# API Key auth
# ---------------------------------------------------------------------------
async def verify_api_key(request: Request):
    """If API_KEY env var is set, require matching X-API-Key header."""
    if API_KEY:
        key = request.headers.get("X-API-Key")
        if not key or key != API_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return True

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_query(database: str, sql: str, params: dict | None = None) -> Any:
    """Execute a read query and return results as list of dicts."""
    engine = get_engine(database)
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        if result.returns_rows:
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]
        return []


def _run_write(database: str, sql: str, params: dict | None = None) -> Any:
    """Execute a write query within a transaction and commit."""
    engine = get_engine(database)
    with engine.begin() as conn:
        result = conn.execute(text(sql), params or {})
        return result


def _detect_primary_key(database: str, table: str) -> list[str]:
    """Auto-detect primary key column(s) for a table."""
    engine = get_engine(database)
    insp = inspect(engine)
    try:
        pk = insp.get_pk_constraint(table, schema="public")
        if pk and pk.get("constrained_columns"):
            return pk["constrained_columns"]
    except Exception:
        pass
    # Fallback: look for common PK column names
    for candidate in ["id", "ID", "uuid", "UUID", "pk", "rowid"]:
        col_sql = text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c LIMIT 1"
        )
        engine = get_engine(database)
        with engine.connect() as conn:
            row = conn.execute(col_sql, {"t": table, "c": candidate}).fetchone()
            if row:
                return [candidate]
    return []


def _serialize_value(val: Any) -> Any:
    """JSON-serialize database values."""
    if isinstance(val, (datetime,)):
        return val.isoformat()
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace")
    return val


def _rows_to_json(rows: list[dict]) -> list[dict]:
    return [{k: _serialize_value(v) for k, v in row.items()} for row in rows]


# ---------------------------------------------------------------------------
# Mock data for aggregated endpoints
# ---------------------------------------------------------------------------

MOCK_MARKET_RADAR = {
    "lastUpdated": "2026-05-15T00:00:00Z",
    "sentiment": {
        "composite": 0.62,
        "label": "CAUTIOUSLY OPTIMISTIC",
        "indicators": [
            {"name": "VIX", "value": 16.4, "signal": "neutral", "direction": "flat"},
            {"name": "Credit Spreads (IG)", "value": 98, "signal": "bullish", "direction": "tightening"},
            {"name": "10Y-2Y Spread", "value": -18, "signal": "bearish", "direction": "flat"},
            {"name": "DXY", "value": 104.2, "signal": "neutral", "direction": "weakening"},
            {"name": "Put/Call Ratio", "value": 0.85, "signal": "bullish", "direction": "declining"},
        ],
    },
    "fedPolicy": {
        "currentRate": "5.25-5.50",
        "nextMeeting": "2026-06-18",
        "impliedCut": 0.25,
        "stance": "HAWKISH HOLD",
    },
    "sectorRotation": [
        {"sector": "Technology", "flow": 4200000000, "direction": "inflow", "weight": 29.1},
        {"sector": "Healthcare", "flow": 1800000000, "direction": "inflow", "weight": 13.4},
        {"sector": "Financials", "flow": 1100000000, "direction": "inflow", "weight": 12.8},
    ],
    "alerts": [
        {"type": "warning", "message": "Yield curve inversion deepening", "since": "2026-05-10"},
        {"type": "info", "message": "Fed funds futures imply 78% probability of July cut", "since": "2026-05-14"},
    ],
}

MOCK_FINANCE_COCKPIT = {
    "budget": {
        "total": 2500000,
        "spent": 1420000,
        "remaining": 1080000,
        "period": "Q2 2026",
    },
    "burnRate": {
        "monthly": 178000,
        "weekly": 44500,
        "trend": "stable",
        "runwayMonths": 6,
    },
    "cashForecast": {
        "currentBalance": 3200000,
        "minProjected": 1850000,
        "minWeek": 8,
        "endPosition": 2750000,
        "riskWeeks": [6, 7, 8],
        "hasCriticalRisk": False,
        "hasWorkingCapitalRisk": True,
    },
    "workingCapital": {
        "dso": 42,
        "dpo": 58,
        "dio": 31,
        "ccc": 15,
        "status": "healthy",
        "score": 78,
        "recommendations": [
            {"action": "Accelerate AR collection on invoices >60 days", "savings": 45000},
            {"action": "Extend DPO with top 5 vendors by 15 days", "savings": 32000},
        ],
    },
}

MOCK_DECISION_BRIEF = {
    "id": "placeholder",
    "title": "Q2 Capex Allocation Review",
    "status": "in_review",
    "priority": "high",
    "created": "2026-05-10T14:30:00Z",
    "brief": {
        "context": "Review of proposed capital expenditure allocation for Q2 2026 across three major initiatives.",
        "options": [
            {"label": "Option A", "description": "Prioritize infrastructure scaling (60% infra, 25% product, 15% reserve)"},
            {"label": "Option B", "description": "Balanced growth approach (40% infra, 40% product, 20% reserve)"},
            {"label": "Option C", "description": "Product-first strategy (25% infra, 55% product, 20% reserve)"},
        ],
        "recommendation": "Option B recommended based on current market conditions and cash flow projections.",
    },
    "financialImpact": {
        "totalBudget": 500000,
        "roi12m": 0.34,
        "paybackMonths": 8,
        "npv": 125000,
    },
    "evidence": [
        {"source": "market_sentiment_fedgpt", "summary": "Market conditions favor balanced growth"},
        {"source": "closed_loop_finance", "summary": "Cash runway supports moderate investment"},
    ],
    "approvalChain": ["CFO", "Board", "Audit Committee"],
}

MOCK_BATTERY_ERP_DASHBOARD = {
    "lastUpdated": "2026-05-15T00:00:00Z",
    "inventory": {
        "totalUnits": 12450,
        "incoming": 3200,
        "outgoing": 2870,
        "lowStockAlerts": 3,
    },
    "quality": {
        "passRate": 97.3,
        "rejectRate": 1.2,
        "pendingInspection": 180,
        "avgCycleTime": 4.2,
    },
    "supplyChain": {
        "activeOrders": 24,
        "onTimeDelivery": 91.5,
        "supplierIssues": 2,
        "leadTimeDays": 14,
    },
    "production": {
        "dailyOutput": 890,
        "targetOutput": 950,
        "utilization": 93.7,
        "downtimeHours": 1.8,
    },
    "hazmat": {
        "activeManifests": 7,
        "compliant": True,
        "nextAudit": "2026-06-01",
    },
}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health_check():
    """Health check — verifies database connectivity."""
    results = {}
    for db in DATABASES:
        try:
            rows = _run_query(db, "SELECT 1 AS ok")
            results[db] = "ok" if rows else "empty"
        except Exception as e:
            results[db] = f"error: {str(e)[:80]}"
    all_ok = all(v == "ok" for v in results.values())
    return JSONResponse(
        status_code=200 if all_ok else 503,
        content={"status": "healthy" if all_ok else "degraded", "databases": results},
    )


@app.get("/api/databases", dependencies=[Depends(verify_api_key)])
async def list_databases():
    """List all 10 databases with their tables and row counts."""
    output = []
    for db in DATABASES:
        try:
            tables = _run_query(
                db,
                """
                SELECT table_name,
                       (SELECT count(*) FROM information_schema.columns c2
                        WHERE c2.table_schema = 'public' AND c2.table_name = c.table_name) AS column_count
                FROM information_schema.tables c
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
            )
            table_info = []
            for t in tables:
                tname = t["table_name"]
                try:
                    count_rows = _run_query(db, f'SELECT count(*) AS cnt FROM "{tname}"')
                    row_count = count_rows[0]["cnt"] if count_rows else 0
                except Exception:
                    row_count = -1
                table_info.append({
                    "name": tname,
                    "columns": t["column_count"],
                    "rows": row_count,
                })
            output.append({"database": db, "tables": table_info, "totalTables": len(table_info)})
        except Exception as e:
            output.append({"database": db, "error": str(e)[:200], "tables": [], "totalTables": 0})
    return {"databases": output, "total": len(output)}


@app.get("/api/{database}/tables", dependencies=[Depends(verify_api_key)])
async def list_tables(database: str):
    """List all tables in a specific database with column details."""
    if database not in DATABASES:
        raise HTTPException(status_code=404, detail=f"Unknown database: {database}")
    try:
        tables = _run_query(
            database,
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
        )
        output = []
        for t in tables:
            tname = t["table_name"]
            # Get columns
            cols = _run_query(
                database,
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :t
                ORDER BY ordinal_position
                """,
                {"t": tname},
            )
            # Get row count
            try:
                count_rows = _run_query(database, f'SELECT count(*) AS cnt FROM "{tname}"')
                row_count = count_rows[0]["cnt"] if count_rows else 0
            except Exception:
                row_count = -1
            output.append({
                "name": tname,
                "columns": cols,
                "rows": row_count,
            })
        return {"database": database, "tables": output}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/{database}/{table}", dependencies=[Depends(verify_api_key)])
async def get_table_rows(
    database: str,
    table: str,
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """Return rows from a table with pagination."""
    if database not in DATABASES:
        raise HTTPException(status_code=404, detail=f"Unknown database: {database}")
    # Sanitize table name
    if not table.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid table name")
    try:
        # Verify table exists
        check = _run_query(
            database,
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :t LIMIT 1
            """,
            {"t": table},
        )
        if not check:
            raise HTTPException(status_code=404, detail=f"Table '{table}' not found in '{database}'")

        rows = _run_query(
            database,
            f'SELECT * FROM "{table}" ORDER BY rowid LIMIT :lim OFFSET :off',
            {"lim": limit, "off": offset},
        )
        count_rows = _run_query(database, f'SELECT count(*) AS cnt FROM "{table}"')
        total = count_rows[0]["cnt"] if count_rows else 0
        return {
            "database": database,
            "table": table,
            "rows": _rows_to_json(rows),
            "pagination": {"limit": limit, "offset": offset, "total": total},
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/{database}/{table}/{record_id}", dependencies=[Depends(verify_api_key)])
async def get_row_by_id(database: str, table: str, record_id: str):
    """Return a single row by primary key."""
    if database not in DATABASES:
        raise HTTPException(status_code=404, detail=f"Unknown database: {database}")
    if not table.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid table name")
    try:
        pk_cols = _detect_primary_key(database, table)
        if not pk_cols:
            raise HTTPException(status_code=400, detail=f"Cannot detect primary key for table '{table}'")
        # Build WHERE clause
        pk_col = pk_cols[0]
        where = f'"{pk_col}" = :pkval'
        rows = _run_query(
            database,
            f'SELECT * FROM "{table}" WHERE {where} LIMIT 1',
            {"pkval": record_id},
        )
        if not rows:
            raise HTTPException(status_code=404, detail=f"Row with {pk_col}={record_id} not found")
        return {"database": database, "table": table, "primaryKey": pk_col, "row": _rows_to_json(rows)[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class InsertRowRequest(BaseModel):
    data: dict[str, Any]


@app.post("/api/{database}/{table}", dependencies=[Depends(verify_api_key)])
async def insert_row(database: str, table: str, body: InsertRowRequest):
    """Insert a new row into a table."""
    if database not in DATABASES:
        raise HTTPException(status_code=404, detail=f"Unknown database: {database}")
    if not table.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid table name")
    if not body.data:
        raise HTTPException(status_code=400, detail="Request body must contain 'data' object")
    try:
        columns = list(body.data.keys())
        placeholders = [f":{c}" for c in columns]
        col_str = ", ".join(f'"{c}"' for c in columns)
        ph_str = ", ".join(placeholders)
        sql = f'INSERT INTO "{table}" ({col_str}) VALUES ({ph_str})'
        _run_write(database, sql, body.data)
        return {"status": "created", "database": database, "table": table, "columns": columns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Aggregated Forge Endpoints (with mock fallback)
# ---------------------------------------------------------------------------

@app.get("/api/market-radar", dependencies=[Depends(verify_api_key)])
async def get_market_radar():
    """Aggregated market data for Market Radar Forge app."""
    data = _fetch_market_radar_from_db()
    if data:
        return data
    return MOCK_MARKET_RADAR


def _fetch_market_radar_from_db() -> dict | None:
    """Try to build market-radar from live DB; return None on failure or empty."""
    try:
        # market_sentiment_fedgpt — sentiment indicators
        indicators = _run_query(
            "market_sentiment_fedgpt",
            "SELECT * FROM sentiment_indicators ORDER BY recorded_at DESC LIMIT 20",
        )
        fed_speeches = _run_query(
            "market_sentiment_fedgpt",
            "SELECT * FROM fed_speeches ORDER BY speech_date DESC LIMIT 5",
        )
        sentiment_reports = _run_query(
            "market_sentiment_fedgpt",
            "SELECT * FROM sentiment_reports ORDER BY created_at DESC LIMIT 3",
        )

        # hedge_fund_13f_radar — sector rotation
        holdings = _run_query(
            "hedge_fund_13f_radar",
            "SELECT * FROM holdings ORDER BY value_usd DESC LIMIT 50",
        )
        radar_reports = _run_query(
            "hedge_fund_13f_radar",
            "SELECT * FROM radar_reports ORDER BY report_date DESC LIMIT 5",
        )

        if not indicators and not holdings:
            return None

        # Build response
        sentiment_indicators = []
        for ind in indicators:
            sentiment_indicators.append({
                "name": ind.get("indicator_name", ind.get("name", "Unknown")),
                "value": float(ind.get("value", 0)),
                "signal": ind.get("signal", "neutral"),
                "direction": ind.get("direction", "flat"),
            })

        composite = 0.62
        if sentiment_reports:
            composite = float(sentiment_reports[0].get("composite_score", 0.62))

        label = "CAUTIOUSLY OPTIMISTIC"
        if composite > 0.7:
            label = "BULLISH"
        elif composite < 0.4:
            label = "RISK-OFF"
        elif composite < 0.55:
            label = "CAUTIOUS"

        fed_policy = {
            "currentRate": "5.25-5.50",
            "nextMeeting": "2026-06-18",
            "impliedCut": 0.25,
            "stance": "HAWKISH HOLD",
        }
        if fed_speeches:
            fs = fed_speeches[0]
            fed_policy["stance"] = fs.get("stance", fs.get("tone", "HAWKISH HOLD"))
            if fs.get("implied_action"):
                fed_policy["stance"] = str(fs["implied_action"]).upper()

        sector_rotation = []
        if holdings:
            sector_map: dict[str, dict] = {}
            for h in holdings:
                sector = h.get("sector", "Other")
                flow = float(h.get("value_usd", 0))
                if sector not in sector_map:
                    sector_map[sector] = {"sector": sector, "flow": 0, "count": 0}
                sector_map[sector]["flow"] += flow
                sector_map[sector]["count"] += 1
            total_flow = sum(s["flow"] for s in sector_map.values())
            for s in sorted(sector_map.values(), key=lambda x: x["flow"], reverse=True)[:5]:
                weight = round((s["flow"] / total_flow) * 100, 1) if total_flow else 0
                sector_rotation.append({
                    "sector": s["sector"],
                    "flow": s["flow"],
                    "direction": "inflow",
                    "weight": weight,
                })

        alerts = []
        for r in (radar_reports or []):
            if r.get("alert_type"):
                alerts.append({
                    "type": r["alert_type"],
                    "message": r.get("summary", r.get("description", "")),
                    "since": str(r.get("report_date", "")),
                })

        return {
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "sentiment": {
                "composite": composite,
                "label": label,
                "indicators": sentiment_indicators or MOCK_MARKET_RADAR["sentiment"]["indicators"],
            },
            "fedPolicy": fed_policy,
            "sectorRotation": sector_rotation or MOCK_MARKET_RADAR["sectorRotation"],
            "alerts": alerts or MOCK_MARKET_RADAR["alerts"],
        }
    except Exception as e:
        logger.warning("Failed to fetch market-radar from DB: %s", e)
        return None


@app.get("/api/finance-cockpit", dependencies=[Depends(verify_api_key)])
async def get_finance_cockpit():
    """Aggregated finance data for Finance Cockpit Forge app."""
    data = _fetch_finance_cockpit_from_db()
    if data:
        return data
    return MOCK_FINANCE_COCKPIT


def _fetch_finance_cockpit_from_db() -> dict | None:
    """Try to build finance-cockpit from live DB; return None on failure or empty."""
    try:
        # closed_loop_finance — budgets, decisions, evidence
        budgets = _run_query(
            "closed_loop_finance",
            "SELECT * FROM budgets ORDER BY period DESC LIMIT 5",
        )
        cash_flows = _run_query(
            "closed_loop_finance",
            "SELECT * FROM cash_flow_forecasts ORDER BY created_at DESC LIMIT 3",
        )
        evidence = _run_query(
            "closed_loop_finance",
            "SELECT * FROM evidence ORDER BY created_at DESC LIMIT 10",
        )

        # multi_agent_cfo_os — briefs, forecasts
        briefs = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM cfo_briefs ORDER BY created_at DESC LIMIT 5",
        )
        forecasts = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM forecasts ORDER BY created_at DESC LIMIT 5",
        )

        if not budgets and not briefs and not cash_flows:
            return None

        budget = {"total": 2500000, "spent": 1420000, "remaining": 1080000, "period": "Q2 2026"}
        if budgets:
            b = budgets[0]
            budget["total"] = float(b.get("total_budget", b.get("amount", 2500000)))
            budget["spent"] = float(b.get("actual_spend", b.get("spent", 1420000)))
            budget["remaining"] = budget["total"] - budget["spent"]
            budget["period"] = b.get("period", b.get("name", "Q2 2026"))

        burn_rate = {"monthly": 178000, "weekly": 44500, "trend": "stable", "runwayMonths": 6}
        if budget["total"] > 0 and budget["spent"] > 0:
            burn_rate["monthly"] = round(budget["spent"], -2)
            burn_rate["weekly"] = round(burn_rate["monthly"] / 4.33, -2)
            if budget["remaining"] > 0:
                burn_rate["runwayMonths"] = round(budget["remaining"] / burn_rate["monthly"], 1)

        cash_forecast = {
            "currentBalance": 3200000,
            "minProjected": 1850000,
            "minWeek": 8,
            "endPosition": 2750000,
            "riskWeeks": [6, 7, 8],
            "hasCriticalRisk": False,
            "hasWorkingCapitalRisk": True,
        }
        if cash_flows:
            cf = cash_flows[0]
            cash_forecast["currentBalance"] = float(cf.get("starting_balance", cf.get("current_balance", 3200000)))
            cash_forecast["endPosition"] = float(cf.get("ending_balance", cf.get("projected_balance", 2750000)))
            cash_forecast["minProjected"] = float(cf.get("min_balance", 1850000))

        if forecasts:
            f = forecasts[0]
            cash_forecast["currentBalance"] = float(f.get("starting_cash", cash_forecast["currentBalance"]))
            cash_forecast["endPosition"] = float(f.get("projected_cash", cash_forecast["endPosition"]))

        working_capital = {
            "dso": 42, "dpo": 58, "dio": 31, "ccc": 15,
            "status": "healthy", "score": 78,
            "recommendations": [
                {"action": "Accelerate AR collection on invoices >60 days", "savings": 45000},
                {"action": "Extend DPO with top 5 vendors by 15 days", "savings": 32000},
            ],
        }
        if evidence:
            for ev in evidence:
                cat = ev.get("category", "").lower()
                if "working" in cat or "capital" in cat:
                    working_capital["recommendations"].append({
                        "action": ev.get("description", ev.get("finding", "")),
                        "savings": ev.get("impact_score", 0),
                    })

        return {
            "budget": budget,
            "burnRate": burn_rate,
            "cashForecast": cash_forecast,
            "workingCapital": working_capital,
        }
    except Exception as e:
        logger.warning("Failed to fetch finance-cockpit from DB: %s", e)
        return None


@app.get("/api/decision-brief/{decision_id}", dependencies=[Depends(verify_api_key)])
async def get_decision_brief(decision_id: str):
    """Aggregated decision brief data."""
    data = _fetch_decision_brief_from_db(decision_id)
    if data:
        return data
    mock = dict(MOCK_DECISION_BRIEF)
    mock["id"] = decision_id
    return mock


def _fetch_decision_brief_from_db(decision_id: str) -> dict | None:
    """Try to build decision brief from live DB; return None on failure or empty."""
    try:
        # multi_agent_cfo_os
        briefs = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM cfo_briefs WHERE brief_id = :id OR id = :id LIMIT 1",
            {"id": decision_id},
        )
        artifacts = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM cfo_artifacts WHERE brief_id = :id LIMIT 10",
            {"id": decision_id},
        )
        audits = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM cfo_audit WHERE brief_id = :id LIMIT 5",
            {"id": decision_id},
        )
        forecasts = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM forecasts WHERE brief_id = :id LIMIT 3",
            {"id": decision_id},
        )
        decisions = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM decision_cases WHERE brief_id = :id LIMIT 5",
            {"id": decision_id},
        )
        rounds = _run_query(
            "multi_agent_cfo_os",
            "SELECT * FROM round_records WHERE brief_id = :id ORDER BY round_number LIMIT 10",
            {"id": decision_id},
        )

        # closed_loop_finance
        evidence = _run_query(
            "closed_loop_finance",
            "SELECT * FROM evidence WHERE decision_id = :id OR related_id = :id LIMIT 10",
            {"id": decision_id},
        )
        findings = _run_query(
            "closed_loop_finance",
            "SELECT * FROM findings WHERE decision_id = :id LIMIT 5",
            {"id": decision_id},
        )

        if not briefs and not decisions and not evidence:
            return None

        brief = briefs[0] if briefs else (decisions[0] if decisions else {})
        title = brief.get("title", brief.get("case_title", "Decision Brief"))
        status = brief.get("status", "draft")
        priority = brief.get("priority", "medium")
        created = brief.get("created_at", str(datetime.now(timezone.utc).isoformat()))

        return {
            "id": decision_id,
            "title": title,
            "status": status,
            "priority": priority,
            "created": _serialize_value(created) if not isinstance(created, str) else created,
            "brief": {
                "context": brief.get("context", brief.get("description", "")),
                "options": [
                    {"label": d.get("option_label", d.get("label", f"Option {i+1}")),
                     "description": d.get("description", "")}
                    for i, d in enumerate(decisions) if d.get("description")
                ] or [{"label": "Default", "description": brief.get("recommendation", "")}],
                "recommendation": brief.get("recommendation", ""),
            },
            "financialImpact": {
                "totalBudget": float(brief.get("budget_amount", 500000)),
                "roi12m": float(brief.get("roi_12m", 0.34)),
                "paybackMonths": int(brief.get("payback_months", 8)),
                "npv": float(brief.get("npv", 125000)),
            },
            "artifacts": _rows_to_json(artifacts),
            "evidence": _rows_to_json(evidence),
            "rounds": _rows_to_json(rounds),
            "forecasts": _rows_to_json(forecasts),
            "findings": _rows_to_json(findings),
            "audits": _rows_to_json(audits),
        }
    except Exception as e:
        logger.warning("Failed to fetch decision-brief from DB: %s", e)
        return None


@app.get("/api/battery-erp-dashboard", dependencies=[Depends(verify_api_key)])
async def get_battery_erp_dashboard():
    """Aggregated battery ERP dashboard data."""
    data = _fetch_battery_erp_from_db()
    if data:
        return data
    return MOCK_BATTERY_ERP_DASHBOARD


def _fetch_battery_erp_from_db() -> dict | None:
    """Try to build battery-erp-dashboard from live DB; return None on failure or empty."""
    try:
        # Try common battery_erp table patterns
        tables = _run_query(
            "battery_erp",
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """,
        )
        table_names = [t["table_name"] for t in tables] if tables else []

        if not table_names:
            return None

        # Try to get data from whatever tables exist
        all_data: dict[str, list] = {}
        for tname in table_names:
            try:
                rows = _run_query(
                    "battery_erp",
                    f'SELECT * FROM "{tname}" ORDER BY rowid LIMIT 100',
                )
                if rows:
                    all_data[tname] = _rows_to_json(rows)
            except Exception:
                pass

        if not all_data:
            return None

        dashboard: dict[str, Any] = {
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "tables": {},
            "summary": {},
        }

        # Categorize tables by name patterns
        inventory_count = 0
        quality_pass = 0.0
        quality_total = 0

        for tname, rows in all_data.items():
            dashboard["tables"][tname] = {"rows": len(rows)}
            tn_lower = tname.lower()

            if "inventory" in tn_lower or "stock" in tn_lower:
                inventory_count += len(rows)
            if "quality" in tn_lower or "inspection" in tn_lower:
                quality_total += len(rows)
                for r in rows:
                    if r.get("status", "").lower() in ("pass", "approved", "accepted"):
                        quality_pass += 1
            if "order" in tn_lower:
                dashboard["summary"]["activeOrders"] = len(rows)
            if "hazard" in tn_lower or "hazmat" in tn_lower or "waste" in tn_lower:
                dashboard["summary"]["activeManifests"] = len(rows)

        if inventory_count > 0:
            dashboard["summary"]["totalInventoryItems"] = inventory_count
        if quality_total > 0:
            dashboard["summary"]["qualityPassRate"] = round((quality_pass / quality_total) * 100, 1)

        return dashboard
    except Exception as e:
        logger.warning("Failed to fetch battery-erp-dashboard from DB: %s", e)
        return None


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception on %s %s: %s", request.method, request.url, exc)
    return JSONResponse(status_code=500, content={"error": "Internal server error", "detail": str(exc)})


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    logger.info("=" * 60)
    logger.info("CockroachDB REST Proxy starting up")
    logger.info("Databases configured: %d", len(DATABASES))
    logger.info("API Key auth: %s", "ENABLED" if API_KEY else "DISABLED")
    logger.info("=" * 60)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
