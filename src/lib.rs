use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use native_tls::TlsConnector;
use resilient_call::{crdb_retry, with_timeout, ResilienceError, SqlError};
use subtle::ConstantTimeEq;
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio_postgres::{
    config::SslMode,
    types::{Json as PgJson, ToSql, Type},
    Client, NoTls, Row,
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

/// Deadline for establishing a database connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Deadline for an individual query/write to complete.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

pub const DATABASES: &[&str] = &[
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
];

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub api_key: Option<String>,
    pub cockroach_host: String,
    pub cockroach_port: u16,
    pub cockroach_user: String,
    pub cockroach_password: Option<String>,
    pub cockroach_ssl: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            port: env::var("PORT")
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(8080),
            api_key: non_empty_env("API_KEY"),
            cockroach_host: env::var("COCKROACH_HOST").unwrap_or_else(|_| {
                "vortex-giraffe-15678.jxf.gcp-us-east1.cockroachlabs.cloud".to_string()
            }),
            cockroach_port: env::var("COCKROACH_PORT")
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(26257),
            cockroach_user: env::var("COCKROACH_USER").unwrap_or_else(|_| "cubiczan".to_string()),
            cockroach_password: non_empty_env("COCKROACH_PASSWORD"),
            cockroach_ssl: env::var("COCKROACH_SSL").unwrap_or_else(|_| "require".to_string()),
        }
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
}

impl AppState {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

pub fn build_router(config: Arc<Config>) -> Router {
    let state = AppState::new(config);
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-api-key"),
        ]);

    Router::new()
        .route("/health", get(health_check))
        .route("/api/databases", get(list_databases))
        .route("/api/market-radar", get(get_market_radar))
        .route("/api/finance-cockpit", get(get_finance_cockpit))
        .route("/api/decision-brief/{decision_id}", get(get_decision_brief))
        .route("/api/battery-erp-dashboard", get(get_battery_erp_dashboard))
        .route("/api/{database}/tables", get(list_tables))
        .route(
            "/api/{database}/{table}",
            get(get_table_rows).post(insert_row),
        )
        .route("/api/{database}/{table}/{record_id}", get(get_row_by_id))
        .with_state(state)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    detail: String,
}

impl ApiError {
    fn new(status: StatusCode, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
        }
    }

    fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, detail)
    }

    fn unauthorized(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, detail)
    }

    fn not_found(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, detail)
    }

    fn service_unavailable(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, detail)
    }

    fn internal(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, detail)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "detail": self.detail }))).into_response()
    }
}

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), ApiError> {
    if let Some(expected) = &state.config.api_key {
        let supplied = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        // Constant-time comparison to avoid leaking the key via response timing.
        // `ct_eq` returns a `Choice`; only branch on the aggregated result.
        if supplied.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() != 1 {
            return Err(ApiError::unauthorized("Invalid or missing API key"));
        }
    }
    Ok(())
}

async fn health_check(State(state): State<AppState>) -> Json<Value> {
    let mut results = Map::new();
    for database in DATABASES {
        // Unauthenticated endpoint: never leak raw DB error detail here.
        // Report only a coarse per-database status.
        let value = match run_query(&state, database, "SELECT 1 AS ok", &[]).await {
            Ok(_) => Value::String("ok".to_string()),
            Err(_) => Value::String("degraded".to_string()),
        };
        results.insert((*database).to_string(), value);
    }

    let all_ok = results.values().all(|value| value == "ok");
    Json(json!({
        "status": if all_ok { "healthy" } else { "degraded" },
        "databases": results
    }))
}

async fn list_databases(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    let mut output = Vec::new();

    for database in DATABASES {
        match database_tables_summary(&state, database).await {
            Ok(tables) => output.push(json!({
                "database": database,
                "tables": tables,
                "totalTables": tables.len()
            })),
            Err(error) => output.push(json!({
                "database": database,
                "error": error.detail.chars().take(200).collect::<String>(),
                "tables": [],
                "totalTables": 0
            })),
        }
    }

    Ok(Json(json!({ "databases": output, "total": output.len() })))
}

async fn database_tables_summary(state: &AppState, database: &str) -> Result<Vec<Value>, ApiError> {
    let rows = run_query(
        state,
        database,
        r#"
        SELECT table_name,
               (SELECT count(*) FROM information_schema.columns c2
                WHERE c2.table_schema = 'public' AND c2.table_name = c.table_name) AS column_count
        FROM information_schema.tables c
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        "#,
        &[],
    )
    .await?;

    let mut tables = Vec::new();
    for row in rows {
        let table_name = row
            .get("table_name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let row_count = table_row_count(state, database, &table_name)
            .await
            .unwrap_or(-1);
        tables.push(json!({
            "name": table_name,
            "columns": row.get("column_count").cloned().unwrap_or(Value::Null),
            "rows": row_count
        }));
    }

    Ok(tables)
}

async fn list_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    ensure_database(&database)?;

    let tables = run_query(
        &state,
        &database,
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        "#,
        &[],
    )
    .await?;

    let mut output = Vec::new();
    for table in tables {
        let table_name = table
            .get("table_name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let columns = run_query(
            &state,
            &database,
            r#"
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
            "#,
            &[&table_name],
        )
        .await?;
        let row_count = table_row_count(&state, &database, &table_name)
            .await
            .unwrap_or(-1);
        output.push(json!({
            "name": table_name,
            "columns": columns,
            "rows": row_count
        }));
    }

    Ok(Json(json!({ "database": database, "tables": output })))
}

#[derive(Debug, Deserialize)]
struct Pagination {
    limit: Option<i64>,
    offset: Option<i64>,
}

impl Pagination {
    fn normalized(&self) -> (i64, i64) {
        let limit = self.limit.unwrap_or(50).clamp(1, 1000);
        let offset = self.offset.unwrap_or(0).max(0);
        (limit, offset)
    }
}

async fn get_table_rows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
    Query(pagination): Query<Pagination>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    ensure_database(&database)?;
    ensure_identifier("table", &table)?;
    ensure_table_exists(&state, &database, &table).await?;

    let (limit, offset) = pagination.normalized();
    let sql = format!(
        "SELECT * FROM {} ORDER BY rowid LIMIT $1 OFFSET $2",
        quote_ident(&table)
    );
    let rows = run_query(&state, &database, &sql, &[&limit, &offset]).await?;
    let total = table_row_count(&state, &database, &table)
        .await
        .unwrap_or(0);

    Ok(Json(json!({
        "database": database,
        "table": table,
        "rows": rows,
        "pagination": { "limit": limit, "offset": offset, "total": total }
    })))
}

async fn get_row_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table, record_id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    ensure_database(&database)?;
    ensure_identifier("table", &table)?;

    let primary_key = detect_primary_key(&state, &database, &table)
        .await?
        .ok_or_else(|| {
            ApiError::bad_request(format!("Cannot detect primary key for table '{table}'"))
        })?;

    let sql = format!(
        "SELECT * FROM {} WHERE {} = $1 LIMIT 1",
        quote_ident(&table),
        quote_ident(&primary_key)
    );
    let rows = run_query(&state, &database, &sql, &[&record_id]).await?;
    let row = rows.into_iter().next().ok_or_else(|| {
        ApiError::not_found(format!("Row with {primary_key}={record_id} not found"))
    })?;

    Ok(Json(json!({
        "database": database,
        "table": table,
        "primaryKey": primary_key,
        "row": row
    })))
}

#[derive(Debug, Deserialize)]
struct InsertRowRequest {
    data: Map<String, Value>,
}

async fn insert_row(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, table)): Path<(String, String)>,
    Json(body): Json<InsertRowRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    ensure_database(&database)?;
    ensure_identifier("table", &table)?;
    if body.data.is_empty() {
        return Err(ApiError::bad_request(
            "Request body must contain 'data' object",
        ));
    }

    let columns = body.data.keys().cloned().collect::<Vec<_>>();
    for column in &columns {
        ensure_identifier("column", column)?;
    }

    let placeholders = (1..=columns.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(&table),
        columns
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", ")
    );
    let params = columns
        .iter()
        .map(|column| value_to_param(body.data.get(column).cloned().unwrap_or(Value::Null)))
        .collect::<Vec<_>>();
    let param_refs = params
        .iter()
        .map(|param| param.as_tosql())
        .collect::<Vec<_>>();

    run_write(&state, &database, &sql, &param_refs).await?;

    Ok(Json(json!({
        "status": "created",
        "database": database,
        "table": table,
        "columns": columns
    })))
}

async fn get_market_radar(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    Ok(Json(
        fetch_market_radar_from_db(&state)
            .await
            .unwrap_or_else(mock_market_radar),
    ))
}

async fn fetch_market_radar_from_db(state: &AppState) -> Option<Value> {
    let indicators = run_query(
        state,
        "market_sentiment_fedgpt",
        "SELECT * FROM sentiment_indicators ORDER BY recorded_at DESC LIMIT 20",
        &[],
    )
    .await
    .ok()?;
    let holdings = run_query(
        state,
        "hedge_fund_13f_radar",
        "SELECT * FROM holdings ORDER BY value_usd DESC LIMIT 50",
        &[],
    )
    .await
    .unwrap_or_default();

    if indicators.is_empty() && holdings.is_empty() {
        return None;
    }

    let sentiment_indicators = indicators
        .iter()
        .map(|row| {
            json!({
                "name": pick_string(row, &["indicator_name", "name"]).unwrap_or_else(|| "Unknown".to_string()),
                "value": pick_f64(row, &["value"]).unwrap_or(0.0),
                "signal": pick_string(row, &["signal"]).unwrap_or_else(|| "neutral".to_string()),
                "direction": pick_string(row, &["direction"]).unwrap_or_else(|| "flat".to_string())
            })
        })
        .collect::<Vec<_>>();

    let sector_rotation = build_sector_rotation(&holdings);
    let fallback = mock_market_radar();

    Some(json!({
        "lastUpdated": Utc::now().to_rfc3339(),
        "sentiment": {
            "composite": 0.62,
            "label": "CAUTIOUSLY OPTIMISTIC",
            "indicators": if sentiment_indicators.is_empty() {
                fallback["sentiment"]["indicators"].clone()
            } else {
                Value::Array(sentiment_indicators)
            }
        },
        "fedPolicy": fallback["fedPolicy"].clone(),
        "sectorRotation": if sector_rotation.is_empty() {
            fallback["sectorRotation"].clone()
        } else {
            Value::Array(sector_rotation)
        },
        "alerts": fallback["alerts"].clone()
    }))
}

async fn get_finance_cockpit(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    Ok(Json(
        fetch_finance_cockpit_from_db(&state)
            .await
            .unwrap_or_else(mock_finance_cockpit),
    ))
}

async fn fetch_finance_cockpit_from_db(state: &AppState) -> Option<Value> {
    let budgets = run_query(
        state,
        "closed_loop_finance",
        "SELECT * FROM budgets ORDER BY period DESC LIMIT 5",
        &[],
    )
    .await
    .ok()?;
    if budgets.is_empty() {
        return None;
    }

    let first = &budgets[0];
    let total = pick_f64(first, &["total_budget", "amount"]).unwrap_or(2_500_000.0);
    let spent = pick_f64(first, &["actual_spend", "spent"]).unwrap_or(1_420_000.0);
    let remaining = total - spent;
    let monthly = if spent > 0.0 {
        spent.round()
    } else {
        178_000.0
    };
    let weekly = (monthly / 4.33).round();

    Some(json!({
        "budget": {
            "total": total,
            "spent": spent,
            "remaining": remaining,
            "period": pick_string(first, &["period", "name"]).unwrap_or_else(|| "Q2 2026".to_string())
        },
        "burnRate": {
            "monthly": monthly,
            "weekly": weekly,
            "trend": "stable",
            "runwayMonths": if monthly > 0.0 { (remaining / monthly * 10.0).round() / 10.0 } else { 6.0 }
        },
        "cashForecast": mock_finance_cockpit()["cashForecast"].clone(),
        "workingCapital": mock_finance_cockpit()["workingCapital"].clone()
    }))
}

async fn get_decision_brief(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(decision_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    Ok(Json(
        fetch_decision_brief_from_db(&state, &decision_id)
            .await
            .unwrap_or_else(|| mock_decision_brief(&decision_id)),
    ))
}

async fn fetch_decision_brief_from_db(state: &AppState, decision_id: &str) -> Option<Value> {
    let briefs = run_query(
        state,
        "multi_agent_cfo_os",
        "SELECT * FROM cfo_briefs WHERE brief_id = $1 OR id = $1 LIMIT 1",
        &[&decision_id],
    )
    .await
    .ok()?;
    let brief = briefs.first()?;

    Some(json!({
        "id": decision_id,
        "title": pick_string(brief, &["title", "case_title"]).unwrap_or_else(|| "Decision Brief".to_string()),
        "status": pick_string(brief, &["status"]).unwrap_or_else(|| "draft".to_string()),
        "priority": pick_string(brief, &["priority"]).unwrap_or_else(|| "medium".to_string()),
        "created": brief.get("created_at").cloned().unwrap_or_else(|| Value::String(Utc::now().to_rfc3339())),
        "brief": {
            "context": pick_string(brief, &["context", "description"]).unwrap_or_default(),
            "options": [{"label": "Default", "description": pick_string(brief, &["recommendation"]).unwrap_or_default()}],
            "recommendation": pick_string(brief, &["recommendation"]).unwrap_or_default()
        },
        "financialImpact": {
            "totalBudget": pick_f64(brief, &["budget_amount"]).unwrap_or(500000.0),
            "roi12m": pick_f64(brief, &["roi_12m"]).unwrap_or(0.34),
            "paybackMonths": pick_i64(brief, &["payback_months"]).unwrap_or(8),
            "npv": pick_f64(brief, &["npv"]).unwrap_or(125000.0)
        },
        "artifacts": [],
        "evidence": [],
        "rounds": [],
        "forecasts": [],
        "findings": [],
        "audits": []
    }))
}

async fn get_battery_erp_dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state)?;
    Ok(Json(
        fetch_battery_erp_from_db(&state)
            .await
            .unwrap_or_else(mock_battery_erp_dashboard),
    ))
}

async fn fetch_battery_erp_from_db(state: &AppState) -> Option<Value> {
    let tables = run_query(
        state,
        "battery_erp",
        r#"
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        "#,
        &[],
    )
    .await
    .ok()?;

    if tables.is_empty() {
        return None;
    }

    let mut summary = Map::new();
    let mut table_map = Map::new();
    for table in tables {
        let Some(name) = table.get("table_name").and_then(Value::as_str) else {
            continue;
        };
        if !is_valid_identifier(name) {
            continue;
        }
        let row_count = table_row_count(state, "battery_erp", name)
            .await
            .unwrap_or(0);
        table_map.insert(name.to_string(), json!({ "rows": row_count }));
        let lower = name.to_ascii_lowercase();
        if lower.contains("inventory") || lower.contains("stock") {
            summary.insert("totalInventoryItems".to_string(), json!(row_count));
        }
        if lower.contains("order") {
            summary.insert("activeOrders".to_string(), json!(row_count));
        }
        if lower.contains("hazard") || lower.contains("hazmat") || lower.contains("waste") {
            summary.insert("activeManifests".to_string(), json!(row_count));
        }
    }

    Some(json!({
        "lastUpdated": Utc::now().to_rfc3339(),
        "tables": table_map,
        "summary": summary
    }))
}

async fn connect_database(state: &AppState, database: &str) -> Result<Client, ApiError> {
    if state.config.cockroach_password.is_none() {
        return Err(ApiError::service_unavailable(
            "COCKROACH_PASSWORD is required for live database access",
        ));
    }

    let mut pg = tokio_postgres::Config::new();
    pg.host(&state.config.cockroach_host);
    pg.port(state.config.cockroach_port);
    pg.user(&state.config.cockroach_user);
    if let Some(password) = &state.config.cockroach_password {
        pg.password(password);
    }
    pg.dbname(database);

    if state.config.cockroach_ssl.eq_ignore_ascii_case("disable") {
        pg.ssl_mode(SslMode::Disable);
        // Bound the connect handshake so a stalled server cannot hang a request.
        let (client, connection) = with_timeout(pg.connect(NoTls), CONNECT_TIMEOUT)
            .await
            .map_err(connect_error)?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "database connection closed");
            }
        });
        Ok(client)
    } else {
        pg.ssl_mode(SslMode::Require);
        let connector = TlsConnector::builder()
            .build()
            .map_err(|error| ApiError::service_unavailable(error.to_string()))?;
        let connector = MakeTlsConnector::new(connector);
        // Bound the connect handshake so a stalled server cannot hang a request.
        let (client, connection) = with_timeout(pg.connect(connector), CONNECT_TIMEOUT)
            .await
            .map_err(connect_error)?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "database connection closed");
            }
        });
        Ok(client)
    }
}

/// Map a resilient-call connect failure onto a 503, distinguishing timeouts.
fn connect_error(error: ResilienceError<tokio_postgres::Error>) -> ApiError {
    match error {
        ResilienceError::Timeout(dur) => {
            ApiError::service_unavailable(format!("database connect timed out after {dur:?}"))
        }
        other => ApiError::service_unavailable(
            other
                .into_source()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "database connect failed".to_string()),
        ),
    }
}

async fn run_query(
    state: &AppState,
    database: &str,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Value>, ApiError> {
    ensure_database(database)?;
    let client = connect_database(state, database).await?;
    // Retry CockroachDB serialization failures (SQLSTATE 40001) under a single
    // 30s budget covering all attempts and their backoff.
    let rows = with_timeout(
        crdb_retry(|| async { client.query(sql, params).await.map_err(to_sql_error) }),
        QUERY_TIMEOUT,
    )
    .await
    .map_err(|error| query_error("query", error))?;
    Ok(rows.into_iter().map(row_to_json).collect())
}

async fn run_write(
    state: &AppState,
    database: &str,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<(), ApiError> {
    ensure_database(database)?;
    let client = connect_database(state, database).await?;
    // Retry CockroachDB serialization failures (SQLSTATE 40001) under a single
    // 30s budget covering all attempts and their backoff.
    with_timeout(
        crdb_retry(|| async { client.execute(sql, params).await.map_err(to_sql_error) }),
        QUERY_TIMEOUT,
    )
    .await
    .map_err(|error| query_error("write", error))?;
    Ok(())
}

/// Convert a tokio-postgres error into the crate's `SqlError`, preserving the
/// SQLSTATE so `crdb_retry` can recognize serialization failures (40001).
fn to_sql_error(error: tokio_postgres::Error) -> SqlError {
    let sqlstate = error
        .code()
        .map(|state| state.code().to_string())
        .unwrap_or_default();
    SqlError::new(sqlstate, error.to_string())
}

/// Map a resilient-call query/write failure onto an `ApiError`, distinguishing
/// the 30s timeout from exhausted/terminal SQL errors. The outer `Timeout`
/// comes from `with_timeout`; the inner `ResilienceError<SqlError>` from
/// `crdb_retry`.
fn query_error(kind: &str, error: ResilienceError<ResilienceError<SqlError>>) -> ApiError {
    match error {
        ResilienceError::Timeout(dur) => {
            ApiError::service_unavailable(format!("database {kind} timed out after {dur:?}"))
        }
        other => ApiError::internal(
            other
                .into_source()
                .and_then(|inner| inner.into_source())
                .map(|e| e.message)
                .unwrap_or_else(|| format!("database {kind} failed")),
        ),
    }
}

async fn table_row_count(state: &AppState, database: &str, table: &str) -> Result<i64, ApiError> {
    ensure_identifier("table", table)?;
    let sql = format!("SELECT count(*) AS cnt FROM {}", quote_ident(table));
    let rows = run_query(state, database, &sql, &[]).await?;
    Ok(rows
        .first()
        .and_then(|row| row.get("cnt"))
        .and_then(json_value_to_i64)
        .unwrap_or(0))
}

async fn ensure_table_exists(
    state: &AppState,
    database: &str,
    table: &str,
) -> Result<(), ApiError> {
    let rows = run_query(
        state,
        database,
        r#"
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = $1 LIMIT 1
        "#,
        &[&table],
    )
    .await?;

    if rows.is_empty() {
        return Err(ApiError::not_found(format!(
            "Table '{table}' not found in '{database}'"
        )));
    }
    Ok(())
}

async fn detect_primary_key(
    state: &AppState,
    database: &str,
    table: &str,
) -> Result<Option<String>, ApiError> {
    ensure_identifier("table", table)?;
    let rows = run_query(
        state,
        database,
        r#"
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = $1
        ORDER BY kcu.ordinal_position
        LIMIT 1
        "#,
        &[&table],
    )
    .await?;
    if let Some(pk) = rows
        .first()
        .and_then(|row| row.get("column_name"))
        .and_then(Value::as_str)
    {
        return Ok(Some(pk.to_string()));
    }

    for candidate in ["id", "ID", "uuid", "UUID", "pk", "rowid"] {
        let exists = run_query(
            state,
            database,
            r#"
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2 LIMIT 1
            "#,
            &[&table, &candidate],
        )
        .await?;
        if !exists.is_empty() {
            return Ok(Some(candidate.to_string()));
        }
    }

    Ok(None)
}

fn ensure_database(database: &str) -> Result<(), ApiError> {
    if DATABASES.contains(&database) {
        Ok(())
    } else {
        Err(ApiError::not_found(format!("Unknown database: {database}")))
    }
}

fn ensure_identifier(kind: &str, value: &str) -> Result<(), ApiError> {
    if is_valid_identifier(value) {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!("Invalid {kind} name")))
    }
}

pub fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn quote_ident(value: &str) -> String {
    format!("\"{value}\"")
}

fn row_to_json(row: Row) -> Value {
    let mut map = Map::new();
    for (idx, column) in row.columns().iter().enumerate() {
        map.insert(
            column.name().to_string(),
            cell_to_json(&row, idx, column.type_()),
        );
    }
    Value::Object(map)
}

fn cell_to_json(row: &Row, idx: usize, ty: &Type) -> Value {
    if ty == &Type::BOOL {
        return nullable(row.try_get::<usize, Option<bool>>(idx));
    }
    if ty == &Type::INT2 {
        return nullable(row.try_get::<usize, Option<i16>>(idx));
    }
    if ty == &Type::INT4 {
        return nullable(row.try_get::<usize, Option<i32>>(idx));
    }
    if ty == &Type::INT8 {
        return nullable(row.try_get::<usize, Option<i64>>(idx));
    }
    if ty == &Type::FLOAT4 {
        return nullable(row.try_get::<usize, Option<f32>>(idx));
    }
    if ty == &Type::FLOAT8 {
        return nullable(row.try_get::<usize, Option<f64>>(idx));
    }
    if ty == &Type::NUMERIC {
        return row
            .try_get::<usize, Option<String>>(idx)
            .map(|value| value.map(Value::String).unwrap_or(Value::Null))
            .unwrap_or_else(|_| unsupported_value(ty));
    }
    if ty == &Type::VARCHAR || ty == &Type::TEXT || ty == &Type::BPCHAR || ty == &Type::NAME {
        return nullable(row.try_get::<usize, Option<String>>(idx));
    }
    if ty == &Type::TIMESTAMPTZ {
        return match row.try_get::<usize, Option<DateTime<Utc>>>(idx) {
            Ok(Some(value)) => Value::String(value.to_rfc3339()),
            Ok(None) => Value::Null,
            Err(_) => unsupported_value(ty),
        };
    }
    if ty == &Type::TIMESTAMP {
        return match row.try_get::<usize, Option<NaiveDateTime>>(idx) {
            Ok(Some(value)) => Value::String(value.to_string()),
            Ok(None) => Value::Null,
            Err(_) => unsupported_value(ty),
        };
    }
    if ty == &Type::DATE {
        return match row.try_get::<usize, Option<NaiveDate>>(idx) {
            Ok(Some(value)) => Value::String(value.to_string()),
            Ok(None) => Value::Null,
            Err(_) => unsupported_value(ty),
        };
    }
    if ty == &Type::TIME {
        return match row.try_get::<usize, Option<NaiveTime>>(idx) {
            Ok(Some(value)) => Value::String(value.to_string()),
            Ok(None) => Value::Null,
            Err(_) => unsupported_value(ty),
        };
    }
    if ty == &Type::JSON || ty == &Type::JSONB {
        return nullable(row.try_get::<usize, Option<Value>>(idx));
    }
    if ty == &Type::UUID {
        return match row.try_get::<usize, Option<Uuid>>(idx) {
            Ok(Some(value)) => Value::String(value.to_string()),
            Ok(None) => Value::Null,
            Err(_) => unsupported_value(ty),
        };
    }
    if ty == &Type::BYTEA {
        return match row.try_get::<usize, Option<Vec<u8>>>(idx) {
            Ok(Some(value)) => Value::String(String::from_utf8_lossy(&value).to_string()),
            Ok(None) => Value::Null,
            Err(_) => unsupported_value(ty),
        };
    }

    row.try_get::<usize, Option<String>>(idx)
        .map(|value| value.map(Value::String).unwrap_or(Value::Null))
        .unwrap_or_else(|_| unsupported_value(ty))
}

fn unsupported_value(ty: &Type) -> Value {
    Value::String(format!("<unsupported:{}>", ty.name()))
}

fn nullable<T>(result: Result<Option<T>, tokio_postgres::Error>) -> Value
where
    T: serde::Serialize,
{
    match result {
        Ok(Some(value)) => serde_json::to_value(value).unwrap_or(Value::Null),
        Ok(None) => Value::Null,
        Err(_) => Value::Null,
    }
}

enum OwnedParam {
    Null(Option<String>),
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Json(PgJson<Value>),
}

impl OwnedParam {
    fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            OwnedParam::Null(value) => value,
            OwnedParam::Bool(value) => value,
            OwnedParam::I64(value) => value,
            OwnedParam::F64(value) => value,
            OwnedParam::String(value) => value,
            OwnedParam::Json(value) => value,
        }
    }
}

fn value_to_param(value: Value) -> OwnedParam {
    match value {
        Value::Null => OwnedParam::Null(None),
        Value::Bool(value) => OwnedParam::Bool(value),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                OwnedParam::I64(value)
            } else if let Some(value) = number.as_f64() {
                OwnedParam::F64(value)
            } else {
                OwnedParam::String(number.to_string())
            }
        }
        Value::String(value) => OwnedParam::String(value),
        Value::Array(_) | Value::Object(_) => OwnedParam::Json(PgJson(value)),
    }
}

fn build_sector_rotation(rows: &[Value]) -> Vec<Value> {
    let mut by_sector: BTreeMap<String, f64> = BTreeMap::new();
    for row in rows {
        let sector = pick_string(row, &["sector"]).unwrap_or_else(|| "Other".to_string());
        let flow = pick_f64(row, &["value_usd", "value"]).unwrap_or_default();
        *by_sector.entry(sector).or_insert(0.0) += flow;
    }
    let total = by_sector.values().sum::<f64>();
    let mut sectors = by_sector
        .into_iter()
        .map(|(sector, flow)| {
            let weight = if total > 0.0 {
                (flow / total * 1000.0).round() / 10.0
            } else {
                0.0
            };
            json!({ "sector": sector, "flow": flow, "direction": "inflow", "weight": weight })
        })
        .collect::<Vec<_>>();
    sectors.sort_by(|a, b| {
        let af = a.get("flow").and_then(Value::as_f64).unwrap_or_default();
        let bf = b.get("flow").and_then(Value::as_f64).unwrap_or_default();
        bf.partial_cmp(&af).unwrap_or(std::cmp::Ordering::Equal)
    });
    sectors.truncate(5);
    sectors
}

fn pick_string(row: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| row.get(*key))
        .find_map(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .or_else(|| Some(value.to_string()))
        })
}

fn pick_f64(row: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .filter_map(|key| row.get(*key))
        .find_map(json_value_to_f64)
}

fn pick_i64(row: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .filter_map(|key| row.get(*key))
        .find_map(json_value_to_i64)
}

fn json_value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn json_value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn mock_market_radar() -> Value {
    json!({
        "lastUpdated": "2026-05-15T00:00:00Z",
        "sentiment": {
            "composite": 0.62,
            "label": "CAUTIOUSLY OPTIMISTIC",
            "indicators": [
                {"name": "VIX", "value": 16.4, "signal": "neutral", "direction": "flat"},
                {"name": "Credit Spreads (IG)", "value": 98, "signal": "bullish", "direction": "tightening"},
                {"name": "10Y-2Y Spread", "value": -18, "signal": "bearish", "direction": "flat"},
                {"name": "DXY", "value": 104.2, "signal": "neutral", "direction": "weakening"},
                {"name": "Put/Call Ratio", "value": 0.85, "signal": "bullish", "direction": "declining"}
            ]
        },
        "fedPolicy": {
            "currentRate": "5.25-5.50",
            "nextMeeting": "2026-06-18",
            "impliedCut": 0.25,
            "stance": "HAWKISH HOLD"
        },
        "sectorRotation": [
            {"sector": "Technology", "flow": 4200000000_i64, "direction": "inflow", "weight": 29.1},
            {"sector": "Healthcare", "flow": 1800000000_i64, "direction": "inflow", "weight": 13.4},
            {"sector": "Financials", "flow": 1100000000_i64, "direction": "inflow", "weight": 12.8}
        ],
        "alerts": [
            {"type": "warning", "message": "Yield curve inversion deepening", "since": "2026-05-10"},
            {"type": "info", "message": "Fed funds futures imply 78% probability of July cut", "since": "2026-05-14"}
        ]
    })
}

fn mock_finance_cockpit() -> Value {
    json!({
        "budget": {
            "total": 2500000,
            "spent": 1420000,
            "remaining": 1080000,
            "period": "Q2 2026"
        },
        "burnRate": {
            "monthly": 178000,
            "weekly": 44500,
            "trend": "stable",
            "runwayMonths": 6
        },
        "cashForecast": {
            "currentBalance": 3200000,
            "minProjected": 1850000,
            "minWeek": 8,
            "endPosition": 2750000,
            "riskWeeks": [6, 7, 8],
            "hasCriticalRisk": false,
            "hasWorkingCapitalRisk": true
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
                {"action": "Extend DPO with top 5 vendors by 15 days", "savings": 32000}
            ]
        }
    })
}

fn mock_decision_brief(decision_id: &str) -> Value {
    json!({
        "id": decision_id,
        "title": "Q2 Capex Allocation Review",
        "status": "in_review",
        "priority": "high",
        "created": "2026-05-10T14:30:00Z",
        "brief": {
            "context": "Review of proposed capital expenditure allocation for Q2 2026 across three major initiatives.",
            "options": [
                {"label": "Option A", "description": "Prioritize infrastructure scaling (60% infra, 25% product, 15% reserve)"},
                {"label": "Option B", "description": "Balanced growth approach (40% infra, 40% product, 20% reserve)"},
                {"label": "Option C", "description": "Product-first strategy (25% infra, 55% product, 20% reserve)"}
            ],
            "recommendation": "Option B recommended based on current market conditions and cash flow projections."
        },
        "financialImpact": {
            "totalBudget": 500000,
            "roi12m": 0.34,
            "paybackMonths": 8,
            "npv": 125000
        },
        "evidence": [
            {"source": "market_sentiment_fedgpt", "summary": "Market conditions favor balanced growth"},
            {"source": "closed_loop_finance", "summary": "Cash runway supports moderate investment"}
        ],
        "approvalChain": ["CFO", "Board", "Audit Committee"]
    })
}

fn mock_battery_erp_dashboard() -> Value {
    json!({
        "lastUpdated": "2026-05-15T00:00:00Z",
        "inventory": {
            "totalUnits": 12450,
            "incoming": 3200,
            "outgoing": 2870,
            "lowStockAlerts": 3
        },
        "quality": {
            "passRate": 97.3,
            "rejectRate": 1.2,
            "pendingInspection": 180,
            "avgCycleTime": 4.2
        },
        "supplyChain": {
            "activeOrders": 24,
            "onTimeDelivery": 91.5,
            "supplierIssues": 2,
            "leadTimeDays": 14
        },
        "production": {
            "dailyOutput": 890,
            "targetOutput": 950,
            "utilization": 93.7,
            "downtimeHours": 1.8
        },
        "hazmat": {
            "activeManifests": 7,
            "compliant": true,
            "nextAudit": "2026-06-01"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_safe_identifiers() {
        assert!(is_valid_identifier("closed_loop_finance"));
        assert!(is_valid_identifier("table_123"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("users;drop"));
        assert!(!is_valid_identifier("public.users"));
        assert!(!is_valid_identifier("user-name"));
    }

    #[test]
    fn pagination_is_clamped() {
        let pagination = Pagination {
            limit: Some(5000),
            offset: Some(-20),
        };
        assert_eq!(pagination.normalized(), (1000, 0));
    }

    #[test]
    fn decision_brief_mock_uses_requested_id() {
        let value = mock_decision_brief("abc-123");
        assert_eq!(value["id"], "abc-123");
    }

    #[test]
    fn numeric_strings_are_readable() {
        assert_eq!(json_value_to_i64(&json!("42")), Some(42));
        assert_eq!(json_value_to_f64(&json!("12.5")), Some(12.5));
    }
}
