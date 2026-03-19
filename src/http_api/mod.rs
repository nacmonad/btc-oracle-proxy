//! HTTP REST API (execution signer scaffolding + metadata cache endpoints)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::path::Path as FsPath;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};
use tracing::info;
use std::str::FromStr as _;
use std::time::Instant;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::clob::types::{OrderType, Side};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::Decimal;
use polymarket_client_sdk::POLYGON;

use crate::error::OracleResult;
use crate::models::{AppState, ExpectedPModelRuntime, LmsrRuntimeParam};

#[derive(Clone)]
pub struct HttpApiState {
    pub app_state: Arc<RwLock<AppState>>,
    pub clob_base_url: String,
    pub client: reqwest::Client,
    pub meta_cache: Arc<RwLock<HashMap<String, MetaCached>>>,
    pub lmsr_pending: Arc<RwLock<HashMap<String, LmsrParamsRecord>>>,
    pub lmsr_last_good_version: Arc<RwLock<Option<String>>>,
    pub lmsr_health_degraded: Arc<AtomicBool>,
    pub presign_queue: Arc<RwLock<HashMap<String, PresignedTxEnvelope>>>,
    pub lmsr_metrics: Arc<RwLock<HashMap<String, LmsrVersionMetrics>>>,
    pub lmsr_state_file: String,
}





#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetaCached {
    pub token_id: String,
    pub tick_size: Option<String>,
    pub fee_rate_bps: Option<u32>,
    pub neg_risk: Option<bool>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedMetaRequest {
    pub token_id: String,
    pub tick_size: Option<String>,
    pub fee_rate_bps: Option<u32>,
    pub neg_risk: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignRequest {
    pub condition_id: String,
    pub token_id: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub tif: String,
    pub expiration_ts: Option<i64>,
    pub strategy: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignResponse {
    pub ok: bool,
    pub message: String,
    pub metadata: MetaCached,
    pub request_echo: SignRequest,
    pub signed_order: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrParamsRequest {
    pub alpha: f64,
    pub b: f64,
    pub version: String,
    pub effective_from: DateTime<Utc>,
    pub trained_at: Option<DateTime<Utc>>,
    pub train_window: Option<String>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrParamsRecord {
    pub alpha: f64,
    pub b: f64,
    pub version: String,
    pub effective_from: DateTime<Utc>,
    pub trained_at: Option<DateTime<Utc>>,
    pub train_window: Option<String>,
    pub notes: Option<String>,
    pub status: String,
    pub stored_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrPersistedState {
    pub pending: HashMap<String, LmsrParamsRecord>,
    pub last_good_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedPModelRequest {
    pub version: String,
    pub intercept: f64,
    pub coefs: HashMap<String, f64>,
    pub means: Option<HashMap<String, f64>>,
    pub stds: Option<HashMap<String, f64>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedPModelResponse {
    pub ok: bool,
    pub version: Option<String>,
    pub enabled: bool,
    pub coef_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrQuoteRequest {
    pub market_probability: f64,
    pub q_yes: Option<f64>,
    pub q_no: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrQuoteResponse {
    pub p_lmsr: f64,
    pub delta_lmsr: f64,
    pub delta_lmsr_z: f64,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrHealthTriggerRequest {
    pub degraded: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrHealthTriggerResponse {
    pub ok: bool,
    pub degraded: bool,
    pub active_version: Option<String>,
    pub last_good_version: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedTxEnvelope {
    pub market_id: String,
    pub side: String,
    pub version: String,
    pub nonce: String,
    pub signed_tx: String,
    pub expires_at: DateTime<Utc>,
    pub reference_price: f64,
    pub notional: f64,
    pub queued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignEnqueueRequest {
    pub market_id: String,
    pub side: String,
    pub version: String,
    pub nonce: String,
    pub signed_tx: String,
    pub expires_at: DateTime<Utc>,
    pub reference_price: f64,
    pub notional: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignQueueResponse {
    pub ok: bool,
    pub key: String,
    pub queue_size: usize,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignDequeueRequest {
    pub market_id: String,
    pub side: String,
    pub version: String,
    pub current_price: f64,
    pub max_price_drift_bps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignDequeueResponse {
    pub ok: bool,
    pub message: String,
    pub tx: Option<PresignedTxEnvelope>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LmsrVersionMetrics {
    pub version: String,
    pub quote_count: u64,
    pub quote_latency_ms_avg: f64,
    pub signal_hit_rate_proxy: Option<f64>,
    pub ev_proxy: Option<f64>,
    pub reject_count: u64,
    pub rollback_events: u64,
    pub last_updated: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LmsrMetricsResponse {
    pub ok: bool,
    pub active_version: Option<String>,
    pub degraded: bool,
    pub by_version: Vec<LmsrVersionMetrics>,
}

fn load_lmsr_state(path: &str) -> Option<LmsrPersistedState> {
    let p = FsPath::new(path);
    if !p.exists() {
        return None;
    }
    let raw = std::fs::read_to_string(p).ok()?;
    serde_json::from_str::<LmsrPersistedState>(&raw).ok()
}

fn persist_lmsr_state(path: &str, pending: &HashMap<String, LmsrParamsRecord>, last_good: &Option<String>) {
    let st = LmsrPersistedState {
        pending: pending.clone(),
        last_good_version: last_good.clone(),
    };
    if let Some(parent) = FsPath::new(path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(raw) = serde_json::to_string_pretty(&st) {
        let _ = std::fs::write(path, raw);
    }
}

pub async fn run_server(
    state: Arc<RwLock<AppState>>,
    listen_addr: &str,
    clob_base_url: &str,
) -> OracleResult<()> {
    info!(listen_addr, "Starting HTTP API server");

    let lmsr_state_file = std::env::var("LMSR_STATE_FILE")
        .unwrap_or_else(|_| "./data/lmsr_params_state.json".to_string());

    let api_state = HttpApiState {
        app_state: state,
        clob_base_url: clob_base_url.trim_end_matches('/').to_string(),
        client: reqwest::Client::new(),
        meta_cache: Arc::new(RwLock::new(HashMap::new())),
        lmsr_pending: Arc::new(RwLock::new(HashMap::new())),
        lmsr_last_good_version: Arc::new(RwLock::new(None)),
        lmsr_health_degraded: Arc::new(AtomicBool::new(false)),
        presign_queue: Arc::new(RwLock::new(HashMap::new())),
        lmsr_metrics: Arc::new(RwLock::new(HashMap::new())),
        lmsr_state_file,
    };

    if let Some(saved) = load_lmsr_state(&api_state.lmsr_state_file) {
        {
            let mut pending = api_state.lmsr_pending.write().await;
            *pending = saved.pending.clone();
        }
        {
            let mut lg = api_state.lmsr_last_good_version.write().await;
            *lg = saved.last_good_version.clone();
        }
        {
            let mut app = api_state.app_state.write().await;
            for rec in saved.pending.values() {
                app.lmsr_params.insert(
                    rec.version.clone(),
                    LmsrRuntimeParam {
                        alpha: rec.alpha,
                        b: rec.b,
                        version: rec.version.clone(),
                        effective_from: rec.effective_from,
                    },
                );
            }
        }
        info!(file=%api_state.lmsr_state_file, count=saved.pending.len(), "restored LMSR params from disk");
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/metadata/seed", post(seed_metadata))
        .route("/v1/metadata/:token_id", get(get_metadata))
        .route("/v1/metadata/resolve/:token_id", post(resolve_metadata))
        .route("/v1/sign", post(sign_stub))
        .route("/lmsr/params", post(set_lmsr_params))
        .route("/lmsr/params/active", get(get_lmsr_params_active))
        .route("/lmsr/quote", post(lmsr_quote))
        .route("/lmsr/health/trigger", post(trigger_lmsr_health))
        .route("/expected-p/model", post(set_expected_p_model).get(get_expected_p_model))
        .route("/v1/presign/enqueue", post(presign_enqueue))
        .route("/v1/presign/dequeue", post(presign_dequeue))
        .route("/lmsr/metrics", get(get_lmsr_metrics))
        .with_state(api_state);

    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true, "service": "oracle-http-api"}))
}

async fn set_expected_p_model(
    State(st): State<HttpApiState>,
    Json(req): Json<ExpectedPModelRequest>,
) -> Result<Json<ExpectedPModelResponse>, (StatusCode, String)> {
    if req.version.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "version required".to_string()));
    }
    if !req.intercept.is_finite() {
        return Err((StatusCode::BAD_REQUEST, "intercept must be finite".to_string()));
    }
    let model = ExpectedPModelRuntime {
        version: req.version.clone(),
        intercept: req.intercept,
        coefs: req.coefs.clone(),
        means: req.means.unwrap_or_default(),
        stds: req.stds.unwrap_or_default(),
        enabled: req.enabled.unwrap_or(true),
    };
    {
        let mut app = st.app_state.write().await;
        app.expected_p_model = Some(model.clone());
    }
    Ok(Json(ExpectedPModelResponse {
        ok: true,
        version: Some(model.version),
        enabled: model.enabled,
        coef_count: model.coefs.len(),
    }))
}

async fn get_expected_p_model(
    State(st): State<HttpApiState>,
) -> Result<Json<ExpectedPModelResponse>, (StatusCode, String)> {
    let app = st.app_state.read().await;
    let m = app.expected_p_model.clone();
    Ok(Json(ExpectedPModelResponse {
        ok: m.is_some(),
        version: m.as_ref().map(|x| x.version.clone()),
        enabled: m.as_ref().map(|x| x.enabled).unwrap_or(false),
        coef_count: m.as_ref().map(|x| x.coefs.len()).unwrap_or(0),
    }))
}

async fn seed_metadata(
    State(st): State<HttpApiState>,
    Json(req): Json<SeedMetaRequest>,
) -> Result<Json<MetaCached>, (StatusCode, String)> {
    if req.token_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "token_id required".to_string()));
    }
    let row = MetaCached {
        token_id: req.token_id.clone(),
        tick_size: req.tick_size.clone(),
        fee_rate_bps: req.fee_rate_bps,
        neg_risk: req.neg_risk,
        source: "seed".to_string(),
    };
    st.meta_cache.write().await.insert(req.token_id, row.clone());
    Ok(Json(row))
}

async fn get_metadata(
    State(st): State<HttpApiState>,
    Path(token_id): Path<String>,
) -> Result<Json<MetaCached>, (StatusCode, String)> {
    if let Some(v) = st.meta_cache.read().await.get(&token_id).cloned() {
        return Ok(Json(v));
    }
    Err((StatusCode::NOT_FOUND, "token metadata not in cache".to_string()))
}

async fn resolve_metadata(
    State(st): State<HttpApiState>,
    Path(token_id): Path<String>,
) -> Result<Json<MetaCached>, (StatusCode, String)> {
    if let Some(v) = st.meta_cache.read().await.get(&token_id).cloned() {
        return Ok(Json(v));
    }

    let tick_url = format!("{}/tick-size", st.clob_base_url);
    let fee_url = format!("{}/fee-rate", st.clob_base_url);
    let neg_url = format!("{}/neg-risk", st.clob_base_url);

    let tick = st
        .client
        .get(&tick_url)
        .query(&[("token_id", token_id.as_str())])
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("tick-size request failed: {e}")))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("tick-size parse failed: {e}")))?;

    let fee = st
        .client
        .get(&fee_url)
        .query(&[("token_id", token_id.as_str())])
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("fee-rate request failed: {e}")))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("fee-rate parse failed: {e}")))?;

    let neg = st
        .client
        .get(&neg_url)
        .query(&[("token_id", token_id.as_str())])
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("neg-risk request failed: {e}")))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("neg-risk parse failed: {e}")))?;

    let tick_size = tick
        .get("minimum_tick_size")
        .and_then(|v| v.as_str().map(|s| s.to_string()).or_else(|| v.as_f64().map(|f| f.to_string())))
        .or_else(|| Some("0.01".to_string()));

    let row = MetaCached {
        token_id: token_id.clone(),
        tick_size,
        fee_rate_bps: fee.get("base_fee").and_then(|v| v.as_u64()).map(|v| v as u32),
        neg_risk: neg.get("neg_risk").and_then(|v| v.as_bool()),
        source: "rest".to_string(),
    };

    st.meta_cache.write().await.insert(token_id, row.clone());
    Ok(Json(row))
}




fn logistic(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

async fn lmsr_quote(
    State(st): State<HttpApiState>,
    Json(req): Json<LmsrQuoteRequest>,
) -> Result<Json<LmsrQuoteResponse>, (StatusCode, String)> {
    if !req.market_probability.is_finite() || req.market_probability <= 0.0 || req.market_probability >= 1.0 {
        return Err((StatusCode::BAD_REQUEST, "market_probability must be in (0,1)".to_string()));
    }

    let now = Utc::now();
    let map = st.lmsr_pending.read().await;
    let prm = active_lmsr_record(&map, now).ok_or((StatusCode::NOT_FOUND, "no active lmsr params".to_string()))?;

    let p_lmsr = match (req.q_yes, req.q_no) {
        (Some(qy), Some(qn)) if qy.is_finite() && qn.is_finite() => {
            let e_yes = (qy / prm.b).exp();
            let e_no = (qn / prm.b).exp();
            e_yes / (e_yes + e_no)
        }
        _ => {
            // fallback: apply alpha as logit tilt over market probability
            let logit_m = (req.market_probability / (1.0 - req.market_probability)).ln();
            logistic(prm.alpha * logit_m)
        }
    };

    let delta = p_lmsr - req.market_probability;
    // z-score placeholder until rolling state is integrated in tick-frame pipeline
    let delta_z = delta / 0.01_f64.max(1e-6);

    {
        let mut mm = st.lmsr_metrics.write().await;
        let e = mm.entry(prm.version.clone()).or_insert_with(|| LmsrVersionMetrics {
            version: prm.version.clone(),
            ..Default::default()
        });
        let n_prev = e.quote_count as f64;
        let elapsed_ms = 0.0_f64; // placeholder until request timing plumbing is added
        e.quote_count += 1;
        e.quote_latency_ms_avg = if e.quote_count == 1 {
            elapsed_ms
        } else {
            ((e.quote_latency_ms_avg * n_prev) + elapsed_ms) / (n_prev + 1.0)
        };
        e.ev_proxy = Some(delta);

        // Monitoring + auto-rollback thresholding
        // If absolute EV proxy remains too negative beyond warmup, mark degraded
        let warmup_quotes = std::env::var("LMSR_MONITOR_WARMUP_QUOTES")
            .ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(50);
        let min_ev = std::env::var("LMSR_MONITOR_MIN_EV_PROXY")
            .ok().and_then(|v| v.parse::<f64>().ok()).unwrap_or(-0.02);

        if e.quote_count >= warmup_quotes && delta < min_ev {
            e.reject_count += 1;
            st.lmsr_health_degraded.store(true, Ordering::Relaxed);
        }

        e.last_updated = Some(Utc::now());
    }

    Ok(Json(LmsrQuoteResponse {
        p_lmsr,
        delta_lmsr: delta,
        delta_lmsr_z: delta_z,
        version: prm.version,
    }))
}


fn active_lmsr_record(map: &HashMap<String, LmsrParamsRecord>, now: DateTime<Utc>) -> Option<LmsrParamsRecord> {
    let mut best: Option<LmsrParamsRecord> = None;
    for v in map.values() {
        if v.effective_from <= now {
            match &best {
                None => best = Some(v.clone()),
                Some(cur) => {
                    if v.effective_from > cur.effective_from {
                        best = Some(v.clone());
                    }
                }
            }
        }
    }
    best
}

async fn get_lmsr_params_active(
    State(st): State<HttpApiState>,
) -> Result<Json<LmsrParamsRecord>, (StatusCode, String)> {
    let now = Utc::now();
    let map = st.lmsr_pending.read().await;
    active_lmsr_record(&map, now)
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "no active lmsr params".to_string()))
}

async fn set_lmsr_params(
    State(st): State<HttpApiState>,
    Json(req): Json<LmsrParamsRequest>,
) -> Result<Json<LmsrParamsRecord>, (StatusCode, String)> {
    if req.version.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "version required".to_string()));
    }
    if !req.b.is_finite() || req.b <= 0.0 {
        return Err((StatusCode::BAD_REQUEST, "b must be > 0".to_string()));
    }
    if !req.alpha.is_finite() || req.alpha <= 0.0 || req.alpha > 10.0 {
        return Err((StatusCode::BAD_REQUEST, "alpha out of range (0, 10]".to_string()));
    }
    // Activation rule: scheduled effective_from must align to bar boundary (30s grid)
    if req.effective_from.timestamp() % 30 != 0 {
        return Err((StatusCode::BAD_REQUEST, "effective_from must align to 30s bar boundary".to_string()));
    }

    let rec = LmsrParamsRecord {
        alpha: req.alpha,
        b: req.b,
        version: req.version.clone(),
        effective_from: req.effective_from,
        trained_at: req.trained_at,
        train_window: req.train_window,
        notes: req.notes,
        status: if req.effective_from > Utc::now() { "pending".to_string() } else { "effective_now".to_string() },
        stored_at: Utc::now(),
    };

    st.lmsr_pending
        .write()
        .await
        .insert(rec.version.clone(), rec.clone());

    {
        let mut app = st.app_state.write().await;
        app.lmsr_params.insert(
            rec.version.clone(),
            LmsrRuntimeParam {
                alpha: rec.alpha,
                b: rec.b,
                version: rec.version.clone(),
                effective_from: rec.effective_from,
            },
        );
    }

    if rec.effective_from <= Utc::now() {
        *st.lmsr_last_good_version.write().await = Some(rec.version.clone());
    }

    {
        let pending = st.lmsr_pending.read().await;
        let last_good = st.lmsr_last_good_version.read().await;
        persist_lmsr_state(&st.lmsr_state_file, &pending, &last_good);
    }

    Ok(Json(rec))
}


async fn trigger_lmsr_health(
    State(st): State<HttpApiState>,
    Json(req): Json<LmsrHealthTriggerRequest>,
) -> Result<Json<LmsrHealthTriggerResponse>, (StatusCode, String)> {
    st.lmsr_health_degraded.store(req.degraded, Ordering::Relaxed);

    let last_good = st.lmsr_last_good_version.read().await.clone();
    let active_now = {
        let map = st.lmsr_pending.read().await;
        active_lmsr_record(&map, Utc::now()).map(|r| r.version)
    };

    if req.degraded {
        if let Some(v) = last_good.clone() {
            {
                let mut mm = st.lmsr_metrics.write().await;
                let e = mm.entry(v.clone()).or_insert_with(|| LmsrVersionMetrics { version: v.clone(), ..Default::default() });
                e.rollback_events += 1;
                e.last_updated = Some(Utc::now());
            }
            return Ok(Json(LmsrHealthTriggerResponse {
                ok: true,
                degraded: true,
                active_version: Some(v.clone()),
                last_good_version: Some(v),
                message: req.reason.unwrap_or_else(|| "health degraded: rollback to last_good_version requested".to_string()),
            }));
        }
        return Ok(Json(LmsrHealthTriggerResponse {
            ok: false,
            degraded: true,
            active_version: active_now,
            last_good_version: None,
            message: "health degraded but no last_good_version available".to_string(),
        }));
    }

    if let Some(v) = active_now.clone() {
        *st.lmsr_last_good_version.write().await = Some(v.clone());
    }

    Ok(Json(LmsrHealthTriggerResponse {
        ok: true,
        degraded: false,
        active_version: active_now,
        last_good_version: st.lmsr_last_good_version.read().await.clone(),
        message: "health restored; active version recorded as last_good_version".to_string(),
    }))
}


async fn presign_enqueue(
    State(st): State<HttpApiState>,
    Json(req): Json<PresignEnqueueRequest>,
) -> Result<Json<PresignQueueResponse>, (StatusCode, String)> {
    if req.market_id.trim().is_empty() || req.side.trim().is_empty() || req.version.trim().is_empty() || req.nonce.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "market_id/side/version/nonce required".to_string()));
    }
    if !req.reference_price.is_finite() || req.reference_price <= 0.0 {
        return Err((StatusCode::BAD_REQUEST, "reference_price must be > 0".to_string()));
    }
    if !req.notional.is_finite() || req.notional <= 0.0 {
        return Err((StatusCode::BAD_REQUEST, "notional must be > 0".to_string()));
    }
    let now = Utc::now();
    if req.expires_at <= now {
        return Err((StatusCode::BAD_REQUEST, "expires_at must be in the future".to_string()));
    }

    let ttl_s = req.expires_at.signed_duration_since(now).num_seconds();
    if !(2..=5).contains(&ttl_s) {
        return Err((StatusCode::BAD_REQUEST, "TTL must be between 2 and 5 seconds".to_string()));
    }

    let max_notional = std::env::var("PRESIGN_MAX_QUEUED_NOTIONAL")
        .ok().and_then(|v| v.parse::<f64>().ok()).unwrap_or(1000.0);

    let key = format!("{}|{}|{}|{}", req.market_id, req.side.to_uppercase(), req.version, req.nonce);
    let mut q = st.presign_queue.write().await;

    // prune expired first
    q.retain(|_, v| v.expires_at > now);

    let bucket_notional: f64 = q.values()
        .filter(|v| v.market_id == req.market_id && v.side.eq_ignore_ascii_case(&req.side) && v.version == req.version)
        .map(|v| v.notional)
        .sum();

    if bucket_notional + req.notional > max_notional {
        return Err((StatusCode::CONFLICT, format!("queued exposure cap exceeded: {:.2} > {:.2}", bucket_notional + req.notional, max_notional)));
    }

    q.insert(key.clone(), PresignedTxEnvelope {
        market_id: req.market_id,
        side: req.side.to_uppercase(),
        version: req.version,
        nonce: req.nonce,
        signed_tx: req.signed_tx,
        expires_at: req.expires_at,
        reference_price: req.reference_price,
        notional: req.notional,
        queued_at: now,
    });

    Ok(Json(PresignQueueResponse {
        ok: true,
        key,
        queue_size: q.len(),
        message: "queued".to_string(),
    }))
}

async fn presign_dequeue(
    State(st): State<HttpApiState>,
    Json(req): Json<PresignDequeueRequest>,
) -> Result<Json<PresignDequeueResponse>, (StatusCode, String)> {
    if !req.current_price.is_finite() || req.current_price <= 0.0 {
        return Err((StatusCode::BAD_REQUEST, "current_price must be > 0".to_string()));
    }

    let now = Utc::now();
    let drift_bps = req.max_price_drift_bps.unwrap_or(10.0).max(0.0);
    let mut q = st.presign_queue.write().await;
    q.retain(|_, v| v.expires_at > now);

    let mut candidates: Vec<(String, PresignedTxEnvelope)> = q.iter()
        .filter(|(_, v)| {
            v.market_id == req.market_id
                && v.side.eq_ignore_ascii_case(&req.side)
                && v.version == req.version
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    candidates.sort_by_key(|(_, v)| v.queued_at);

    for (k, v) in candidates {
        let drift = ((req.current_price - v.reference_price).abs() / v.reference_price) * 10_000.0;
        if drift <= drift_bps {
            q.remove(&k);
            return Ok(Json(PresignDequeueResponse {
                ok: true,
                message: "dequeued".to_string(),
                tx: Some(v),
            }));
        } else {
            // stale-price invalidation
            q.remove(&k);
        }
    }

    Ok(Json(PresignDequeueResponse {
        ok: false,
        message: "no valid presigned tx available".to_string(),
        tx: None,
    }))
}


async fn get_lmsr_metrics(
    State(st): State<HttpApiState>,
) -> Result<Json<LmsrMetricsResponse>, (StatusCode, String)> {
    let active_version = {
        let map = st.lmsr_pending.read().await;
        active_lmsr_record(&map, Utc::now()).map(|r| r.version)
    };
    let by_version = st.lmsr_metrics.read().await.values().cloned().collect::<Vec<_>>();

    Ok(Json(LmsrMetricsResponse {
        ok: true,
        active_version,
        degraded: st.lmsr_health_degraded.load(Ordering::Relaxed),
        by_version,
    }))
}

async fn sign_stub(
    State(st): State<HttpApiState>,
    Json(req): Json<SignRequest>,
) -> Result<Json<SignResponse>, (StatusCode, String)> {
    let t0 = Instant::now();
    info!(token_id=%req.token_id, side=%req.side, tif=%req.tif, "sign_start");

    let stage_timeout = Duration::from_secs(
        std::env::var("SIGN_STAGE_TIMEOUT_S").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(20)
    );

    let md = if let Some(v) = st.meta_cache.read().await.get(&req.token_id).cloned() {
        info!(elapsed_ms=%t0.elapsed().as_millis(), "sign_stage metadata_cache_hit");
        v
    } else {
        return Err((
            StatusCode::CONFLICT,
            "metadata_missing_seed_required: call /v1/metadata/seed (or /v1/metadata/resolve) before /v1/sign".to_string(),
        ));
    };

    let pk = std::env::var("POLYMARKET_WALLET_PRIVATE_KEY")
        .or_else(|_| std::env::var("POLYMARKET_PRIVATE_KEY"))
        .or_else(|_| std::env::var("PRIVATE_KEY"))
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "missing POLYMARKET_WALLET_PRIVATE_KEY env".to_string()))?;

    let signer = LocalSigner::from_str(&pk)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("invalid private key: {e}")))?
        .with_chain_id(Some(POLYGON));

    let cfg = ClobConfig::builder().use_server_time(true).build();
    let t_client = Instant::now();
    let base_client = ClobClient::new(&st.clob_base_url, cfg)
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("clob client init failed: {e}")))?;
    info!(elapsed_ms=%t_client.elapsed().as_millis(), total_ms=%t0.elapsed().as_millis(), "sign_stage clob_client_init");

    let t_auth = Instant::now();
    let client = timeout(
        stage_timeout,
        base_client.authentication_builder(&signer).authenticate(),
    )
    .await
    .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, format!("clob authenticate timed out after {}s", stage_timeout.as_secs())))?
    .map_err(|e| (StatusCode::BAD_GATEWAY, format!("clob authenticate failed: {e}")))?;
    info!(elapsed_ms=%t_auth.elapsed().as_millis(), total_ms=%t0.elapsed().as_millis(), "sign_stage clob_authenticate");

    let side = match req.side.to_ascii_uppercase().as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return Err((StatusCode::BAD_REQUEST, "side must be BUY|SELL".to_string())),
    };
    let tif = match req.tif.to_ascii_uppercase().as_str() {
        "FOK" => OrderType::FOK,
        "FAK" | "IOC" => OrderType::FAK,
        "GTD" => OrderType::GTD,
        _ => OrderType::GTC,
    };

    let px = Decimal::from_str(&req.price.to_string())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid price: {e}")))?;
    let sz = Decimal::from_str(&req.size.to_string())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid size: {e}")))?;

    let mut builder = client.limit_order()
        .token_id(req.token_id.clone())
        .order_type(tif)
        .price(px)
        .size(sz)
        .side(side);

    if matches!(tif, OrderType::GTD) {
        let exp = req.expiration_ts.unwrap_or(0);
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(exp, 0)
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "invalid expiration_ts".to_string()))?;
        builder = builder.expiration(dt);
    }

    let t_build = Instant::now();
    let order = timeout(stage_timeout, builder.build())
        .await
        .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, format!("build order timed out after {}s", stage_timeout.as_secs())))?
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("build order failed: {e}")))?;
    info!(elapsed_ms=%t_build.elapsed().as_millis(), total_ms=%t0.elapsed().as_millis(), "sign_stage build_order");

    let t_sign = Instant::now();
    let signed = timeout(stage_timeout, client.sign(&signer, order))
        .await
        .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, format!("sign timed out after {}s", stage_timeout.as_secs())))?
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("sign failed: {e}")))?;
    info!(elapsed_ms=%t_sign.elapsed().as_millis(), total_ms=%t0.elapsed().as_millis(), "sign_stage sign_order");

    let t_ser = Instant::now();
    let signed_json = serde_json::to_value(&signed)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("serialize signed order failed: {e}")))?;
    info!(elapsed_ms=%t_ser.elapsed().as_millis(), total_ms=%t0.elapsed().as_millis(), "sign_stage serialize_signed");

    info!(total_ms=%t0.elapsed().as_millis(), "sign_done");
    Ok(Json(SignResponse {
        ok: true,
        message: "signed".to_string(),
        metadata: md,
        request_echo: req,
        signed_order: Some(signed_json),
    }))
}
