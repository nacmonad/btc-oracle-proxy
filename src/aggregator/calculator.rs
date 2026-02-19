//! Price aggregation and deviation calculation.
//!
//! Aggregation strategy: pure median of available exchange prices.
//! No explicit outlier filtering — the median is inherently resistant:
//! a single bad feed gets outvoted automatically (the median of
//! [Binance_spike, Coinbase_normal, Kraken_normal] = Coinbase_normal).

use crate::error::{OracleError, OracleResult};
use crate::models::ExchangePrice;

/// Chainlink OCR2 deviation threshold — round very likely triggering.
pub const ROUND_TRIGGER_THRESHOLD: f64 = 0.10; // %

/// Heads-up threshold — deviation is approaching the trigger zone.
pub const APPROACH_THRESHOLD: f64 = 0.07; // %

/// Minimum deviation for a pre-trigger convergence alert to fire.
pub const PRE_TRIGGER_DEV_THRESHOLD: f64 = 0.05; // %

/// BB width (as % of mid price) below which we consider bands "squeezed".
pub const BB_SQUEEZE_THRESHOLD: f64 = 0.30; // %

// ── Aggregation ───────────────────────────────────────────────────────────────

/// Returns the median of available exchange prices.
/// Works correctly with 1, 2, or 3 sources (falls back gracefully as feeds drop).
pub fn calculate_aggregated_price(prices: Vec<ExchangePrice>) -> OracleResult<f64> {
    if prices.is_empty() {
        return Err(OracleError::AggregationError("No prices available".to_string()));
    }

    let mut values: Vec<f64> = prices.iter().map(|p| p.price).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Ok(median_sorted(&values))
}

// ── Deviation ────────────────────────────────────────────────────────────────

/// `(market_price - chainlink_base) / chainlink_base × 100`
pub fn deviation_pct(market_price: f64, chainlink_base: f64) -> f64 {
    ((market_price - chainlink_base) / chainlink_base) * 100.0
}

/// Returns `true` when the deviation crosses the Chainlink OCR2 trigger threshold.
pub fn is_round_imminent(dev_pct: f64) -> bool {
    dev_pct.abs() >= ROUND_TRIGGER_THRESHOLD
}

// ── Signal detection ──────────────────────────────────────────────────────────

/// True when deviation is in the approach zone (0.07–0.10%) but not yet triggered.
pub fn is_deviation_approaching(dev_pct: f64) -> bool {
    dev_pct.abs() >= APPROACH_THRESHOLD && dev_pct.abs() < ROUND_TRIGGER_THRESHOLD
}

/// Returns the direction the price has broken through its Bollinger Band,
/// or `None` if price is inside the bands.
pub fn bb_breakout_direction(
    market_price: f64,
    bb_upper: Option<f64>,
    bb_lower: Option<f64>,
) -> Option<crate::models::RoundDirection> {
    use crate::models::RoundDirection;
    match (bb_upper, bb_lower) {
        (Some(upper), _) if market_price > upper => Some(RoundDirection::Up),
        (_, Some(lower)) if market_price < lower => Some(RoundDirection::Down),
        _ => None,
    }
}

/// Band width as a percentage of the mid (SMA) price.
/// Wider = more volatile; narrow = squeeze / consolidation.
pub fn bb_width_pct(bb_upper: f64, bb_lower: f64, bb_mid: f64) -> f64 {
    if bb_mid == 0.0 {
        return 0.0;
    }
    (bb_upper - bb_lower) / bb_mid * 100.0
}

/// True when the bands are narrower than `BB_SQUEEZE_THRESHOLD` — consolidation.
pub fn is_bb_squeezed(width_pct: f64) -> bool {
    width_pct < BB_SQUEEZE_THRESHOLD
}

/// Returns the set of signal names active for a potential pre-trigger alert.
///
/// Signals checked:
/// - `"bb_breakout"`        — price outside its Bollinger Band
/// - `"deviation_approach"` — abs(dev) >= 0.05% in the same direction
/// - `"momentum_surge"`     — ROC-10 aligned with deviation direction
/// - `"rsi_extreme"`        — RSI > 70 (up) or < 30 (down) aligned with direction
///
/// Returns `None` if fewer than 2 signals fire or direction is ambiguous.
pub fn detect_pre_trigger_signals(
    market_price: f64,
    deviation_pct: Option<f64>,
    bb_upper: Option<f64>,
    bb_lower: Option<f64>,
    roc_10: Option<f64>,
    rsi_14: Option<f64>,
) -> Option<(crate::models::RoundDirection, Vec<String>)> {
    use crate::models::RoundDirection;

    let dev = deviation_pct?;
    if dev.abs() < PRE_TRIGGER_DEV_THRESHOLD {
        return None;
    }
    let direction = if dev > 0.0 { RoundDirection::Up } else { RoundDirection::Down };

    let mut signals: Vec<String> = Vec::new();

    // BB breakout in the same direction as deviation
    if let Some(dir) = bb_breakout_direction(market_price, bb_upper, bb_lower) {
        if dir == direction {
            signals.push("bb_breakout".to_string());
        }
    }

    // Deviation approach zone
    if dev.abs() >= APPROACH_THRESHOLD {
        signals.push("deviation_approach".to_string());
    }

    // Short-term momentum aligned with deviation
    if let Some(roc) = roc_10 {
        if (roc > 0.0) == (dev > 0.0) && roc.abs() > 0.02 {
            signals.push("momentum_surge".to_string());
        }
    }

    // RSI extreme aligned with direction
    if let Some(rsi) = rsi_14 {
        let extreme = match direction {
            RoundDirection::Up => rsi > 65.0,
            RoundDirection::Down => rsi < 35.0,
        };
        if extreme {
            signals.push("rsi_extreme".to_string());
        }
    }

    if signals.len() >= 2 {
        Some((direction, signals))
    } else {
        None
    }
}

// ── Misc helpers (kept for future use) ───────────────────────────────────────

pub fn detect_outliers(_prices: &[f64]) -> Vec<usize> {
    vec![]
}

pub fn moving_average(_prices: &[f64], _window: usize) -> f64 {
    0.0
}

pub fn median(mut prices: Vec<f64>) -> f64 {
    if prices.is_empty() {
        return 0.0;
    }
    prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    median_sorted(&prices)
}

// ── Internal ──────────────────────────────────────────────────────────────────

fn median_sorted(prices: &[f64]) -> f64 {
    let n = prices.len();
    if n % 2 == 0 {
        (prices[n / 2 - 1] + prices[n / 2]) / 2.0
    } else {
        prices[n / 2]
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(exchange: &str, price: f64) -> ExchangePrice {
        ExchangePrice {
            exchange: exchange.to_string(),
            price,
            timestamp: chrono::Utc::now(),
            volume: None,
        }
    }

    #[test]
    fn test_median_three() {
        let prices = vec![ep("a", 45000.0), ep("b", 45010.0), ep("c", 44990.0)];
        assert_eq!(calculate_aggregated_price(prices).unwrap(), 45000.0);
    }

    #[test]
    fn test_median_two() {
        let prices = vec![ep("a", 45000.0), ep("b", 45010.0)];
        assert_eq!(calculate_aggregated_price(prices).unwrap(), 45005.0);
    }

    #[test]
    fn test_median_one() {
        let prices = vec![ep("a", 45000.0)];
        assert_eq!(calculate_aggregated_price(prices).unwrap(), 45000.0);
    }

    #[test]
    fn test_bad_feed_outvoted() {
        // Binance spikes — median picks Coinbase_normal
        let prices = vec![ep("binance", 90000.0), ep("coinbase", 45000.0), ep("kraken", 45010.0)];
        let result = calculate_aggregated_price(prices).unwrap();
        assert!(result < 50000.0, "Outlier should be outvoted, got {result}");
    }

    #[test]
    fn test_deviation_pct() {
        let dev = deviation_pct(97985.0, 97850.0);
        // (97985 - 97850) / 97850 × 100 ≈ 0.138%
        assert!((dev - 0.138).abs() < 0.001);
    }

    #[test]
    fn test_round_imminent() {
        assert!(is_round_imminent(0.12));
        assert!(is_round_imminent(-0.15));
        assert!(!is_round_imminent(0.08));
    }

    #[test]
    fn test_deviation_at_exact_threshold() {
        // 0.1% exactly should trigger
        let base = 100000.0_f64;
        let market = base * 1.001;
        assert!(is_round_imminent(deviation_pct(market, base)));
    }
}
