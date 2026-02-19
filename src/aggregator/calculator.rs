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
