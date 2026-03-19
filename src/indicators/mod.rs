//! Technical indicator calculations

pub mod ema;
pub mod momentum;
pub mod volatility;
pub mod composite;

use crate::models::IndicatorValues;

fn mro_percentile(prices: &[f64], lookback_bars: usize, rank_window: usize) -> Option<f64> {
    if prices.len() < lookback_bars + 2 {
        return None;
    }
    let mut rets: Vec<f64> = Vec::new();
    let start = lookback_bars;
    for i in start..prices.len() {
        let base = prices[i - lookback_bars];
        if base > 0.0 {
            rets.push((prices[i] / base - 1.0) * 100.0);
        }
    }
    if rets.len() < 20 {
        return None;
    }
    let w = rank_window.min(rets.len()).max(20);
    let slice = &rets[rets.len() - w..];
    let last = *slice.last()?;
    let mut less_eq = 0usize;
    for v in slice {
        if *v <= last { less_eq += 1; }
    }
    let pct = (less_eq as f64) / (slice.len() as f64);
    Some(pct * 200.0 - 100.0)
}

/// Calculates all indicators for a given price history
pub fn calculate_all_indicators(prices: &[f64], config: &IndicatorConfig) -> IndicatorValues {
    let bb = volatility::bollinger_bands(prices, config.volatility_period, 2.0);
    let macd = composite::macd(prices, config.ema_short_period, config.ema_long_period, 9);

    IndicatorValues {
        ema_12: ema::ema(prices, config.ema_short_period),
        ema_26: ema::ema(prices, config.ema_long_period),
        ema_50: ema::ema(prices, 50),
        rsi_14: momentum::rsi(prices, config.rsi_period),
        momentum_10: momentum::roc(prices, 10),
        momentum_20: momentum::roc(prices, 20),
        mro_5: mro_percentile(prices, config.mro_5_bars, config.mro_rank_window),
        mro_10: mro_percentile(prices, config.mro_10_bars, config.mro_rank_window),
        mro_15: mro_percentile(prices, config.mro_15_bars, config.mro_rank_window),
        volatility: volatility::standard_deviation(prices, config.volatility_period),
        bb_upper: bb.map(|b| b.0),
        bb_middle: bb.map(|b| b.1),
        bb_lower: bb.map(|b| b.2),
        macd: macd.map(|m| m.0),
        macd_signal: macd.map(|m| m.1),
        macd_histogram: macd.map(|m| m.2),
    }
}

pub struct IndicatorConfig {
    pub ema_short_period: usize,
    pub ema_long_period: usize,
    pub rsi_period: usize,
    pub volatility_period: usize,
    pub mro_5_bars: usize,
    pub mro_10_bars: usize,
    pub mro_15_bars: usize,
    pub mro_rank_window: usize,
}
