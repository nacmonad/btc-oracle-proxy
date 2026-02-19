//! Technical indicator calculations

pub mod ema;
pub mod momentum;
pub mod volatility;
pub mod composite;

use crate::models::IndicatorValues;

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
}
