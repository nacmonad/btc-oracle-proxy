//! Technical indicator calculations

pub mod ema;
pub mod momentum;
pub mod volatility;
pub mod composite;

use crate::models::IndicatorValues;

/// Calculates all indicators for a given price history
pub fn calculate_all_indicators(prices: &[f64], config: &IndicatorConfig) -> IndicatorValues {
    IndicatorValues {
        ema_12: ema::ema(prices, config.ema_short_period),
        ema_26: ema::ema(prices, config.ema_long_period),
        ema_50: ema::ema(prices, 50),
        rsi_14: momentum::rsi(prices, config.rsi_period),
        momentum_10: momentum::roc(prices, 10),
        momentum_20: momentum::roc(prices, 20),
        volatility: volatility::standard_deviation(prices, config.volatility_period),
        bb_upper: None, // TODO: Implement Bollinger Bands
        bb_middle: None,
        bb_lower: None,
        macd: None, // TODO: Implement MACD
        macd_signal: None,
        macd_histogram: None,
    }
}

pub struct IndicatorConfig {
    pub ema_short_period: usize,
    pub ema_long_period: usize,
    pub rsi_period: usize,
    pub volatility_period: usize,
}
