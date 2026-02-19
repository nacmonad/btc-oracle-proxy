//! Exponential Moving Average (EMA) calculation

/// Calculates Exponential Moving Average
/// EMA = Price × α + EMA_prev × (1 - α)
/// where α = 2 / (N + 1)
pub fn ema(prices: &[f64], period: usize) -> Option<f64> {
    if prices.is_empty() || period == 0 {
        return None;
    }

    if prices.len() < period {
        return None; // Not enough data
    }

    let alpha = 2.0 / (period as f64 + 1.0);
    
    // Initialize with SMA of first 'period' prices
    let sma = prices[..period].iter().sum::<f64>() / period as f64;
    let mut ema_value = sma;

    // Calculate EMA for remaining prices
    for &price in &prices[period..] {
        ema_value = price * alpha + ema_value * (1.0 - alpha);
    }

    Some(ema_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ema_basic() {
        let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = ema(&prices, 2);
        assert!(result.is_some());
    }

    #[test]
    fn test_ema_insufficient_data() {
        let prices = vec![1.0];
        let result = ema(&prices, 5);
        assert!(result.is_none());
    }
}
