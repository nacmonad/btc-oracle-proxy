//! Momentum indicators: ROC, RSI

/// Rate of Change (ROC) - percentage change over N periods
pub fn roc(prices: &[f64], period: usize) -> Option<f64> {
    if prices.len() < period + 1 {
        return None;
    }

    let current = prices[prices.len() - 1];
    let previous = prices[prices.len() - 1 - period];
    
    if previous == 0.0 {
        return None;
    }

    Some((current - previous) / previous * 100.0)
}

/// Relative Strength Index (RSI)
/// RSI = 100 - (100 / (1 + RS))
/// where RS = Average Gain / Average Loss
pub fn rsi(prices: &[f64], period: usize) -> Option<f64> {
    if prices.len() < period + 1 {
        return None;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;

    // Calculate gains and losses
    for i in 1..=period {
        let change = prices[prices.len() - i] - prices[prices.len() - i - 1];
        if change > 0.0 {
            gains += change;
        } else {
            losses += -change;
        }
    }

    let avg_gain = gains / period as f64;
    let avg_loss = losses / period as f64;

    if avg_loss == 0.0 {
        return Some(100.0);
    }

    let rs = avg_gain / avg_loss;
    Some(100.0 - (100.0 / (1.0 + rs)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roc() {
        let prices = vec![100.0, 102.0, 104.0, 103.0, 105.0];
        let result = roc(&prices, 2);
        assert!(result.is_some());
        // ROC should be (105 - 104) / 104 * 100 ≈ 0.96%
        assert!((result.unwrap() - 0.96).abs() < 0.1);
    }

    #[test]
    fn test_rsi() {
        let prices = vec![44.0, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08];
        let result = rsi(&prices, 3);
        assert!(result.is_some());
    }
}
