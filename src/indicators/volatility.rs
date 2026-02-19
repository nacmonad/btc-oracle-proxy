//! Volatility indicators: Standard Deviation, Bollinger Bands

/// Calculates standard deviation of prices
pub fn standard_deviation(prices: &[f64], period: usize) -> Option<f64> {
    if prices.len() < period {
        return None;
    }

    let window = &prices[prices.len() - period..];
    let mean = window.iter().sum::<f64>() / period as f64;
    
    let variance = window
        .iter()
        .map(|&x| (x - mean).powi(2))
        .sum::<f64>() / period as f64;

    Some(variance.sqrt())
}

/// Bollinger Bands: SMA ± (StdDev × multiplier)
pub fn bollinger_bands(prices: &[f64], period: usize, std_multiplier: f64) -> Option<(f64, f64, f64)> {
    if prices.len() < period {
        return None;
    }

    let window = &prices[prices.len() - period..];
    let sma = window.iter().sum::<f64>() / period as f64;
    let std_dev = standard_deviation(prices, period)?;

    let upper = sma + (std_dev * std_multiplier);
    let lower = sma - (std_dev * std_multiplier);

    Some((upper, sma, lower))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_deviation() {
        let prices = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = standard_deviation(&prices, 3);
        assert!(result.is_some());
    }

    #[test]
    fn test_bollinger_bands() {
        let prices = vec![100.0, 101.0, 102.0, 101.0, 100.0, 101.0, 102.0];
        let result = bollinger_bands(&prices, 5, 2.0);
        assert!(result.is_some());
        let (upper, middle, lower) = result.unwrap();
        assert!(upper > middle);
        assert!(middle > lower);
    }
}
