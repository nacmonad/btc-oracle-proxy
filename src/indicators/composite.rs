//! Composite indicators combining multiple signals

use super::ema::ema;

/// MACD (Moving Average Convergence Divergence)
/// MACD = EMA12 - EMA26
/// Signal = EMA9(MACD)
pub fn macd(prices: &[f64], fast: usize, slow: usize, _signal: usize) -> Option<(f64, f64, f64)> {
    let ema_fast = ema(prices, fast)?;
    let ema_slow = ema(prices, slow)?;
    
    let macd_line = ema_fast - ema_slow;
    
    // For signal line, we'd need historical MACD values
    // For now, return approximation
    let signal_line = macd_line; // TODO: Calculate proper signal line
    let histogram = macd_line - signal_line;

    Some((macd_line, signal_line, histogram))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_macd() {
        let prices = vec![
            44.0, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 
            45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.00, 46.00,
            46.00, 46.00, 46.00, 46.00,
        ];
        let result = macd(&prices, 12, 26, 9);
        assert!(result.is_some());
    }
}
