//! Rolling price history buffer

pub struct PriceHistory {
    prices: Vec<f64>,
    capacity: usize,
}

impl PriceHistory {
    pub fn new(capacity: usize) -> Self {
        Self {
            prices: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn add_price(&mut self, price: f64) {
        self.prices.push(price);
        if self.prices.len() > self.capacity {
            self.prices.remove(0);
        }
    }

    pub fn get_prices(&self) -> &[f64] {
        &self.prices
    }

    pub fn len(&self) -> usize {
        self.prices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.prices.is_empty()
    }

    pub fn clear(&mut self) {
        self.prices.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_history() {
        let mut history = PriceHistory::new(3);
        history.add_price(100.0);
        history.add_price(101.0);
        history.add_price(102.0);
        history.add_price(103.0); // Should remove first

        assert_eq!(history.len(), 3);
        assert_eq!(history.get_prices()[0], 101.0);
    }
}
