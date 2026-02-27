use super::client::BookLevel;

#[derive(Debug, Clone, Default)]
pub struct DerivedL2Metrics {
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mid_price: Option<f64>,
    pub spread: Option<f64>,
    pub bid_depth_1: Option<f64>,
    pub ask_depth_1: Option<f64>,

    pub bid_depth_5: Option<f64>,
    pub ask_depth_5: Option<f64>,
    pub bid_depth_10: Option<f64>,
    pub ask_depth_10: Option<f64>,
    pub depth_imbalance_5: Option<f64>,
    pub depth_imbalance_10: Option<f64>,
    pub slippage_100: Option<f64>,
    pub slippage_1000: Option<f64>,
    pub total_bid_levels: i32,
    pub total_ask_levels: i32,
}

pub fn derive_l2_metrics(bids: &[BookLevel], asks: &[BookLevel]) -> DerivedL2Metrics {
    let mut m = DerivedL2Metrics::default();

    let mut sbids = bids.to_vec();
    sbids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    let mut sasks = asks.to_vec();
    sasks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));

    m.total_bid_levels = sbids.len() as i32;
    m.total_ask_levels = sasks.len() as i32;

    if let Some(b) = sbids.first() {
        m.best_bid = Some(b.price);
        m.bid_depth_1 = Some(b.size * b.price);
    }
    if let Some(a) = sasks.first() {
        m.best_ask = Some(a.price);
        m.ask_depth_1 = Some(a.size * a.price);
    }
    if let (Some(bb), Some(ba)) = (m.best_bid, m.best_ask) {
        m.mid_price = Some((bb + ba) / 2.0);
        m.spread = Some(ba - bb);
    }

    m.bid_depth_5 = Some(depth_notional(&sbids, 5));
    m.ask_depth_5 = Some(depth_notional(&sasks, 5));
    m.bid_depth_10 = Some(depth_notional(&sbids, 10));
    m.ask_depth_10 = Some(depth_notional(&sasks, 10));

    m.depth_imbalance_5 = imbalance(m.bid_depth_5, m.ask_depth_5);
    m.depth_imbalance_10 = imbalance(m.bid_depth_10, m.ask_depth_10);

    m.slippage_100 = estimate_slippage_buy(&sasks, 100.0, m.best_ask);
    m.slippage_1000 = estimate_slippage_buy(&sasks, 1000.0, m.best_ask);

    m
}

fn depth_notional(levels: &[BookLevel], n: usize) -> f64 {
    levels.iter().take(n).map(|l| l.price * l.size).sum()
}

fn imbalance(bid: Option<f64>, ask: Option<f64>) -> Option<f64> {
    match (bid, ask) {
        (Some(b), Some(a)) if (b + a) > 0.0 => Some((b - a) / (b + a)),
        _ => None,
    }
}

fn estimate_slippage_buy(asks: &[BookLevel], usd_notional: f64, best_ask: Option<f64>) -> Option<f64> {
    let best = best_ask?;
    if asks.is_empty() || usd_notional <= 0.0 {
        return None;
    }

    let mut remaining = usd_notional;
    let mut acquired = 0.0;
    let mut spent = 0.0;

    for lvl in asks {
        if remaining <= 0.0 {
            break;
        }
        let lvl_notional = lvl.price * lvl.size;
        let take_notional = remaining.min(lvl_notional);
        let qty = take_notional / lvl.price;
        acquired += qty;
        spent += take_notional;
        remaining -= take_notional;
    }

    if acquired <= 0.0 {
        return None;
    }

    let vwap = spent / acquired;
    Some((vwap - best).max(0.0))
}
