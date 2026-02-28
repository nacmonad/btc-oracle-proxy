use std::env;
use std::time::Duration;

use duckdb::{params, Connection};
use tracing::{info, warn};

#[derive(Debug)]
struct Candidate {
    condition_id: String,
    close_time: String,
    yes_mid: Option<f64>,
    no_mid: Option<f64>,
    yes_ts: Option<String>,
    no_ts: Option<String>,
}

fn open_db() -> anyhow::Result<Connection> {
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "../data/researcher.db".to_string());
    Ok(Connection::open(db_path)?)
}

fn resolve_once(conn: &mut Connection) -> anyhow::Result<usize> {
    // Resolve unresolved markets using post-close token mids from local pm_snapshots.
    // Heuristic: pick side with higher latest post-close mid in first 15m after close.
    let mut stmt = conn.prepare(
        r#"
        WITH c AS (
            SELECT condition_id, close_time, token_yes_id, token_no_id
            FROM pm_markets
            WHERE outcome IS NULL
              AND close_time IS NOT NULL
              AND try_cast(close_time AS TIMESTAMPTZ) <= now() - INTERVAL '20 seconds'
        ),
        m AS (
            SELECT
              c.condition_id,
              c.close_time,
              (
                SELECT s.mid_price
                FROM pm_snapshots s
                WHERE s.condition_id = c.condition_id
                  AND s.token_id = c.token_yes_id
                  AND try_cast(s.ts AS TIMESTAMPTZ) >= try_cast(c.close_time AS TIMESTAMPTZ)
                  AND try_cast(s.ts AS TIMESTAMPTZ) <= try_cast(c.close_time AS TIMESTAMPTZ) + INTERVAL '15 minutes'
                  AND s.mid_price IS NOT NULL
                ORDER BY try_cast(s.ts AS TIMESTAMPTZ) DESC
                LIMIT 1
              ) AS yes_mid,
              (
                SELECT s.mid_price
                FROM pm_snapshots s
                WHERE s.condition_id = c.condition_id
                  AND s.token_id = c.token_no_id
                  AND try_cast(s.ts AS TIMESTAMPTZ) >= try_cast(c.close_time AS TIMESTAMPTZ)
                  AND try_cast(s.ts AS TIMESTAMPTZ) <= try_cast(c.close_time AS TIMESTAMPTZ) + INTERVAL '15 minutes'
                  AND s.mid_price IS NOT NULL
                ORDER BY try_cast(s.ts AS TIMESTAMPTZ) DESC
                LIMIT 1
              ) AS no_mid,
              (
                SELECT CAST(s.ts AS VARCHAR)
                FROM pm_snapshots s
                WHERE s.condition_id = c.condition_id
                  AND s.token_id = c.token_yes_id
                  AND try_cast(s.ts AS TIMESTAMPTZ) >= try_cast(c.close_time AS TIMESTAMPTZ)
                  AND try_cast(s.ts AS TIMESTAMPTZ) <= try_cast(c.close_time AS TIMESTAMPTZ) + INTERVAL '15 minutes'
                  AND s.mid_price IS NOT NULL
                ORDER BY try_cast(s.ts AS TIMESTAMPTZ) DESC
                LIMIT 1
              ) AS yes_ts,
              (
                SELECT CAST(s.ts AS VARCHAR)
                FROM pm_snapshots s
                WHERE s.condition_id = c.condition_id
                  AND s.token_id = c.token_no_id
                  AND try_cast(s.ts AS TIMESTAMPTZ) >= try_cast(c.close_time AS TIMESTAMPTZ)
                  AND try_cast(s.ts AS TIMESTAMPTZ) <= try_cast(c.close_time AS TIMESTAMPTZ) + INTERVAL '15 minutes'
                  AND s.mid_price IS NOT NULL
                ORDER BY try_cast(s.ts AS TIMESTAMPTZ) DESC
                LIMIT 1
              ) AS no_ts
            FROM c
        )
        SELECT condition_id, CAST(close_time AS VARCHAR), yes_mid, no_mid, yes_ts, no_ts
        FROM m
        WHERE yes_mid IS NOT NULL AND no_mid IS NOT NULL
        "#,
    )?;

    let mut rows = stmt.query([])?;
    let mut candidates: Vec<Candidate> = Vec::new();
    while let Some(r) = rows.next()? {
        candidates.push(Candidate {
            condition_id: r.get::<usize, String>(0)?,
            close_time: r.get::<usize, String>(1)?,
            yes_mid: r.get::<usize, Option<f64>>(2)?,
            no_mid: r.get::<usize, Option<f64>>(3)?,
            yes_ts: r.get::<usize, Option<String>>(4)?,
            no_ts: r.get::<usize, Option<String>>(5)?,
        });
    }

    let tx = conn.transaction()?;
    let mut up = tx.prepare(
        r#"
        UPDATE pm_markets
        SET outcome = ?,
            winning_price = ?,
            resolved_at = ?,
            last_updated = now()
        WHERE condition_id = ?
          AND outcome IS NULL
        "#,
    )?;

    let mut updated = 0usize;
    for c in candidates {
        let (Some(yes), Some(no)) = (c.yes_mid, c.no_mid) else { continue };
        if (yes - no).abs() < 0.03 {
            continue; // too close/ambiguous, wait for cleaner post-close signal
        }
        let (outcome, winning_price, resolved_at) = if yes > no {
            ("YES", yes, c.yes_ts.clone())
        } else {
            ("NO", no, c.no_ts.clone())
        };

        let n = up.execute(params![
            outcome,
            winning_price,
            resolved_at,
            c.condition_id,
        ])?;
        if n > 0 {
            updated += 1;
            info!(condition_id=%c.condition_id, close_time=%c.close_time, outcome=%outcome, winning_price=%winning_price, "pm_market resolved from local post-close mids");
        }
    }

    drop(up);
    tx.commit()?;
    Ok(updated)
}

pub async fn run_local_outcome_loop(interval_ms: u64) {
    let mut conn = match open_db() {
        Ok(c) => c,
        Err(e) => {
            warn!(error=%e, "local outcome resolver failed to open DB");
            return;
        }
    };

    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(5_000)));
    ticker.tick().await;

    loop {
        ticker.tick().await;
        match resolve_once(&mut conn) {
            Ok(n) if n > 0 => info!(resolved=n, "local outcome resolver updated markets"),
            Ok(_) => {}
            Err(e) => warn!(error=%e, "local outcome resolver iteration failed"),
        }
    }
}
