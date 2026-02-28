use duckdb::Connection;

fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string());

    let conn = Connection::open(&db_path)?;

    println!("db_path={}", db_path);

    // Last 1h anchored to latest snapshot ts
    let latest_ts: String = conn.query_row(
        "SELECT CAST(MAX(try_cast(ts AS TIMESTAMPTZ)) AS VARCHAR) FROM pm_snapshots",
        [],
        |r| r.get(0),
    )?;
    println!("anchor_latest_ts={}", latest_ts);

    // Overall top-of-book unchanged ratio in last hour
    let overall: f64 = conn.query_row(
        r#"
        WITH x AS (
          SELECT token_id,
                 try_cast(ts AS TIMESTAMPTZ) AS ts,
                 best_bid, best_ask,
                 LAG(best_bid) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_bid,
                 LAG(best_ask) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_ask
          FROM pm_snapshots
          WHERE EPOCH(try_cast(ts AS TIMESTAMPTZ)) >= (
            SELECT MAX(EPOCH(try_cast(ts AS TIMESTAMPTZ))) - 3600 FROM pm_snapshots
          )
        )
        SELECT COALESCE(AVG(CASE WHEN best_bid = p_bid AND best_ask = p_ask THEN 1.0 ELSE 0.0 END),0.0)
        FROM x WHERE p_bid IS NOT NULL OR p_ask IS NOT NULL
        "#,
        [],
        |r| r.get(0),
    )?;
    println!("same_topbook_ratio_1h_overall={:.3}", overall);

    // Ratio by timeframe and time-to-close bucket (requires pm_markets)
    let mut stmt = conn.prepare(
        r#"
        WITH base AS (
          SELECT
            ps.token_id,
            try_cast(ps.ts AS TIMESTAMPTZ) AS ts,
            ps.best_bid, ps.best_ask,
            pm.timeframe,
            try_cast(pm.close_time AS TIMESTAMPTZ) AS close_ts
          FROM pm_snapshots ps
          LEFT JOIN pm_markets pm ON pm.condition_id = ps.condition_id
          WHERE EPOCH(try_cast(ps.ts AS TIMESTAMPTZ)) >= (
            SELECT MAX(EPOCH(try_cast(ts AS TIMESTAMPTZ))) - 3600 FROM pm_snapshots
          )
        ), lagged AS (
          SELECT
            token_id, ts, best_bid, best_ask, timeframe, close_ts,
            LAG(best_bid) OVER (PARTITION BY token_id ORDER BY ts) AS p_bid,
            LAG(best_ask) OVER (PARTITION BY token_id ORDER BY ts) AS p_ask,
            (EPOCH(close_ts) - EPOCH(ts)) AS secs_to_close
          FROM base
        )
        SELECT
          COALESCE(timeframe, '?') AS timeframe,
          CASE
            WHEN secs_to_close IS NULL THEN 'unknown'
            WHEN secs_to_close < 0 THEN 'closed'
            WHEN secs_to_close <= 300 THEN '0-5m'
            WHEN secs_to_close <= 900 THEN '5-15m'
            WHEN secs_to_close <= 3600 THEN '15-60m'
            ELSE '>60m'
          END AS bucket,
          COUNT(*) AS n_rows,
          AVG(CASE WHEN p_bid IS NOT NULL OR p_ask IS NOT NULL THEN CASE WHEN best_bid = p_bid AND best_ask = p_ask THEN 1.0 ELSE 0.0 END ELSE NULL END) AS same_ratio
        FROM lagged
        GROUP BY 1,2
        ORDER BY 1,2
        "#,
    )?;

    let mut rows = stmt.query([])?;
    println!("breakdown=timeframe,bucket,n_rows,same_ratio");
    while let Some(r) = rows.next()? {
        let tf: String = r.get(0)?;
        let bucket: String = r.get(1)?;
        let n_rows: i64 = r.get(2)?;
        let same_ratio: Option<f64> = r.get(3)?;
        println!("{},{},{},{}", tf, bucket, n_rows, same_ratio.map(|v| format!("{:.3}", v)).unwrap_or_else(|| "null".into()));
    }

    Ok(())
}
