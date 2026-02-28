use duckdb::Connection;

fn q1_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get::<usize, i64>(0)).unwrap_or(0)
}

fn q1_f64(conn: &Connection, sql: &str) -> f64 {
    conn.query_row(sql, [], |r| r.get::<usize, f64>(0)).unwrap_or(0.0)
}

fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string());

    let conn = Connection::open(&db_path)?;

    let snapshots = q1_i64(&conn, "SELECT COUNT(*) FROM pm_snapshots");
    let levels = q1_i64(&conn, "SELECT COUNT(*) FROM pm_order_book_levels");
    let src_live = q1_i64(&conn, "SELECT COUNT(*) FROM pm_snapshots WHERE source='live_rust'");

    let tok_count = q1_i64(&conn, "SELECT COUNT(DISTINCT token_id) FROM pm_snapshots");

    let min_epoch = q1_f64(&conn, "SELECT COALESCE(EPOCH(MIN(try_cast(ts AS TIMESTAMPTZ))), 0) FROM pm_snapshots");
    let max_epoch = q1_f64(&conn, "SELECT COALESCE(EPOCH(MAX(try_cast(ts AS TIMESTAMPTZ))), 0) FROM pm_snapshots");
    let span_secs = (max_epoch - min_epoch).max(1.0);

    let lvl_per_snap = if snapshots > 0 { levels as f64 / snapshots as f64 } else { 0.0 };
    let snap_per_sec_span = snapshots as f64 / span_secs;
    let lvl_per_sec_span = levels as f64 / span_secs;

    let snap_1h = q1_i64(&conn, "SELECT COUNT(*) FROM pm_snapshots WHERE try_cast(ts AS TIMESTAMPTZ) >= (SELECT MAX(try_cast(ts AS TIMESTAMPTZ)) FROM pm_snapshots) - INTERVAL '1 hour'");
    let lvl_1h = q1_i64(&conn, "SELECT COUNT(*) FROM pm_order_book_levels WHERE try_cast(ts AS TIMESTAMPTZ) >= (SELECT MAX(try_cast(ts AS TIMESTAMPTZ)) FROM pm_order_book_levels) - INTERVAL '1 hour'");

    let size_mb = std::fs::metadata(&db_path)?.len() as f64 / (1024.0*1024.0);

    println!("db_path={}", db_path);
    println!("db_size_mb={:.1}", size_mb);
    println!("pm_snapshots_total={}", snapshots);
    println!("pm_order_book_levels_total={}", levels);
    let min_ts: String = conn.query_row("SELECT COALESCE(CAST(MIN(try_cast(ts AS TIMESTAMPTZ)) AS VARCHAR), 'null') FROM pm_snapshots", [], |r| r.get(0)).unwrap_or_else(|_| "null".to_string());
    let max_ts: String = conn.query_row("SELECT COALESCE(CAST(MAX(try_cast(ts AS TIMESTAMPTZ)) AS VARCHAR), 'null') FROM pm_snapshots", [], |r| r.get(0)).unwrap_or_else(|_| "null".to_string());

    println!("pm_snapshots_live_rust_total={}", src_live);
    println!("pm_snapshots_min_ts={}", min_ts);
    println!("pm_snapshots_max_ts={}", max_ts);
    println!("distinct_tokens_total={}", tok_count);
    println!("capture_span_hours={:.2}", span_secs / 3600.0);
    println!("snapshots_per_sec_span={:.3}", snap_per_sec_span);
    println!("levels_per_sec_span={:.3}", lvl_per_sec_span);
    println!("levels_per_snapshot_total={:.2}", lvl_per_snap);
    println!("pm_snapshots_1h={}", snap_1h);
    println!("pm_order_book_levels_1h={}", lvl_1h);

    // rough projection assuming average row-size proportional to current total share
    let total_rows = snapshots + levels;
    if total_rows > 0 {
        let bytes_per_row = (size_mb * 1024.0 * 1024.0) / total_rows as f64;
        println!("approx_bytes_per_row={:.1}", bytes_per_row);
        let mb_per_hour_recent = (snap_1h + lvl_1h) as f64 * bytes_per_row / (1024.0 * 1024.0);
        println!("approx_mb_per_hour_recent={:.1}", mb_per_hour_recent);
    }

    // optional sanity: unchanged-top-book duplicates in last 1h by token
    let dup_ratio = q1_f64(&conn, r#"
        WITH x AS (
          SELECT token_id,
                 try_cast(ts AS TIMESTAMPTZ) AS ts,
                 best_bid, best_ask,
                 LAG(best_bid) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_bid,
                 LAG(best_ask) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_ask
          FROM pm_snapshots
          WHERE try_cast(ts AS TIMESTAMPTZ) >= (
            SELECT MAX(try_cast(ts AS TIMESTAMPTZ)) - INTERVAL '1 hour' FROM pm_snapshots
          )
        )
        SELECT COALESCE(AVG(CASE WHEN best_bid = p_bid AND best_ask = p_ask THEN 1.0 ELSE 0.0 END), 0.0)
        FROM x
        WHERE p_bid IS NOT NULL OR p_ask IS NOT NULL
    "#);
    println!("same_topbook_ratio_1h={:.3}", dup_ratio);

    Ok(())
}
