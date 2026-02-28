use duckdb::Connection;

fn q_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get::<usize, i64>(0)).unwrap_or(0)
}

fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string());

    let conn = Connection::open(&db_path)?;

    let total_snap = q_i64(&conn, "SELECT COUNT(*) FROM pm_snapshots");
    let total_lvl = q_i64(&conn, "SELECT COUNT(*) FROM pm_order_book_levels");

    // Redundant = same top-of-book as previous snapshot for the same token (global history)
    let redundant_snap = q_i64(
        &conn,
        r#"
        WITH x AS (
          SELECT
            token_id,
            best_bid, best_ask,
            LAG(best_bid) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_bid,
            LAG(best_ask) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_ask
          FROM pm_snapshots
        )
        SELECT COUNT(*)
        FROM x
        WHERE (p_bid IS NOT NULL OR p_ask IS NOT NULL)
          AND best_bid = p_bid AND best_ask = p_ask
        "#,
    );

    let redundant_snap_keys = q_i64(
        &conn,
        r#"
        WITH x AS (
          SELECT
            ts, condition_id, token_id,
            best_bid, best_ask,
            LAG(best_bid) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_bid,
            LAG(best_ask) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_ask
          FROM pm_snapshots
        )
        SELECT COUNT(*)
        FROM x
        WHERE (p_bid IS NOT NULL OR p_ask IS NOT NULL)
          AND best_bid = p_bid AND best_ask = p_ask
        "#,
    );

    let redundant_lvl = q_i64(
        &conn,
        r#"
        WITH red AS (
          SELECT DISTINCT ts, condition_id, token_id
          FROM (
            SELECT
              ts, condition_id, token_id,
              best_bid, best_ask,
              LAG(best_bid) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_bid,
              LAG(best_ask) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_ask
            FROM pm_snapshots
          ) z
          WHERE (p_bid IS NOT NULL OR p_ask IS NOT NULL)
            AND best_bid = p_bid AND best_ask = p_ask
        )
        SELECT COUNT(*)
        FROM pm_order_book_levels l
        JOIN red r
          ON l.ts = r.ts
         AND l.condition_id = r.condition_id
         AND l.token_id = r.token_id
        "#,
    );

    let db_size_bytes = std::fs::metadata(&db_path)?.len() as f64;
    let total_rows = (total_snap + total_lvl).max(1) as f64;
    let approx_bytes_per_row = db_size_bytes / total_rows;

    let candidate_rows = redundant_snap + redundant_lvl;
    let approx_reclaim_mb = (candidate_rows as f64 * approx_bytes_per_row) / (1024.0 * 1024.0);

    println!("db_path={}", db_path);
    println!("pm_snapshots_total={}", total_snap);
    println!("pm_order_book_levels_total={}", total_lvl);
    println!("redundant_snapshots_same_topbook={}", redundant_snap);
    println!("redundant_snapshot_keys_count={}", redundant_snap_keys);
    println!("redundant_levels_joined_to_redundant_snapshots={}", redundant_lvl);
    println!("candidate_rows_total={}", candidate_rows);
    println!("candidate_snapshots_pct={:.2}", (redundant_snap as f64 * 100.0) / (total_snap.max(1) as f64));
    println!("candidate_levels_pct={:.2}", (redundant_lvl as f64 * 100.0) / (total_lvl.max(1) as f64));
    println!("approx_reclaim_mb_before_vacuum={:.1}", approx_reclaim_mb);

    // last hour lens (anchored to latest ts)
    let last1h_red = q_i64(&conn, r#"
        WITH x AS (
          SELECT
            token_id,
            best_bid, best_ask,
            LAG(best_bid) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_bid,
            LAG(best_ask) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_ask
          FROM pm_snapshots
          WHERE EPOCH(try_cast(ts AS TIMESTAMPTZ)) >= (
            SELECT MAX(EPOCH(try_cast(ts AS TIMESTAMPTZ))) - 3600 FROM pm_snapshots
          )
        )
        SELECT COUNT(*) FROM x
        WHERE (p_bid IS NOT NULL OR p_ask IS NOT NULL)
          AND best_bid = p_bid AND best_ask = p_ask
    "#);
    let last1h_all = q_i64(&conn, r#"
        SELECT COUNT(*) FROM pm_snapshots
        WHERE EPOCH(try_cast(ts AS TIMESTAMPTZ)) >= (
          SELECT MAX(EPOCH(try_cast(ts AS TIMESTAMPTZ))) - 3600 FROM pm_snapshots
        )
    "#);
    println!("last1h_same_topbook_count={}", last1h_red);
    println!("last1h_snapshot_count={}", last1h_all);
    println!("last1h_same_topbook_ratio={:.3}", (last1h_red as f64) / (last1h_all.max(1) as f64));

    Ok(())
}
