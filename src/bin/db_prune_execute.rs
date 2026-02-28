use duckdb::Connection;

fn q_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get::<usize, i64>(0)).unwrap_or(0)
}

fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string());

    let conn = Connection::open(&db_path)?;

    let before_snap = q_i64(&conn, "SELECT COUNT(*) FROM pm_snapshots");
    let before_lvl = q_i64(&conn, "SELECT COUNT(*) FROM pm_order_book_levels");

    conn.execute_batch(
        r#"
        BEGIN TRANSACTION;

        CREATE TEMP TABLE redundant_keys AS
        WITH z AS (
          SELECT
            ts, condition_id, token_id,
            best_bid, best_ask,
            LAG(best_bid) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_bid,
            LAG(best_ask) OVER (PARTITION BY token_id ORDER BY try_cast(ts AS TIMESTAMPTZ)) AS p_ask
          FROM pm_snapshots
        )
        SELECT DISTINCT ts, condition_id, token_id
        FROM z
        WHERE (p_bid IS NOT NULL OR p_ask IS NOT NULL)
          AND best_bid = p_bid AND best_ask = p_ask;

        DELETE FROM pm_order_book_levels l
        WHERE EXISTS (
          SELECT 1 FROM redundant_keys r
          WHERE r.ts = l.ts
            AND r.condition_id = l.condition_id
            AND r.token_id = l.token_id
        );

        DELETE FROM pm_snapshots s
        WHERE EXISTS (
          SELECT 1 FROM redundant_keys r
          WHERE r.ts = s.ts
            AND r.condition_id = s.condition_id
            AND r.token_id = s.token_id
        );

        COMMIT;
        "#,
    )?;

    let after_snap = q_i64(&conn, "SELECT COUNT(*) FROM pm_snapshots");
    let after_lvl = q_i64(&conn, "SELECT COUNT(*) FROM pm_order_book_levels");

    let deleted_snap = before_snap - after_snap;
    let deleted_lvl = before_lvl - after_lvl;

    println!("db_path={}", db_path);
    println!("before_snapshots={}", before_snap);
    println!("before_levels={}", before_lvl);
    println!("deleted_snapshots={}", deleted_snap);
    println!("deleted_levels={}", deleted_lvl);
    println!("after_snapshots={}", after_snap);
    println!("after_levels={}", after_lvl);

    Ok(())
}
