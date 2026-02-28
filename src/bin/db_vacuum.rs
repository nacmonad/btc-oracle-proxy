use duckdb::Connection;

fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string());

    let before = std::fs::metadata(&db_path)?.len();
    let conn = Connection::open(&db_path)?;
    conn.execute_batch("VACUUM;")?;
    let after = std::fs::metadata(&db_path)?.len();

    println!("db_path={}", db_path);
    println!("size_before_bytes={}", before);
    println!("size_after_bytes={}", after);
    println!("size_before_mb={:.1}", before as f64 / (1024.0*1024.0));
    println!("size_after_mb={:.1}", after as f64 / (1024.0*1024.0));
    println!("reclaimed_mb={:.1}", (before as f64 - after as f64) / (1024.0*1024.0));
    Ok(())
}
