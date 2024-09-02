use std::{
    fmt::{self, format},
    path::PathBuf,
};

use rusqlite::{Connection, Result, Transaction};

// FIXME: Build a common error thing
type MetaError<T> = std::result::Result<T, MissUse>;

#[derive(Debug, Clone)]
struct MissUse;

impl fmt::Display for MissUse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Metadata creation missuse")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect() {}
}

pub fn get_connection() -> Result<Connection> {
    return Connection::open_in_memory();
}

pub fn init_db(conn: &Connection, bucket_id: &String) -> Result<()> {
    // Construct the SQL statement with the table name
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS obj_{} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bucket_id TEXT NOT NULL UNIQUE,
            path TEXT NOT NULL,
            size INTEGER
        )",
        bucket_id
    );
    let _ = conn.execute(&sql, ());
    Ok(())
}

pub fn start_transaction(conn: &mut Connection) -> Transaction {
    return conn.transaction().unwrap();
}

// Function to insert new metadata within a transaction
pub fn insert_metadata(tx: &Transaction, bucket_id: &str, path: &str, size: u32) -> Result<()> {
    let tbl = format!("INSERT INTO obj_{} ", bucket_id);
    tx.execute(
        &(tbl + "(bucket_id, path, size, last_modified) VALUES (?, ?, ?, ?)"),
        (bucket_id, path, size),
    )
    .unwrap();
    Ok(())
}

pub fn get_metadata(conn: &Connection, path: &str) -> Result<Option<(i64, String)>> {
    let stmt = conn.prepare("SELECT size, last_modified FROM objects WHERE bucket_id = ?");
    let mut binding = stmt.unwrap();
    let mut rows = binding.query(&[path]).unwrap();

    if let Some(row) = rows.next().unwrap() {
        let size: i64 = row.get(0)?;
        let last_modified: String = row.get(1)?;
        Ok(Some((size, last_modified)))
    } else {
        Ok(None)
    }
}

pub fn delete_metadata(tx: &Transaction, bucket_id: &str) -> Result<usize> {
    tx.execute("DELETE FROM objects WHERE bucket_id = ?", &[bucket_id])
}
