use std::path::PathBuf;

use libsql::{Builder, Connection, Result, Transaction};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}

pub async fn get_connection() -> Result<Connection> {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    return db.connect();
}

// Function to initialize the database and create necessary tables
pub async fn initialize_db(conn: &Connection, bucket_id: &String) -> Result<()> {
    // Validate bucket_id to prevent SQL injection
    if !bucket_id.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(libsql::Error::Misuse(
            "Bucket name must be alphanumeric".to_string(),
        ));
    }

    // Construct the SQL statement with the table name
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS obj_{} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            object_id TEXT NOT NULL UNIQUE,
            path TEXT NOT NULL,
            size INTEGER
        )",
        bucket_id
    );

    // Execute the SQL statement
    conn.execute(&sql, ()).await.unwrap();

    Ok(())
}

// Function to start a transaction and return a Transaction object
pub async fn start_transaction(conn: &Connection) -> Result<Transaction> {
    Ok(conn.transaction().await.unwrap())
}

// Function to insert new metadata within a transaction
pub async fn insert_metadata(
    tx: &Transaction,
    object_id: &str,
    path: &PathBuf,
    size: i64,
    last_modified: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO objects (object_id, path, size, last_modified) VALUES (?, ?, ?, ?)",
        (
            object_id,
            path.clone().into_os_string().into_string().unwrap(),
            size,
            last_modified,
        ),
    )
    .await
    .unwrap();
    Ok(())
}

// Function to retrieve metadata by object_id
pub async fn get_metadata(conn: &Connection, path: &str) -> Result<Option<(i64, String)>> {
    let stmt = conn.prepare("SELECT size, last_modified FROM objects WHERE object_id = ?");
    let mut rows = stmt.await.unwrap().query(&[path]).await.unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let size: i64 = row.get(0)?;
        let last_modified: String = row.get(1)?;
        Ok(Some((size, last_modified)))
    } else {
        Ok(None)
    }
}

// Function to delete metadata by object_id within a transaction
pub async fn delete_metadata(tx: &Transaction, object_id: &str) -> Result<(u64)> {
    tx.execute("DELETE FROM objects WHERE object_id = ?", &[object_id])
        .await
}
