use std::{
    fmt::{self, format},
    path::PathBuf,
};

use rusqlite::{params, Connection, Error, Result, Transaction};
// FIXME: Build a common error thing

#[cfg(test)]
mod tests {

    use rusqlite::Error;

    use super::*;
    extern crate tempdir;

    fn init() -> Connection {
        let con = get_connection().unwrap();
        init_db(&con).unwrap();
        return con;
    }

    #[test]
    fn test_triggers() {
        let con = init();
        con.execute(
            "INSERT INTO objects (bucket_id, path, file_size) VALUES(?,?,?) ",
            params!["test_id", "/some/file/path.txt", 1024],
        )
        .unwrap();
        let mut stmt = con
            .prepare("SELECT bucket_id, total_size FROM buckets WHERE bucket_id ='test_id'")
            .unwrap();
        let result: (String, i64) = stmt
            .query_row([], |row| {
                Ok((
                    row.get(0)?, // bucket_id
                    row.get(1)?, // total_size
                ))
            })
            .unwrap();

        assert_eq!(result.0, "test_id");
        assert_eq!(result.1, 1024);

        con.execute(
            "INSERT INTO objects (bucket_id, path, file_size) VALUES(?,?,?) ",
            params!["test_id", "/some/file/path2.txt", 1024],
        )
        .unwrap();

        let mut stmt = con
            .prepare("SELECT bucket_id, total_size FROM buckets WHERE bucket_id ='test_id'")
            .unwrap();
        let result: (String, i64) = stmt
            .query_row([], |row| {
                Ok((
                    row.get(0)?, // bucket_id
                    row.get(1)?, // total_size
                ))
            })
            .unwrap();

        assert_eq!(result.0, "test_id");
        assert_eq!(result.1, 2048);
    }

    #[test]
    fn test_insert_and_delete() {
        let mut con = init();
        let tx = start_transaction(&mut con);
        insert_metadata(&tx, "testid", "/path", "1024").unwrap();
        tx.commit().unwrap();
        let result: (String, String) = con
            .prepare("SELECT bucket_id, path FROM objects WHERE bucket_id='testid'")
            .unwrap()
            .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        assert_eq!(
            (result.0, result.1),
            ("testid".to_string(), "/path".to_string())
        );
        let bucket_total_size: i32 = con
            .prepare("SELECT total_size FROM buckets WHERE bucket_id = 'testid'")
            .unwrap()
            .query_row([], |row| row.get(0))
            .unwrap();

        assert_eq!(bucket_total_size, 1024);

        let tx = start_transaction(&mut con);
        delete_metadata(&tx, "testid", "/path").unwrap();
        tx.commit().unwrap();

        let bucket_total_size: i32 = con
            .prepare("SELECT total_size FROM buckets WHERE bucket_id = 'testid'")
            .unwrap()
            .query_row([], |row| row.get(0))
            .unwrap();

        assert_eq!(bucket_total_size, 0);
    }
}

pub fn get_connection() -> Result<Connection> {
    let con = Connection::open_in_memory().unwrap();
    init_db(&con).unwrap();
    return Ok(con);
}

pub fn init_db(conn: &Connection) -> Result<()> {
    // Enable WAL mode
    conn.pragma_update(None, "journal_mode", &"WAL")?;
    // create object table
    conn.execute(
        " -- reference table for buckets (root dirs, and their metadata)
    CREATE TABLE IF NOT EXISTS buckets (
    bucket_id TEXT PRIMARY KEY,   -- UUID of the top-level folder
    total_size INTEGER DEFAULT 0
    );
    ",
        params![],
    )
    .unwrap();

    conn.execute(
        " -- reference table for objects on disk
    CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id TEXT NOT NULL,
    path TEXT,
    file_size INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bucket_id, path),
    FOREIGN KEY (bucket_id) REFERENCES buckets(bucket_id)
    ON DELETE CASCADE);
    ",
        params![],
    )
    .unwrap();

    conn.execute("-- Trigger to update the total size when a file reference is inserted, handle case if bucket does not exists (no other files there)
    CREATE TRIGGER IF NOT EXISTS update_total_size_after_insert
    AFTER INSERT ON objects 
    FOR EACH ROW
    BEGIN
        -- Insert bucket_id into objects if it does not exist, then update the total size
        INSERT INTO buckets (bucket_id, total_size)
        VALUES (NEW.bucket_id, NEW.file_size)
        ON CONFLICT(bucket_id)
        DO UPDATE SET total_size = total_size + NEW.file_size;
    END; 
    ", params![]).unwrap();

    conn.execute(
        "-- Trigger to update the total size when a file reference is updated
    CREATE TRIGGER IF NOT EXISTS update_total_size_after_update
    AFTER UPDATE OF file_size ON objects 
    FOR EACH ROW
    BEGIN
        UPDATE buckets 
        SET total_size = total_size - OLD.file_size + NEW.file_size
        WHERE bucket_id = NEW.bucket_id;
    END;
    ",
        params![],
    )
    .unwrap();

    conn.execute(
        "-- Trigger to update the total size when a file reference is deleted
    CREATE TRIGGER IF NOT EXISTS update_total_size_after_delete
    AFTER DELETE ON objects 
    FOR EACH ROW
    BEGIN
        UPDATE buckets 
        SET total_size = total_size - OLD.file_size
        WHERE bucket_id = OLD.bucket_id;
    END;
    ",
        params![],
    )
    .unwrap();
    Ok(())
}

pub fn start_transaction(conn: &mut Connection) -> Transaction {
    return conn.transaction().unwrap();
}

pub fn delete_metadata(tx: &Transaction, bucket_id: &str, path: &str) -> Result<usize, Error> {
    Ok(tx
        .execute(
            "DELETE FROM objects WHERE bucket_id = ? AND path = ?",
            &[bucket_id, path],
        )
        .unwrap())
}

pub fn insert_metadata(
    tx: &Transaction,
    bucket_id: &str,
    path: &str,
    size: &str,
) -> Result<usize, Error> {
    Ok(tx
        .execute(
            "INSERT INTO objects (bucket_id, path, file_size) VALUES(?,?,?)",
            &[bucket_id, path, size],
        )
        .unwrap())
}
