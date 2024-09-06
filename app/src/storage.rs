use std::{fs, path::PathBuf};

use rusqlite::{self, params, Connection};

#[derive(Debug)]
pub struct Host {
    pub id: Option<i32>, // This will be None when inserting a new host
    pub host: Option<String>,
    pub port: Option<String>,
    pub bucket_id: Option<String>,
}

impl Default for Host {
    fn default() -> Self {
        Self {
            id: Some(999999999), // You can set the default value as needed
            host: Some(String::new()),
            port: Some(String::new()),
            bucket_id: Some(String::new()),
        }
    }
}

pub fn local_storage() -> Connection {
    let home_dir = PathBuf::from("~/.tcpfs-client");
    fs::create_dir_all(&home_dir).unwrap();
    let con = rusqlite::Connection::open(home_dir.join("storage.db")).unwrap();
    init_db(&con);
    return con;
}

pub fn init_db(con: &Connection) {
    con.execute(
        "
    CREATE TABLE IF NOT EXISTS hosts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host TEXT NOT NULL, 
    port TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    UNIQUE(host, port, bucket_id))",
        params![],
    )
    .unwrap();
}

pub fn create_host(con: &Connection, host: &Host) -> rusqlite::Result<()> {
    match con.execute(
        "INSERT INTO hosts (host, port, bucket_id) VALUES (?1, ?2, ?3)",
        &[&host.host, &host.port, &host.bucket_id],
    ) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(err, _)) if err.extended_code == 2067 => {
            // Handle unique constraint violation (SQLITE_CONSTRAINT_UNIQUE)
            println!("A host with this host/port/bucket_id already exists.");
            Err(rusqlite::Error::SqliteFailure(err, None))
        }
        Err(e) => Err(e),
    }
}

pub fn get_all_hosts(con: &Connection) -> rusqlite::Result<Vec<Host>> {
    let mut stmt = con.prepare("SELECT id, host, port, bucket_id FROM hosts")?;
    let host_iter = stmt.query_map([], |row| {
        Ok(Host {
            id: row.get(0)?,
            host: row.get(1)?,
            port: row.get(2)?,
            bucket_id: row.get(3)?,
        })
    })?;

    let mut hosts = Vec::new();
    for host in host_iter {
        hosts.push(host?);
    }
    Ok(hosts)
}

pub fn get_hosts_by_host(con: &Connection, host: &str) -> rusqlite::Result<Vec<Host>> {
    let mut stmt = con.prepare("SELECT id, host, port, bucket_id FROM hosts WHERE host = ?1")?;
    let host_iter = stmt.query_map([host], |row| {
        Ok(Host {
            id: row.get(0)?,
            host: row.get(1)?,
            port: row.get(2)?,
            bucket_id: row.get(3)?,
        })
    })?;

    let mut hosts = Vec::new();
    for host in host_iter {
        hosts.push(host?);
    }
    Ok(hosts)
}

pub fn get_hosts_by_bucket_id(con: &Connection, bucket_id: &str) -> rusqlite::Result<Vec<Host>> {
    let mut stmt =
        con.prepare("SELECT id, host, port, bucket_id FROM hosts WHERE bucket_id = ?1")?;
    let host_iter = stmt.query_map([bucket_id], |row| {
        Ok(Host {
            id: row.get(0)?,
            host: row.get(1)?,
            port: row.get(2)?,
            bucket_id: row.get(3)?,
        })
    })?;

    let mut hosts = Vec::new();
    for host in host_iter {
        hosts.push(host?);
    }
    Ok(hosts)
}
