use std::{
    env,
    error::Error,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    time::SystemTime
};
use chrono::Datelike;
use chrono::Timelike;
use chrono::prelude::{DateTime, Utc};

use meta_sqlite;
use protocol;


fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let default_host = "127.0.01".to_string();
    let defualt_port = "8888".to_string();

    let host = args.get(1).unwrap_or(&default_host);
    let port = args.get(2).unwrap_or(&defualt_port);

    let default_common_dir = ".tcpfs_store";
    let default_db_path = env::current_dir()
        .unwrap()
        .join(&default_common_dir)
        .join("metadata.db")
        .to_string_lossy()
        .to_string();
    let db_path = args.get(3).unwrap_or(&default_db_path);
    fs::create_dir_all(PathBuf::from(db_path.clone()).parent().unwrap()).unwrap();

    let default_working_dir = env::current_dir()
        .unwrap()
        .join(&default_common_dir)
        .join("file_store")
        .to_string_lossy()
        .to_string();
    let working_dir: PathBuf = PathBuf::from(args.get(4).unwrap_or(&default_working_dir));

    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(addr.clone())?;

    println!("Server listening on port {addr}");

    // FIXME: get con initializes the db, when inevitably we re-work the db connection stuff (maybe with a MVCC DB)
    // we should refactor this to be clearer
    let _ = meta_sqlite::get_connection(Some(db_path.clone()));

    loop {
        let (stream, _) = listener.accept()?;
        let db_path = db_path.clone();
        let working_dir = working_dir.clone();
        std::thread::spawn(move || {
            if let Err(e) = protocol::RequestHandler::handle_client(stream, Some(&db_path), &working_dir) {
                eprintln!("Error handling client: {:?}", e);
            }
        });
    }
}
