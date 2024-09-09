use meta_sqlite;
use std::{
    default, env,
    error::Error,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    thread::{sleep, spawn},
    time::Duration,
};

/*
COMMAND TYPES:
0x01 -> UPLOAD | this will create a 'bucket' automatically |
0x02 -> DOWNLOAD -> bytes
0x03 -> DELETE -> u64 (bytes freed)
0x04 -> LIST -> ARRAY[BUCKET_ID: UUID]
0x06 -> DELETE BUCKET -> u64 (bytes freed)
*/

/*
DOWNLOAD REQUEST:
header:
+----------------------+--------------------+----------------------+
|          0x02        | Path Length (4 byte)| bucket_id (128 bits)|
+----------------------+--------------------+----------------------+
+-----------------------------------------------------------------------------------------+
|        Relative Path (variable length)                                                  |
+-----------------------------------------------------------------------------------------+
DOWNLOAD RESPONSE:
+-----------------------------------------------------------------------------------------+
|                              File Data (variable length)                                |
+-----------------------------------------------------------------------------------------+
*/

/*
UPLOAD REQUEST:
header:
+----------------------+--------------------+----------------------+----------------------+
| Command Type (1 byte)| Path Length (4 byte)| File Length (4 byte)| bucket_id (128 bits) |
+----------------------+--------------------+----------------------+----------------------+
data:
+-----------------------------------------------------------------------------------------+
|        Relative Path (variable length)                                                  |
+-----------------------------------------------------------------------------------------+
|        Relative Path (variable length)                                                  |
+-----------------------------------------------------------------------------------------+
|                              File Data (variable length)                                |
+-----------------------------------------------------------------------------------------+
*/

struct RequestHandler;

impl RequestHandler {
    fn handle_client(
        mut stream: TcpStream,
        db_path: Option<&String>,
        working_dir: &PathBuf,
    ) -> Result<(), Box<dyn Error>> {
        // Buffer to hold the command type
        let mut command_type = [0; 1];

        // Read the first byte to determine the command type
        stream.read_exact(&mut command_type)?;

        // Match the command type and handle accordingly
        match command_type[0] {
            0x01 => {
                println!("UPLOAD command received");
                // Call your upload handling function here
                Self::handle_upload(stream, db_path, working_dir)?;
            }
            0x02 => {
                println!("DOWNLOAD command received");
                // Call your download handling function here
                //Self::handle_download(stream).await?;
            }
            0x03 => {
                println!("DELETE command received");
                // Call your delete handling function here
                //handle_delete(&mut stream).await?;
            }
            0x04 => {
                println!("LIST command received");
                // Call your list handling function here
                //handle_list(&mut stream).await?;
            }

            0x05 => {
                println!("DELETE BUCKET");
                // Call your list handling function here
                //handle_bucket_delete(&mut stream).await?;
            }
            _ => {
                println!("Unknown command received");
                // Handle unknown commands here, possibly returning an error or ignoring
            }
        }
        Ok(())
    }

    fn handle_download() {}

    fn handle_upload(
        mut stream: TcpStream,
        db_path: Option<&String>,
        working_dir: &PathBuf,
    ) -> Result<(), Box<dyn Error>> {
        let mut path_length_buf = [0; 4];
        stream.read_exact(&mut path_length_buf)?;
        let path_length = u32::from_be_bytes(path_length_buf);

        let mut file_length_buf = [0; 4];
        stream.read_exact(&mut file_length_buf)?;
        let file_length = u32::from_be_bytes(file_length_buf);

        // FIXME: Lots of allocs happening here, probably could be done better
        let mut bucket_id_buf = [0; 16];
        stream.read_exact(&mut bucket_id_buf)?;
        let bucket_id = uuid::Uuid::from_u128(u128::from_be_bytes(bucket_id_buf)).to_string();

        let mut con = meta_sqlite::get_connection(db_path.cloned()).unwrap();
        let trans = meta_sqlite::start_transaction(&mut con);

        let mut relative_path_buf = vec![0; path_length as usize];
        stream.read_exact(&mut relative_path_buf)?;
        let relative_path = String::from_utf8(relative_path_buf).expect("Invalid UTF-8 in path");

        let pb = PathBuf::new();
        let destination = pb
            .join(working_dir)
            .join(bucket_id.clone())
            .join(relative_path.clone());
        let dest_parent = destination.parent().unwrap();
        std::fs::create_dir_all(dest_parent).unwrap();
        meta_sqlite::insert_metadata(
            &trans,
            bucket_id.as_str(),
            destination.as_os_str().to_str().unwrap(),
            &file_length.to_string(),
        )?;

        // Open the destination file for writing
        let mut file = fs::File::create(destination)?;

        // Copy the bytes from the limited stream to the file
        std::io::copy(&mut stream.take(file_length.into()), &mut file)?;
        trans.commit()?;

        // Buffer to store each received message
        Ok(())
    }
    fn handle_delete() {}
    fn handle_list() {}
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let default_host = "127.0.01".to_string();
    let defualt_port = "8888".to_string();

    let host = args.get(1).unwrap_or(&default_host);
    let port = args.get(2).unwrap_or(&defualt_port);

    let default_db_path = env::current_dir()
        .unwrap()
        .join("metadata.db")
        .to_string_lossy()
        .to_string();
    let db_path = args.get(3).unwrap_or(&default_db_path);

    let default_working_dir = env::current_dir()
        .unwrap()
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
            if let Err(e) = RequestHandler::handle_client(stream, Some(&db_path), &working_dir) {
                eprintln!("Error handling client: {:?}", e);
            }
        });
    }
}
