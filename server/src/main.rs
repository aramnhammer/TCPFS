use meta_sqlite;
use std::{
    env,
    error::Error,
    fs::OpenOptions,
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
        working_dir: &String,
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
        working_dir: &String,
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
            .join("/")
            .join(bucket_id.clone())
            .join(relative_path.clone());
        let dest_parent = destination.parent().unwrap();
        std::fs::create_dir_all(dest_parent).unwrap();

        // Wrap the stream in a BufReader for efficient buffered reading
        let mut reader = BufReader::new(stream);

        // Open (or create) the output file in append mode
        let mut file = OpenOptions::new()
            .create(true) // Create the file if it doesn't exist
            .append(true) // Append to the file instead of overwriting
            .open(destination.clone())
            .unwrap();

        // Buffer to store each received message
        let mut buffer = Vec::new();
        loop {
            buffer.clear(); // Clear the buffer before each read

            // Read from the stream until a newline character ('\n') is found
            // This includes reading up to '\r\n' if present
            let bytes_read = reader.read_until(b'\n', &mut buffer).unwrap();
            meta_sqlite::insert_metadata(
                &trans,
                bucket_id.as_str(),
                destination.as_os_str().to_str().unwrap(),
                &file_length.to_string(),
            )
            .unwrap();

            if bytes_read == 0 {
                // No more data; the connection was closed
                println!("Connection closed by the peer");
                break;
            }

            // Check and remove the '\r\n' delimiter if present
            if buffer.ends_with(b"\r\n") {
                buffer.truncate(buffer.len() - 2); // Remove '\r\n'
            } else if buffer.ends_with(b"\n") {
                buffer.truncate(buffer.len() - 1); // Remove '\n'
            }

            // Optionally, convert the buffer to a String for processing
            // let message = String::from_utf8_lossy(&buffer);
            // println!("Received message: {}", message);

            // Write the buffer to the file, followed by a newline for separation
            file.write_all(&buffer)?;

            // // Optionally, flush the file to ensure data is written to disk
            // file.flush()?;
        }
        Ok(())
    }
    fn handle_delete() {}
    fn handle_list() {}
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let host = args[1].clone();
    let port = args[2].clone();
    let db_path = Some(args[3].clone());
    let working_dir = args[4].clone();
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(addr.clone())?;
    println!("Server listening on port {addr}, workdir: {working_dir}");

    loop {
        let (stream, _) = listener.accept()?;
        let db_path = db_path.clone();
        let working_dir = working_dir.clone();
        std::thread::spawn(move || {
            if let Err(e) = RequestHandler::handle_client(stream, db_path.as_ref(), &working_dir) {
                eprintln!("Error handling client: {:?}", e);
            }
        });
    }
}
