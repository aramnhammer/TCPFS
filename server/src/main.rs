use std::{
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Result},
    join,
    net::{TcpListener, TcpStream},
};

/*
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
    async fn handle_client(mut stream: TcpStream) -> Result<()> {
        
        let mut command_type = [0; 1];
        stream.read_exact(&mut command_type).await?;

        let mut path_length_buf = [0; 4];
        stream.read_exact(&mut path_length_buf).await?;
        let path_length = u32::from_be_bytes(path_length_buf);

        let mut file_length_buf = [0; 4];
        stream.read_exact(&mut file_length_buf).await?;
        let file_length = u32::from_be_bytes(file_length_buf);

        let mut bucket_id_buf = [0; 16];
        stream.read_exact(&mut bucket_id_buf).await?;
        let bucket_id = uuid::Uuid::from_u128(u128::from_be_bytes(bucket_id_buf)).to_string();
        let con = meta::get_connection().await.unwrap();
        meta::initialize_db(&con, &bucket_id).await.unwrap();
        let trans = meta::start_transaction(&con).await.unwrap();

        let mut relative_path_buf = vec![0; path_length as usize];
        stream.read_exact(&mut relative_path_buf).await?;
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
        let file = OpenOptions::new()
            .create(true) // Create the file if it doesn't exist
            .append(true) // Append to the file instead of overwriting
            .open(destination.clone())
            .await?;
        let mut file = file;

        // Buffer to store each received message
        let mut buffer = Vec::new();
        loop {
            buffer.clear(); // Clear the buffer before each read

            // Read from the stream until a newline character ('\n') is found
            // This includes reading up to '\r\n' if present
            let bytes_read = reader.read_until(b'\n', &mut buffer).await?;
            meta::insert_metadata(
                &trans,
                bucket_id.as_str(),
                destination.as_os_str().to_str().unwrap(),
                file_length,
            )
            .await
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
            file.write_all(&buffer).await?;
            file.write_all(b"\n").await?; // Adds a newline in the file

            // Optionally, flush the file to ensure data is written to disk
            file.flush().await?;
        }

        Ok(())
    }
    fn handle_download() {}
    fn handle_upload() {}
    fn handle_delete() {}
    fn handle_list() {}
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("Server listening on port 8080");

    // let work_dir = meta::initialize_db(con, bucket_id)

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = RequestHandler::handle_client(stream).await {
                eprintln!("Error handling client: {:?}", e);
            }
        });
    }
}
