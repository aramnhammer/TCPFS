use std::{
    env,
    error::Error,
    fs,
    io::{Read, Write},
    net::{TcpStream},
    path::PathBuf,
    time::SystemTime
};
use chrono::Datelike;
use chrono::Timelike;
use chrono::prelude::{DateTime, Utc};


/*
COMMAND TYPES:
0x01 -> UPLOAD | this will create a 'bucket' automatically |
0x02 -> DOWNLOAD -> bytes
0x03 -> DELETE -> u64 (bytes freed)
0x04 -> LIST -> ARRAY[BUCKET_ID: UUID]
0x06 -> DELETE BUCKET -> u64 (bytes freed)
*/


pub struct RequestHandler;

impl RequestHandler {
    pub fn handle_client(
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
                Self::handle_download(stream, db_path, working_dir)?;
            }
            0x03 => {
                println!("DELETE command received");
                // Call your delete handling function here
                //handle_delete(&mut stream).await?;
            }
            0x04 => {
                println!("LIST command received");
                // Call your list handling function here
                Self::handle_list(stream, db_path).unwrap_err();
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


    fn iso8601_now() -> (String, (i32, u32, u32, u32, u32, u32, u32)) 
    {
        let dt: DateTime<Utc> = SystemTime::now().into();

        // ISO8601 formatted timestamp
        let iso8601 = format!("{}", dt.format("%+"));

        // Deconstructed parts: year, month, day, hour, minute, second
        let deconstructed = (
            dt.year(),       // Year (i32)
            dt.month(),      // Month (u32)
            dt.day(),        // Day (u32)
            dt.hour(),       // Hour (u32)
            dt.minute(),     // Minute (u32)
            dt.second(),     // Second (u32)
            dt.nanosecond()  // 
        );

        (iso8601, deconstructed)
    }

/*
LIST REQUEST:
header:
+----------------------+--------------------+----------------------+
|          0x02        | Path Length (32 bits)| bucket_id (128 bits)|
+----------------------+--------------------+----------------------+
+-----------------------------------------------------------------------------------------+
|        Relative Path (variable length)                                                  |
+-----------------------------------------------------------------------------------------+
LIST RESPONSE (REPEATING):

+------------------------------+--------------------------+--------------------+------------------------------------------+
|    1 byte (0==file, 1==dir)    | Path Length (32 bits) |  bucket_id (128 bits)     |    File/Dir size length ( 32 bits ) |
+------------------------------+--------------------------+----------------------+-----------------------------------------+
+----------------------+--------------------+----------------------+----------------------+
|                                Path
+----------------------+--------------------+----------------------+----------------------+
\r\n
*/
    fn handle_list(mut stream: TcpStream, db_path: Option<&String>) -> Result<(), Box<dyn Error>> 
    {
        let mut key_length_buf: [u8; 4] = [0; 4];
        stream.read_exact(&mut key_length_buf)?;
        let key_length = u32::from_be_bytes(key_length_buf);

        let mut bucket_id_buf = [0; 16];
        stream.read_exact(&mut bucket_id_buf)?;
        let bucket_id = uuid::Uuid::from_u128(u128::from_be_bytes(bucket_id_buf)).to_string();

        let mut key_buf = vec![0; key_length as usize];
        stream.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf).expect("Invalid UTF-8 in path");

        let con = meta_sqlite::get_connection(db_path.cloned()).unwrap();
        for obj in meta_sqlite::get_objects_in_path(&con, &bucket_id, &key)
            .unwrap()
            .into_iter()
        {
            stream.write_all(&obj.serialize().clone()).unwrap();
        }

        Ok(())
    }


/*
DOWNLOAD REQUEST:
header:
+----------------------+----------------------+----------------------+
|          0x02        | Key Length (32 bits) | bucket_id (128 bits) |
+----------------------+----------------------+----------------------+
+-----------------------------------------------------------------------------------------+
|                              Key (variable length)                                      |
+-----------------------------------------------------------------------------------------+
DOWNLOAD RESPONSE:
+-----------------------------------------------------------------------------------------+
|                              File Data (variable length)                                |
+-----------------------------------------------------------------------------------------+
*/
    fn handle_download(
        mut stream: TcpStream,
        db_path: Option<&String>,
        working_dir: &PathBuf,
    ) -> Result<(), Box<dyn Error>> 
    {
        let mut key_length_buf = [0; 4];
        stream.read_exact(&mut key_length_buf)?;
        let path_length = u32::from_be_bytes(key_length_buf);

        // FIXME: Lots of allocs happening here, probably could be done better
        let mut bucket_id_buf = [0; 16];
        stream.read_exact(&mut bucket_id_buf)?;
        let bucket_id = uuid::Uuid::from_u128(u128::from_be_bytes(bucket_id_buf)).to_string();

        let mut con = meta_sqlite::get_connection(db_path.cloned()).unwrap();
        let trans = meta_sqlite::start_transaction(&mut con); 

        let mut key_buf = vec![0; path_length as usize];
        stream.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf).expect("Invalid UTF-8 in path");

        let path: String = meta_sqlite::get_metadata_by_key(&trans,
                                                            bucket_id.as_str(),
                                                            &key).unwrap();

        let pb = PathBuf::new();
        let target = pb
            .join(path);
        match target.is_file() {
            true => std::io::copy(&mut fs::File::open(&target).unwrap(), &mut stream)?,
            false => Err("Invalid key path")?,
        };
        Ok(())
    }

/*
UPLOAD REQUEST:
header:
+----------------------+----------------------+----------------------+----------------------+
| Command Type (8 bits)| Key Length (32 bits) | File Length (32 bits)| bucket_id (128 bits) |
+----------------------+----------------------+----------------------+----------------------+
data:
+-----------------------------------------------------------------------------------------+
|                                 Key (variable length)                                   |
+-----------------------------------------------------------------------------------------+
|                              File Data (variable length)                                |
+-----------------------------------------------------------------------------------------+
*/
    fn handle_upload(
        mut stream: TcpStream,
        db_path: Option<&String>,
        working_dir: &PathBuf,
    ) -> Result<(), Box<dyn Error>> 
    {
        let mut key_length_buf = [0; 4];
        stream.read_exact(&mut key_length_buf)?;
        let key_length = u32::from_be_bytes(key_length_buf);

        let mut file_length_buf = [0; 4];
        stream.read_exact(&mut file_length_buf)?;
        let file_length = u32::from_be_bytes(file_length_buf);

        // FIXME: Lots of allocs happening here, probably could be done better
        let mut bucket_id_buf = [0; 16];
        stream.read_exact(&mut bucket_id_buf)?;
        let bucket_id = uuid::Uuid::from_u128(u128::from_be_bytes(bucket_id_buf)).to_string();

        let mut con = meta_sqlite::get_connection(db_path.cloned()).unwrap();
        let trans = meta_sqlite::start_transaction(&mut con);

        let mut key_buf = vec![0; key_length as usize];
        stream.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf).expect("Invalid UTF-8 in path");

        let (iso, parts) = Self::iso8601_now();
        println!("ISO8601: {}", iso);

        let pb = PathBuf::new();
        let destination = pb
            .join(working_dir)
            .join(bucket_id.clone())
            .join(parts.0.to_string())
            .join(parts.1.to_string())
            .join(parts.2.to_string())
            .join(parts.3.to_string())
            .join(parts.4.to_string())
            .join(parts.5.to_string())
            .join(parts.6.to_string())
            .join("file.data".to_string());
            
        let dest_parent = destination.parent().unwrap();
        std::fs::create_dir_all(dest_parent).unwrap();

        meta_sqlite::insert_metadata(
            &trans,
            bucket_id.as_str(),
            &key,
            destination.as_os_str().to_str().unwrap(),
            &file_length.to_string(),
            &iso
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
}
