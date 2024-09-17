from enum import Enum
from pathlib import Path
import struct
from typing import Optional
import uuid
import socket
import argparse
import pdb


class UploadRequest:
    def __init__(self, relative_path: str, file_data: bytes, bucket_id: uuid.UUID):
        self.command_type = 0x01
        self.relative_path = relative_path.encode('utf-8')
        self.file_data = file_data
        self.path_length = len(self.relative_path)
        self.file_length = len(file_data)
        self.bucket_id = bucket_id

    def to_bytes(self) -> bytes:
        # Header: Command Type (1 byte), Path Length (4 bytes), File Length (4 bytes), bucket_id (16 bytes)
        header_format = '>BII16s'  # Big-endian: Command Type (1 byte), Path Length (4 bytes), File Length (4 bytes),
        # bucket_id (16 bytes)
        header = struct.pack(header_format,
                             self.command_type,
                             self.path_length,
                             self.file_length,
                             self.bucket_id.bytes)

        # Data: Relative Path (variable length) + File Data (variable length)
        data = self.relative_path + self.file_data

        return header + data
    

class ListRequest:
    """    
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
    """
    def __init__(self, path_from: Optional[str], bucket_id: uuid.UUID) -> None:
        self.command_type = 0x04
        self.path = path_from if path_from is not None else ''
        self.path_length = len(self.path)
        self.bucket_id = bucket_id
    
    def to_bytes(self) -> bytes:
        header_format = f'>BI16s{self.path_length}s'
        # Convert relative_path to bytes (assume it's a string)
        relative_path_bytes = self.path.encode('utf-8')
        header = struct.pack(header_format,
                             self.command_type,
                             self.path_length,
                             self.bucket_id.bytes,
                             relative_path_bytes
                             )
        return header


def send_upload_request(server_ip: str, server_port: int, upload_request: UploadRequest):
    # Convert the request to bytes
    request_bytes = upload_request.to_bytes()

    # Open a TCP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            # Connect to the server
            print(f"Connecting to {server_ip}:{server_port}...")
            s.connect((server_ip, server_port))

            # Send the upload request
            print("Sending upload request...")
            s.sendall(request_bytes)

            # Wait for a response (optional)
            response = s.recv(1024)
            print(f"Response from server: {response.decode()}")
        except Exception as e:
            print(f"Error occurred: {e}")

def list_bucket(server_ip: str, server_port: int, list_request: ListRequest):
    objects = {}
    request_bytes = list_request.to_bytes()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        print(f"Connecting to {server_ip}:{server_port}...")
        s.connect((server_ip, server_port))
        s.sendall(request_bytes)
        while True:
            # Read the 1-byte file/dir indicator
            print("fetching file indicator")
            file_dir_indicator = s.recv(1)
            print(f"got indicator: {file_dir_indicator}")

            # Read the 4-byte (32-bit) Path Length
            path_length_bytes = s.recv(4)
            if len(path_length_bytes) < 4:
                print("incomplete message: path_len_bytes")
                break  # Incomplete message

            path_length = struct.unpack('>I', path_length_bytes)[0]  # '>I' is for big-endian unsigned int

            # Read the 16-byte (128-bit) bucket_id
            bucket_id_bytes = s.recv(16)
            if len(bucket_id_bytes) < 16:
                print("incomplete message: bucket_id")
                break  # Incomplete message

            bucket_id = bucket_id_bytes.hex()

            # Read the 4-byte (32-bit) File/Dir size length
            size_length_bytes = s.recv(4)
            if len(size_length_bytes) < 4:
                print("incomplete message: size_len_bytes")
                break  # Incomplete message

            size_length = s.recv(size_length_bytes)

            # Read the path
            path_bytes = s.recv(path_length)
            if len(path_bytes) < path_length:
                print("incomplete message: path_bytes")
                break  # Incomplete message

            path = path_bytes.decode('utf-8')

            # Read the trailing "\r\n"
            separator = s.recv(2)
            if separator != b'\r\n':
                print("incomplete message: separator")
                break  # Incorrect format

            # Store the parsed response
            object = {path: {
                "type": "dir" if file_dir_indicator == 1 else "file",
                "size_length": size_length}
            }
            if bucket_id not in objects:
                objects[bucket_id] = [object]
            else:
                objects[bucket_id].append(object)

    return objects 

class Command(Enum):
    UPLOAD = '0x01'
    DOWNLOAD = '0x02'
    DELETE_OBJECT = '0x03'
    LIST = '0x04'
    DELETE_BUCKET = '0x05'



def send_download_request_and_receive_response(host, port, bucket_id, relative_path):
    # Command type for DOWNLOAD (1 byte)
    command_type = 0x02

    # Convert the relative path to bytes and calculate its length
    relative_path_bytes = relative_path.encode('utf-8')
    path_length = len(relative_path_bytes)

    # Construct the DOWNLOAD REQUEST manually as bytes
    request = bytearray()

    # Append the command type (1 byte)
    request.append(command_type)

    # Append the path length (4 bytes, big-endian)
    request += path_length.to_bytes(4, 'big')

    # Append the bucket_id (16 bytes)
    request += uuid.UUID(bucket_id).bytes

    # Append the relative path (variable length)
    request += relative_path_bytes

    # Send the request over the socket and read the response
    file_data = b""

    try:
        # Create a socket connection to the server
        with socket.create_connection((host, port)) as sock:
            # Send the request
            sock.sendall(request)

            # Receive the file data (variable length)
            buffer_size = 4096  # You can adjust the buffer size
            while True:
                chunk = sock.recv(buffer_size)
                if not chunk:
                    break
                file_data += chunk
    except Exception as e:
        print(f"An error occurred: {e}")

    return file_data


def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Send an upload request to a TCPFS server.")
    parser.add_argument("--host", type=str, required=True, help="The server IP or hostname to connect to.")
    parser.add_argument("--port", type=int, required=True, help="The port on the server to connect to.")
    
    subparsers = parser.add_subparsers(dest="command", help="tcpfs commands") 
    upload_parser = subparsers.add_parser(name="upload")
    upload_parser.add_argument("--key", type=str, required=True, help="The key of the file being uploaded. "
                                                               "This can be a path on the server")
    upload_parser.add_argument("--file", type=str, required=True, help="The path to the file to be uploaded.")
    upload_parser.add_argument("--bucket", type=str, help="The bucket UUID (will be auto-generated if not provided).")

    download_parser = subparsers.add_parser(name="download")
    download_parser.add_argument("--key", type=str, required=True, help="They key of the file being uploaded")
    download_parser.add_argument("--bucket", type=str, required=True, help="bucket id, UUID")
    download_parser.add_argument("--destination", type=str, required=True, help="Where to write the file")

    list_parser = subparsers.add_parser(name="list")
    list_parser.add_argument("--key", type=str, required=False, default=".")
    list_parser.add_argument("--bucket", type=str, required=True)

    args = parser.parse_args()
    if args.command == "upload":
        print("uploading file")
        # Read the file data
        try:
            with open(args.file, 'rb') as f:
                file_data = f.read()
        except FileNotFoundError:
            print(f"File not found: {args.file}")
            return

        # Generate or use provided bucket UUID
        if args.bucket:
            bucket_id = uuid.UUID(args.bucket)
        else:
            bucket_id = uuid.uuid4()
        print(bucket_id)

        # Command type is hardcoded to 0x01 for upload, can be changed if needed

        # Create an UploadRequest instance
        upload_request = UploadRequest(args.key, file_data, bucket_id)

        # Send the upload request to the server
        send_upload_request(args.host, args.port, upload_request)
    
    if args.command == "download":
        print("download file")
        ret = send_download_request_and_receive_response(args.host, args.port, args.bucket, args.key)
        with open(args.destination, 'wb') as f:
            f.write(ret)


    if args.command == "list":
        print("geting object list")
        print(args.bucket)
        bucket_id = uuid.UUID(args.bucket)
        print(bucket_id)
        lr = ListRequest(path_from=args.key, bucket_id=bucket_id)
        objects = list_bucket(args.host, args.port, lr)

if __name__ == "__main__":
    main()
