from enum import Enum
from pathlib import Path
import struct
from typing import Optional
import uuid
import socket
import argparse


class UploadRequest:
    def __init__(self, int, relative_path: str, file_data: bytes, bucket_id: uuid.UUID):
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
        self.command_type = 0x02
        self.path = path_from.encode("UTF-8") if path_from is not None else ''.encode("UTF-8")
        self.path_length = len(self.path)
        self.bucket_id = bucket_id
    
    def to_bytes(self) -> bytes:

        header_format = '>BII16s'
        header = struct.pack(header_format,
                             self.command_type,
                             self.path_length,
                             self.bucket_id.bytes
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
        try:
            while True:
                # Read the 1-byte file/dir indicator
                file_dir_indicator = s.recv(1)
                if not file_dir_indicator:
                    break  # End of stream

                file_dir_indicator = struct.unpack('B', file_dir_indicator)[0]

                # Read the 4-byte (32-bit) Path Length
                path_length_bytes = s.recv(4)
                if len(path_length_bytes) < 4:
                    break  # Incomplete message

                path_length = struct.unpack('>I', path_length_bytes)[0]  # '>I' is for big-endian unsigned int

                # Read the 16-byte (128-bit) bucket_id
                bucket_id_bytes = s.recv(16)
                if len(bucket_id_bytes) < 16:
                    break  # Incomplete message

                bucket_id = bucket_id_bytes.hex()

                # Read the 4-byte (32-bit) File/Dir size length
                size_length_bytes = s.recv(4)
                if len(size_length_bytes) < 4:
                    break  # Incomplete message

                size_length = struct.unpack('>I', size_length_bytes)[0]

                # Read the path
                path_bytes = s.recv(path_length)
                if len(path_bytes) < path_length:
                    break  # Incomplete message

                path = path_bytes.decode('utf-8')

                # Read the trailing "\r\n"
                separator = s.recv(2)
                if separator != b'\r\n':
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
        except:
            print("LIST command failed")

    return objects 

class Command(Enum):
    UPLOAD = '0x01'
    LIST = '0x02'
    

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="Send an upload request to a TCPFS server.")
    parser.add_argument("--host", type=str, required=True, help="The server IP or hostname to connect to.")
    parser.add_argument("--port", type=int, required=True, help="The port on the server to connect to.")
    parser.add_argument("--key", type=str, required=True, help="The key of the file being uploaded. "
                                                               "This can be a path on the server")
    parser.add_argument("--file", type=str, required=True, help="The path to the file to be uploaded.")
    parser.add_argument("--bucket", type=str, help="The bucket UUID (will be auto-generated if not provided).")

    args = parser.parse_args()

    # Read the file data
    try:
        with open(args.file, 'rb') as f:
            file_data = f.read()
    except FileNotFoundError:
        print(f"File not found: {args.file}")
        return

    # Generate or use provided bucket UUID
    if args.bucket:
        try:
            bucket_id = uuid.UUID(args.bucket)
        except ValueError:
            print(f"Invalid UUID format for bucket: {args.bucket}")
            return
    else:
        bucket_id = uuid.uuid4()

    # Command type is hardcoded to 0x01 for upload, can be changed if needed

    # Create an UploadRequest instance
    upload_request = UploadRequest(args.key, file_data, bucket_id)

    # Send the upload request to the server
    send_upload_request(args.host, args.port, upload_request)


if __name__ == "__main__":
    main()
