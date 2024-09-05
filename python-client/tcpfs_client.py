import struct
import uuid
import socket
import argparse


class UploadRequest:
    def __init__(self, command_type: int, relative_path: str, file_data: bytes, bucket_id: uuid.UUID):
        self.command_type = command_type
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
    command_type = 0x01

    # Create an UploadRequest instance
    upload_request = UploadRequest(command_type, args.key, file_data, bucket_id)

    # Send the upload request to the server
    send_upload_request(args.host, args.port, upload_request)


if __name__ == "__main__":
    main()
