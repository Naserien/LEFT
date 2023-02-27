import os
import socket
import threading
import time
import zlib
import hashlib

BUFFER_SIZE = 4096


class FileSync:
    def __init__(self, host_ip, host_port, share_dir):
        self.host_ip = host_ip
        self.host_port = host_port
        self.share_dir = share_dir

    def start_sync(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host_ip, self.host_port))
            server_sock.listen()
            print(f"Listening on {self.host_ip}:{self.host_port}")
            while True:
                conn, addr = server_sock.accept()
                print(f"Connection from {addr}")
                threading.Thread(target=self.handle_request, args=(conn,)).start()

    def handle_request(self, conn):
        try:
            data = conn.recv(BUFFER_SIZE)
            if not data:
                return
            data = zlib.decompress(data).decode()
            if data.startswith("GET "):
                filename = data[4:]
                abs_filename = os.path.abspath(os.path.join(self.share_dir, filename))
                if os.path.isfile(abs_filename):
                    self.send_file(conn, abs_filename)
                elif os.path.isdir(abs_filename):
                    for root, dirs, files in os.walk(abs_filename):
                        for file in files:
                            abs_file = os.path.join(root, file)
                            self.send_file(conn, abs_file)
            elif data.startswith("FILE "):
                filename, sent_size, total_size, checksum = data.split()[1:]
                abs_filename = os.path.abspath(os.path.join(self.share_dir, filename))
                if os.path.isfile(abs_filename):
                    file_checksum = self.get_file_checksum(abs_filename)
                    if file_checksum == checksum:
                        print(f"{filename} already exists and is up to date")
                    else:
                        self.receive_file(conn, abs_filename, int(sent_size), int(total_size))
                else:
                    self.receive_file(conn, abs_filename, int(sent_size), int(total_size))
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            conn.close()

    def send_file(self, sock, filename):
        abs_filename = os.path.abspath(filename)
        total_size = os.path.getsize(abs_filename)
        sent_size = 0
        with open(abs_filename, "rb") as f:
            f.seek(sent_size)
            while sent_size < total_size:
                data = f.read(min(total_size - sent_size, BUFFER_SIZE))
                sent_size += len(data)
                checksum = self.get_data_checksum(data)
                msg = f"FILE {filename} {sent_size} {total_size} {checksum}"
                sock.send(zlib.compress(msg.encode()) + data)

    def receive_file(self, sock, filename, sent_size, total_size):
        abs_filename = os.path.abspath(filename)
        os.makedirs(os.path.dirname(abs_filename), exist_ok=True)
        with open(abs_filename, "ab") as f:
            f.seek(sent_size)
            while sent_size < total_size:
                data = sock.recv(min(total_size - sent_size, BUFFER_SIZE))
                if not data:
                    break
                f.write(data)
                sent_size += len(data)
            if sent_size == total_size:
                print(f"Received file: {filename}")
            else:
                print(f"File transfer interrupted: {filename}")

    def sync_with_peer(self, peer_ip, peer_port):
        while True:
            try:
                peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_sock.connect((peer_ip, peer_port))
                peer_sock.sendall(zlib.compress("GET /".encode()))
                while True:
                    data = peer_sock.recv(BUFFER_SIZE)
                    if not data:
                        break
                    data = zlib.decompress(data)
                    if data.startswith(b"FILE "):
                        data = data.decode()
                        filename, sent_size, total_size, checksum = data.split()[1:]
                        abs_filename = os.path.abspath(os.path.join(self.share_dir, filename))
                        if os.path.isfile(abs_filename):
                            file_checksum = self.get_file_checksum(abs_filename)
                            if file_checksum == checksum:
                                print(f"{filename} already exists and is up to date")
                            else:
                                peer_sock.sendall(zlib.compress(
                                    f"FILE {filename} {os.path.getsize(abs_filename)} {os.path.getsize(abs_filename)} {checksum}".encode()))
                                with open(abs_filename, "rb") as f:
                                    f.seek(int(sent_size))
                                    while True:
                                        data = f.read(BUFFER_SIZE)
                                        if not data:
                                            break
                                        peer_sock.sendall(zlib.compress(data))
                        else:
                            peer_sock.sendall(zlib.compress(f"FILE {filename} 0 {total_size} {checksum}".encode()))
                            self.receive_file(peer_sock, abs_filename, int(sent_size), int(total_size))
            except Exception as e:
                print(f"Error syncing with peer: {e}")
                time.sleep(10)

    def get_data_checksum(self, data):
        m = hashlib.md5()
        m.update(data)
        return m.hexdigest()

    def get_file_checksum(self, filename):
        m = hashlib.md5()
        with open(filename, "rb") as f:
            while True:
                data = f.read(BUFFER_SIZE)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()


if __name__ == "main":
    os.makedirs('share', exist_ok=True)
    fs = FileSync("localhost", 8000, "share")
    threading.Thread(target=fs.start_sync).start()
    threading.Thread(target=fs.sync_with_peer, args=("localhost", 8001)).start()
    threading.Thread(target=fs.sync_with_peer, args=("localhost", 8002)).start()