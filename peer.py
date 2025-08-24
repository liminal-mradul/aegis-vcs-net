import json
import hashlib
import socket
import threading
import sys
import time
from datetime import datetime
import argparse

# Constants
BROADCAST_PORT = 9999
BROADCAST_INTERVAL = 5  # seconds
SYNC_INTERVAL = 10  # seconds
NETWORK_KEY = "aegis-mesh-001"  # Change this for separate private networks

# Utility functions

def compute_hash(commit):
    commit_str = json.dumps(commit, sort_keys=True)
    return hashlib.sha256(commit_str.encode()).hexdigest()

def load_history(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except:
        return []

def save_history(filename, history):
    with open(filename, 'w') as f:
        json.dump(history, f, indent=2)

# Node class

class AegisNode:
    def __init__(self, port):
        self.port = port
        self.history_file = f"history_{port}.json"
        self.history = load_history(self.history_file)
        self.lock = threading.Lock()
        self.peers = set()  # Set of (ip, port)
        self.stop_event = threading.Event()

        # Start server thread
        self.server_thread = threading.Thread(target=self.run_server, daemon=True)
        self.server_thread.start()

        # Start broadcast listener thread (LAN discovery)
        self.listener_thread = threading.Thread(target=self.listen_for_peers, daemon=True)
        self.listener_thread.start()

        # Start broadcast sender thread (LAN discovery)
        self.sender_thread = threading.Thread(target=self.broadcast_presence, daemon=True)
        self.sender_thread.start()

        # Start periodic sync thread
        self.sync_thread = threading.Thread(target=self.periodic_sync, daemon=True)
        self.sync_thread.start()

    def create_commit(self, message, author="dev"):
        parent_hash = self.history[-1]['commit_id'] if self.history else None
        commit = {
            "parent_id": parent_hash,
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "author": author,
            "message": message,
            "commit_id": None
        }
        commit["commit_id"] = compute_hash(commit)
        with self.lock:
            self.history.append(commit)
            save_history(self.history_file, self.history)
        print(f"Created commit: {commit['commit_id']}")

    def get_missing_commits(self, their_latest_hash):
        with self.lock:
            if not self.history:
                return []
            if their_latest_hash is None:
                return self.history
            idx = next((i for i, c in enumerate(self.history) if c['commit_id'] == their_latest_hash), -1)
            if idx == -1:
                return self.history
            return self.history[idx+1:]

    def add_peer(self, ip, port):
        if (ip == 'localhost' and port == self.port):
            return
        if (ip == self.get_local_ip() and port == self.port):
            return
        if (ip, port) not in self.peers:
            self.peers.add((ip, port))
            print(f"[Peer Discovery] Added peer: {ip}:{port}")

    def get_local_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    # UDP Broadcast sender
    def broadcast_presence(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        message = json.dumps({
            "network_key": NETWORK_KEY,
            "node_ip": self.get_local_ip(),
            "node_port": self.port
        }).encode()
        while not self.stop_event.is_set():
            sock.sendto(message, ('<broadcast>', BROADCAST_PORT))
            time.sleep(BROADCAST_INTERVAL)

    # UDP Broadcast listener
    def listen_for_peers(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', BROADCAST_PORT))
        while not self.stop_event.is_set():
            try:
                data, addr = sock.recvfrom(1024)
                msg = json.loads(data.decode())
                if msg.get("network_key") == NETWORK_KEY:
                    ip = msg.get("node_ip")
                    port = msg.get("node_port")
                    if ip and port:
                        self.add_peer(ip, port)
            except Exception as e:
                print(f"[Peer Discovery] Error parsing broadcast: {e}")

    # TCP Server to handle sync requests
    def handle_client(self, conn, addr):
        try:
            data = conn.recv(65536).decode()
            request = json.loads(data)
            their_latest = request.get('latest_commit_id')
            missing = self.get_missing_commits(their_latest)
            response = json.dumps(missing)
            conn.sendall(response.encode())
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('0.0.0.0', self.port))
            s.listen()
            print(f"Node listening on port {self.port}")
            while not self.stop_event.is_set():
                try:
                    conn, addr = s.accept()
                    threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
                except Exception as e:
                    print(f"Server error: {e}")

    def sync_with_peer(self, ip, port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((ip, port))
                their_latest = self.history[-1]['commit_id'] if self.history else None
                request = json.dumps({"latest_commit_id": their_latest})
                s.sendall(request.encode())
                data = s.recv(65536).decode()
                commits = json.loads(data)
                new_commits = 0
                with self.lock:
                    existing_hashes = set(c['commit_id'] for c in self.history)
                    for c in commits:
                        if c['commit_id'] not in existing_hashes:
                            self.history.append(c)
                            new_commits += 1
                    if new_commits:
                        save_history(self.history_file, self.history)
                if new_commits:
                    print(f"Synced {new_commits} new commits from {ip}:{port}")
        except Exception as e:
            print(f"Sync failed with {ip}:{port} - {e}")
            self.peers.discard((ip, port))

    def periodic_sync(self):
        while not self.stop_event.is_set():
            peers_snapshot = list(self.peers)
            for ip, port in peers_snapshot:
                self.sync_with_peer(ip, port)
            time.sleep(SYNC_INTERVAL)

    def print_history(self):
        with self.lock:
            print(f"\nCommit History (Node port {self.port}):")
            for c in self.history:
                print(f"- {c['commit_id']} | {c['timestamp']} | {c['author']} | {c['message']}")
            print("")

    def stop(self):
        self.stop_event.set()


def repl(node):
    print("Commands: commit <msg>, history, exit")
    while True:
        cmd = input(">>> ").strip()
        if cmd.startswith("commit "):
            msg = cmd[7:].strip()
            if msg:
                node.create_commit(msg)
            else:
                print("Please provide a commit message.")
        elif cmd == "history":
            node.print_history()
        elif cmd == "exit":
            print("Exiting...")
            node.stop()
            break
        else:
            print("Unknown command.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int)
    parser.add_argument("--bootstrap-ip", type=str, default=None)
    parser.add_argument("--bootstrap-port", type=int, default=None)
    args = parser.parse_args()

    node = AegisNode(args.port)
    if args.bootstrap_ip and args.bootstrap_port:
        node.add_peer(args.bootstrap_ip, args.bootstrap_port)

    repl(node)
