#!/usr/bin/env python3
import os
import sys
import signal
import subprocess
import socket
import time
from pathlib import Path

PORT = 8000

def is_port_in_use(port):
    """Check if port is in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def kill_existing_process(port):
    """Kill any process running on the specified port."""
    try:
        # Find process on the port
        result = subprocess.run(
            ['lsof', '-ti', f':{port}'],
            capture_output=True,
            text=True
        )
        if result.stdout.strip():
            pids = result.stdout.strip().split()
            for pid in pids:
                try:
                    os.kill(int(pid), signal.SIGKILL)
                    print(f"Killed process {pid}")
                    time.sleep(0.5)
                except ProcessLookupError:
                    pass
    except (FileNotFoundError, subprocess.CalledProcessError):
        # lsof not available, try fuser
        try:
            subprocess.run(['fuser', '-k', f'{port}/tcp'], capture_output=True)
            print(f"Killed process on port {port}")
            time.sleep(0.5)
        except FileNotFoundError:
            print(f"Warning: Could not find process on port {port}")

def serve():
    """Serve the web folder."""
    # Change to web directory
    web_dir = Path(__file__).parent / 'web'
    if not web_dir.exists():
        print(f"Error: {web_dir} does not exist")
        sys.exit(1)

    os.chdir(web_dir)
    print(f"Serving from {web_dir}")

    # Kill existing process if port is in use
    if is_port_in_use(PORT):
        print(f"Port {PORT} is in use, killing existing process...")
        kill_existing_process(PORT)
        time.sleep(1)

    print(f"Starting server on http://localhost:{PORT}")
    print("Press Ctrl+C to stop")

    try:
        # Python 3.7+
        subprocess.run([sys.executable, '-m', 'http.server', str(PORT)])
    except KeyboardInterrupt:
        print("\nServer stopped")
        sys.exit(0)

if __name__ == '__main__':
    serve()
