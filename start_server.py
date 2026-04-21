"""
Start app.py as a silent background process (no console window).
Usage: python start_server.py [--restart]
"""
import subprocess
import sys
import time
import urllib.request
import argparse

PORT = 7888
CREATE_NO_WINDOW = 0x08000000


def kill_port(port):
    r = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if f":{port}" in line and "LISTENING" in line:
            pid = line.strip().split()[-1]
            subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
            print(f"Killed PID {pid} on port {port}")
            time.sleep(1)


def is_up(port, timeout=3):
    try:
        urllib.request.urlopen(f"http://localhost:{port}/api/markets", timeout=timeout)
        return True
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart", action="store_true", help="Kill existing server first")
    args = parser.parse_args()

    if args.restart or is_up(PORT, timeout=1):
        kill_port(PORT)

    log_path = "app.log"
    proc = subprocess.Popen(
        [sys.executable, "app.py"],
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        creationflags=CREATE_NO_WINDOW,
    )
    print(f"Started app.py (PID {proc.pid}), logging to {log_path}")

    for _ in range(15):
        time.sleep(1)
        if is_up(PORT):
            print(f"Server is up at http://localhost:{PORT}")
            return
    print("Warning: server did not respond within 15s, check app.log")


if __name__ == "__main__":
    main()
