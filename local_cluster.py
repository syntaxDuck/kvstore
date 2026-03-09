#!/usr/bin/env python3
"""
Local development script to run a 3-node Raft cluster.

Each node runs as a separate process on ports:
- Node 0: 8080
- Node 1: 8081
- Node 2: 8082

Usage:
    python local_cluster.py

Then in another terminal, run the client:
    python client.py
"""

import os
import signal
import subprocess
import sys
from pathlib import Path


NODES = [
    {"id": 0, "port": 8080},
    {"id": 1, "port": 8081},
    {"id": 2, "port": 8082},
]

PROJECT_DIR = Path(__file__).parent.resolve()


def get_python_executable() -> str:
    venv_python = PROJECT_DIR / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def main():
    print("=" * 60)
    print("Starting local 3-node Raft cluster")
    print("=" * 60)

    for node in NODES:
        data_dir = PROJECT_DIR / f"data/node{node['id']}"
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"  Node {node['id']}: port {node['port']}, data: {data_dir}")

    print()
    print("Starting nodes...")

    python_exe = get_python_executable()
    print(f"  Using Python: {python_exe}")

    processes: list[subprocess.Popen] = []

    for node in NODES:
        env = os.environ.copy()
        env["NODE_ID"] = str(node["id"])
        env["API_PORT"] = str(node["port"])
        env["LOCAL_DEV"] = "true"
        env["CLUSTER_SIZE"] = "3"
        env["BASE_PORT"] = "8080"

        proc = subprocess.Popen(
            [
                python_exe,
                "-c",
                f"""
import asyncio
import sys
sys.path.insert(0, '{PROJECT_DIR}')
from main import main
asyncio.run(main())
""",
            ],
            cwd=PROJECT_DIR,
            env=env,
        )
        processes.append(proc)
        print(f"  Node {node['id']} started (PID: {proc.pid})")

    print()
    print("All nodes started!")
    print("Press Ctrl+C to stop all nodes...")
    print()

    def cleanup(signum, frame):
        print("\nShutting down nodes...")
        for i, proc in enumerate(processes):
            proc.terminate()
            print(f"  Node {i} terminated")
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        for i, proc in enumerate(processes):
            proc.wait()
            if proc.returncode != 0:
                print(f"Node {i} exited with code {proc.returncode}")
    except KeyboardInterrupt:
        cleanup(signal.SIGINT, None)


if __name__ == "__main__":
    main()
