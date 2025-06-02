"""
Script to start the distributed LMS cluster.
Starts all Raft nodes, LMS servers, and the tutoring server.
"""

import os
import sys
import json
import time
import subprocess
import signal
from typing import Dict, List

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.logger import setup_logging, get_logger


def load_config() -> Dict:
    """Load configuration from config.json"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)


def start_lms_node(node_id: str, node_config: Dict, config: Dict) -> subprocess.Popen:
    """Start a single LMS node with Raft"""
    cmd = [
        sys.executable,
        "-c",
        f"""
import sys
sys.path.append('{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')
print('{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

import grpc
from concurrent import futures
import threading

from raft.node import RaftNode
from raft.rpc import RaftRPCServer
from lms.server import LMSServer
from common.logger import setup_logging

# Setup logging
config = {json.dumps(config)}
setup_logging(config['logging'])

# Extract peer addresses for Raft
peers = {{}}
for peer_id, peer_config in config['cluster']['nodes'].items():
    if peer_id != '{node_id}':
        peers[peer_id] = f"{{peer_config['host']}}:{{peer_config['raft_port']}}"

# Create Raft node
raft_node = RaftNode('{node_id}', peers, config)

# Create and start Raft RPC server
raft_rpc_server = RaftRPCServer(raft_node)
raft_rpc_server.start({node_config['raft_port']})

# Create LMS server
lms_server = LMSServer('{node_id}', config, raft_node)
lms_server.set_raft_node(raft_node)

# Start Raft node
raft_node.start()

# Start LMS server
lms_server.start({node_config['lms_port']})

print(f"Node {node_id} started - Raft: {node_config['raft_port']}, LMS: {node_config['lms_port']}")

# Keep running
try:
    while True:
        import time
        time.sleep(1)
except KeyboardInterrupt:
    print(f"\\nShutting down node {node_id}...")
    raft_node.stop()
    raft_rpc_server.stop()
    lms_server.stop()
"""
    ]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    return process


def start_tutoring_server(config: Dict) -> subprocess.Popen:
    """Start the tutoring server"""
    tutoring_config = config['cluster']['tutoring_server']
    
    cmd = [
        sys.executable,
        "-c",
        f"""
import sys
sys.path.append('{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}')

from tutoring.server import TutoringServer
from common.logger import setup_logging

# Setup logging
config = {json.dumps(config)}
setup_logging(config['logging'])

# Create and start tutoring server
server = TutoringServer(config)
server.start({tutoring_config['port']})

print(f"Tutoring server started on port {tutoring_config['port']}")

# Keep running
try:
    while True:
        import time
        time.sleep(1)
except KeyboardInterrupt:
    print("\\nShutting down tutoring server...")
    server.stop()
"""
    ]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    return process


def main():
    """Main function to start the cluster"""
    print("\n" + "="*60)
    print("STARTING DISTRIBUTED LMS CLUSTER")
    print("="*60)
    
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logging(config['logging'])
    logger = get_logger(__name__)
    
    # Store process handles
    processes = []
    
    try:
        # Start tutoring server first
        print("\n1. Starting Tutoring Server...")
        tutoring_process = start_tutoring_server(config)
        processes.append(("Tutoring Server", tutoring_process))
        time.sleep(2)  # Give it time to start
        
        # Start LMS nodes
        print("\n2. Starting LMS Nodes with Raft...")
        for node_id, node_config in config['cluster']['nodes'].items():
            print(f"   Starting {node_id}...")
            process = start_lms_node(node_id, node_config, config)
            processes.append((node_id, process))
            time.sleep(1)  # Stagger node starts
        
        print("\n" + "="*60)
        print("✅ CLUSTER STARTED SUCCESSFULLY!")
        print("="*60)
        
        print("\nCluster Status:")
        print("-" * 40)
        print("Tutoring Server:")
        print(f"  - Host: {config['cluster']['tutoring_server']['host']}")
        print(f"  - Port: {config['cluster']['tutoring_server']['port']}")
        print(f"  - Model: {config['cluster']['tutoring_server']['model']}")
        
        print("\nLMS Nodes:")
        for node_id, node_config in config['cluster']['nodes'].items():
            print(f"  {node_id}:")
            print(f"    - Raft Port: {node_config['raft_port']}")
            print(f"    - LMS Port: {node_config['lms_port']}")
        
        print("\n" + "="*60)
        print("Press Ctrl+C to stop the cluster")
        print("="*60)
        
        # Monitor processes
        while True:
            # Check if any process has died
            for name, process in processes:
                if process.poll() is not None:
                    print(f"\n⚠️  {name} has stopped unexpectedly!")
                    # Print any error output
                    stdout, stderr = process.communicate()
                    if stdout:
                        print(f"STDOUT: {stdout}")
                    if stderr:
                        print(f"STDERR: {stderr}")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\nShutting down cluster...")
        
        # Terminate all processes
        for name, process in processes:
            print(f"Stopping {name}...")
            process.terminate()
        
        # Wait for processes to terminate
        for name, process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"Force killing {name}...")
                process.kill()
        
        print("\n✅ Cluster stopped successfully!")
        
    except Exception as e:
        print(f"\n❌ Error starting cluster: {e}")
        
        # Clean up any started processes
        for name, process in processes:
            try:
                process.terminate()
            except:
                pass


if __name__ == "__main__":
    main()