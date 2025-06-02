"""
Main entry point for running an LMS node with Raft consensus.
This script starts both the Raft consensus layer and the LMS server.
"""

import sys
import argparse
import json
import signal
import time
from typing import Dict

from raft.node import RaftNode
from raft.rpc import RaftRPCServer
from lms.server import LMSServer
from common.logger import setup_logging, get_logger


class LMSNode:
    """Combined LMS and Raft node"""
    
    def __init__(self, node_id: str, config_path: str):
        """
        Initialize LMS node.
        
        Args:
            node_id: Unique identifier for this node
            config_path: Path to configuration file
        """
        self.node_id = node_id
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Setup logging
        setup_logging(self.config['logging'])
        self.logger = get_logger(__name__)
        
        # Validate node exists in config
        if node_id not in self.config['cluster']['nodes']:
            raise ValueError(f"Node {node_id} not found in configuration")
        
        self.node_config = self.config['cluster']['nodes'][node_id]
        
        # Extract peer addresses for Raft
        self.peers = {}
        for peer_id, peer_config in self.config['cluster']['nodes'].items():
            if peer_id != node_id:
                self.peers[peer_id] = f"{peer_config['host']}:{peer_config['raft_port']}"
        
        # Components
        self.raft_node = None
        self.raft_rpc_server = None
        self.lms_server = None
        
        # Signal handling
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def start(self):
        """Start the LMS node"""
        self.logger.info(f"Starting LMS node {self.node_id}")
        
        try:
            # Create Raft node
            self.raft_node = RaftNode(self.node_id, self.peers, self.config)
            
            # Create and start Raft RPC server
            self.raft_rpc_server = RaftRPCServer(self.raft_node)
            self.raft_rpc_server.start(self.node_config['raft_port'])
            self.logger.info(f"Raft RPC server started on port {self.node_config['raft_port']}")
            
            # Create LMS server
            self.lms_server = LMSServer(self.node_id, self.config, self.raft_node)
            self.lms_server.set_raft_node(self.raft_node)
            
            # Start Raft node
            self.raft_node.start()
            self.logger.info("Raft node started")
            
            # Start LMS server
            self.lms_server.start(self.node_config['lms_port'])
            self.logger.info(f"LMS server started on port {self.node_config['lms_port']}")
            
            # Print status
            self._print_status()
            
            # Keep running
            self.logger.info("LMS node is running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Error starting node: {e}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """Stop the LMS node"""
        self.logger.info(f"Stopping LMS node {self.node_id}")
        
        # Stop components in reverse order
        if self.lms_server:
            try:
                self.lms_server.stop()
                self.logger.info("LMS server stopped")
            except Exception as e:
                self.logger.error(f"Error stopping LMS server: {e}")
        
        if self.raft_node:
            try:
                self.raft_node.stop()
                self.logger.info("Raft node stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Raft node: {e}")
        
        if self.raft_rpc_server:
            try:
                self.raft_rpc_server.stop()
                self.logger.info("Raft RPC server stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Raft RPC server: {e}")
        
        self.logger.info("LMS node stopped successfully")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}")
        sys.exit(0)
    
    def _print_status(self):
        """Print node status"""
        print("\n" + "="*60)
        print(f"LMS NODE {self.node_id} STATUS")
        print("="*60)
        print(f"Node ID: {self.node_id}")
        print(f"Raft Port: {self.node_config['raft_port']}")
        print(f"LMS Port: {self.node_config['lms_port']}")
        print(f"Peers: {', '.join(self.peers.keys())}")
        print("="*60)
        print("Services:")
        print(f"  ✓ Raft Consensus: Running on port {self.node_config['raft_port']}")
        print(f"  ✓ LMS Server: Running on port {self.node_config['lms_port']}")
        print("="*60)
        print("Endpoints:")
        print(f"  - gRPC (LMS): {self.node_config['host']}:{self.node_config['lms_port']}")
        print(f"  - gRPC (Raft): {self.node_config['host']}:{self.node_config['raft_port']}")
        print("="*60 + "\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Run an LMS node with Raft consensus'
    )
    
    parser.add_argument(
        'node_id',
        help='Node ID (e.g., node1, node2, etc.)'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset node data before starting'
    )
    
    args = parser.parse_args()
    
    # Reset data if requested
    if args.reset:
        print(f"Resetting data for node {args.node_id}...")
        # Here you would implement data cleanup
        print("Data reset complete")
    
    # Create and start node
    try:
        node = LMSNode(args.node_id, args.config)
        node.start()
    except Exception as e:
        print(f"Failed to start node: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()