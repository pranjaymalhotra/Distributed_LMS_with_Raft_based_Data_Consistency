"""
Visual Raft cluster monitor.
Shows real-time leader election, log replication, and cluster health.
"""

import os
import sys
import time
import json
import grpc
import threading
from datetime import datetime
from typing import Dict, List, Optional
import curses

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raft import raft_pb2, raft_pb2_grpc

from common.logger import get_logger

logger = get_logger(__name__)


class RaftMonitor:
    """Visual monitor for Raft cluster"""
    
    def __init__(self, config_path: str = "config.json"):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.nodes = {}
        self.node_states = {}
        self.last_update = {}
        self.events = []
        self.max_events = 20
        self.running = True
        
        # Initialize node connections
        self._init_connections()
    
    def _init_connections(self):
        """Initialize gRPC connections to all nodes"""
        for node_id, node_config in self.config['cluster']['nodes'].items():
            address = f"{node_config['host']}:{node_config['raft_port']}"
            try:
                channel = grpc.insecure_channel(address)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                self.nodes[node_id] = {
                    'stub': stub,
                    'address': address,
                    'config': node_config
                }
            except Exception as e:
                logger.error(f"Failed to connect to {node_id}: {e}")
    
    def _get_node_status(self, node_id: str) -> Optional[Dict]:
        """Get status of a single node"""
        if node_id not in self.nodes:
            return None
        
        try:
            stub = self.nodes[node_id]['stub']
            request = raft_pb2.HealthCheckRequest(node_id=node_id)
            response = stub.HealthCheck(request, timeout=1)
            
            return {
                'healthy': response.healthy,
                'state': response.state,
                'term': response.current_term,
                'leader_id': response.leader_id,
                'timestamp': time.time()
            }
        except grpc.RpcError:
            return {
                'healthy': False,
                'state': 'OFFLINE',
                'term': 0,
                'leader_id': '',
                'timestamp': time.time()
            }
    
    def _update_all_nodes(self):
        """Update status for all nodes"""
        for node_id in self.nodes:
            old_state = self.node_states.get(node_id, {})
            new_state = self._get_node_status(node_id)
            
            if new_state:
                self.node_states[node_id] = new_state
                self.last_update[node_id] = time.time()
                
                # Detect state changes
                if old_state.get('state') != new_state['state']:
                    self._add_event(
                        f"{node_id} changed state: {old_state.get('state', 'UNKNOWN')} -> {new_state['state']}"
                    )
                
                # Detect term changes
                if old_state.get('term', 0) < new_state['term']:
                    self._add_event(
                        f"{node_id} advanced to term {new_state['term']}"
                    )
                
                # Detect new leader
                if (new_state['state'] == 'LEADER' and 
                    old_state.get('state') != 'LEADER'):
                    self._add_event(
                        f"🎉 {node_id} became LEADER in term {new_state['term']}"
                    )
    
    def _add_event(self, event: str):
        """Add event to event log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.events.append(f"[{timestamp}] {event}")
        
        # Keep only recent events
        if len(self.events) > self.max_events:
            self.events = self.events[-self.max_events:]
    
    def _draw_interface(self, stdscr):
        """Draw the monitoring interface"""
        curses.curs_set(0)  # Hide cursor
        stdscr.nodelay(1)   # Non-blocking input
        
        # Define colors
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)   # Leader
        curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # Candidate
        curses.init_pair(3, curses.COLOR_CYAN, curses.COLOR_BLACK)    # Follower
        curses.init_pair(4, curses.COLOR_RED, curses.COLOR_BLACK)     # Offline
        curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLACK)   # Normal text
        
        while self.running:
            try:
                # Clear screen
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                # Title
                title = "🚀 RAFT CLUSTER MONITOR 🚀"
                stdscr.addstr(0, (width - len(title)) // 2, title, curses.A_BOLD)
                stdscr.addstr(1, 0, "=" * width)
                
                # Node status section
                row = 3
                stdscr.addstr(row, 0, "NODE STATUS:", curses.A_BOLD)
                row += 2
                
                # Find current leader
                current_leader = None
                for node_id, state in self.node_states.items():
                    if state.get('state') == 'LEADER':
                        current_leader = node_id
                        break
                
                # Display each node
                for node_id in sorted(self.nodes.keys()):
                    state = self.node_states.get(node_id, {})
                    
                    # Choose color based on state
                    if not state.get('healthy', False):
                        color = curses.color_pair(4)
                        status_char = "❌"
                    elif state.get('state') == 'LEADER':
                        color = curses.color_pair(1)
                        status_char = "👑"
                    elif state.get('state') == 'CANDIDATE':
                        color = curses.color_pair(2)
                        status_char = "🗳️"
                    else:  # FOLLOWER
                        color = curses.color_pair(3)
                        status_char = "📡"
                    
                    # Node info
                    node_info = f"{status_char} {node_id:<8} "
                    node_info += f"State: {state.get('state', 'OFFLINE'):<10} "
                    node_info += f"Term: {state.get('term', 0):<4} "
                    
                    if state.get('leader_id'):
                        node_info += f"Leader: {state.get('leader_id')}"
                    
                    stdscr.addstr(row, 2, node_info, color)
                    row += 1
                
                # Cluster summary
                row += 1
                stdscr.addstr(row, 0, "-" * width)
                row += 1
                
                stdscr.addstr(row, 0, "CLUSTER SUMMARY:", curses.A_BOLD)
                row += 1
                
                # Count node states
                state_counts = {'LEADER': 0, 'FOLLOWER': 0, 'CANDIDATE': 0, 'OFFLINE': 0}
                max_term = 0
                
                for state in self.node_states.values():
                    state_name = state.get('state', 'OFFLINE')
                    if state_name in state_counts:
                        state_counts[state_name] += 1
                    max_term = max(max_term, state.get('term', 0))
                
                stdscr.addstr(row, 2, f"Current Term: {max_term}")
                row += 1
                stdscr.addstr(row, 2, f"Current Leader: {current_leader or 'None'}")
                row += 1
                stdscr.addstr(row, 2, 
                    f"Nodes: {state_counts['LEADER']} Leader, "
                    f"{state_counts['FOLLOWER']} Followers, "
                    f"{state_counts['CANDIDATE']} Candidates, "
                    f"{state_counts['OFFLINE']} Offline"
                )
                
                # Event log
                row += 2
                stdscr.addstr(row, 0, "-" * width)
                row += 1
                stdscr.addstr(row, 0, "EVENT LOG:", curses.A_BOLD)
                row += 1
                
                # Show recent events
                event_start = max(0, len(self.events) - (height - row - 2))
                for i, event in enumerate(self.events[event_start:]):
                    if row < height - 2:
                        # Highlight important events
                        if "became LEADER" in event:
                            stdscr.addstr(row, 2, event, curses.color_pair(1))
                        elif "OFFLINE" in event:
                            stdscr.addstr(row, 2, event, curses.color_pair(4))
                        else:
                            stdscr.addstr(row, 2, event)
                        row += 1
                
                # Instructions
                stdscr.addstr(height - 1, 0, 
                    "Press 'q' to quit | 'r' to refresh | Updates every 1s", 
                    curses.A_REVERSE)
                
                # Refresh screen
                stdscr.refresh()
                
                # Check for input
                key = stdscr.getch()
                if key == ord('q'):
                    self.running = False
                    break
                elif key == ord('r'):
                    self._update_all_nodes()
                
                # Auto-update
                time.sleep(1)
                self._update_all_nodes()
                
            except KeyboardInterrupt:
                self.running = False
                break
            except Exception as e:
                logger.error(f"Display error: {e}")
                time.sleep(1)
    
    def run(self):
        """Run the monitor"""
        # Initial update
        self._update_all_nodes()
        self._add_event("Monitor started")
        
        # Run with curses
        try:
            curses.wrapper(self._draw_interface)
        except KeyboardInterrupt:
            pass
        finally:
            print("\nMonitor stopped")


class RaftTester:
    """Interactive Raft testing tool"""
    
    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.nodes = {}
        self._init_connections()
    
    def _init_connections(self):
        """Initialize connections to nodes"""
        for node_id, node_config in self.config['cluster']['nodes'].items():
            # For testing, we need to interact with the actual processes
            self.nodes[node_id] = {
                'config': node_config,
                'process': None  # Will be set if we start/stop nodes
            }
    
    def kill_node(self, node_id: str):
        """Simulate node failure"""
        print(f"\n💀 Killing node {node_id}...")
        
        # In a real implementation, you would:
        # 1. Find the process running this node
        # 2. Send SIGTERM or SIGKILL
        
        # For now, we'll use a system command
        port = self.nodes[node_id]['config']['raft_port']
        os.system(f"lsof -ti:{port} | xargs kill -9 2>/dev/null")
        
        print(f"✅ Node {node_id} killed")
    
    def partition_node(self, node_id: str):
        """Simulate network partition"""
        print(f"\n🔌 Partitioning node {node_id}...")
        
        # In a real implementation, you would:
        # 1. Use iptables to block traffic
        # 2. Or modify the node's peer list
        
        print(f"⚠️  Network partition simulation not implemented")
        print("   In production, use iptables or network namespaces")
    
    def start_node(self, node_id: str):
        """Start a specific node"""
        print(f"\n🚀 Starting node {node_id}...")
        
        import subprocess
        
        cmd = [
            sys.executable,
            "lms/main.py",
            node_id,
            "--config", "config.json"
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        self.nodes[node_id]['process'] = process
        print(f"✅ Node {node_id} started")
    
    def run_interactive(self):
        """Run interactive testing menu"""
        print("\n" + "="*60)
        print("RAFT TESTING CONSOLE")
        print("="*60)
        
        while True:
            print("\nOptions:")
            print("1. Kill a node")
            print("2. Start a node")
            print("3. Partition a node")
            print("4. Show cluster status")
            print("5. Trigger election")
            print("0. Exit")
            
            choice = input("\nSelect option: ").strip()
            
            if choice == '0':
                break
            elif choice == '1':
                node_id = input("Node to kill (e.g., node1): ").strip()
                if node_id in self.nodes:
                    self.kill_node(node_id)
                else:
                    print("Invalid node ID")
            elif choice == '2':
                node_id = input("Node to start (e.g., node1): ").strip()
                if node_id in self.nodes:
                    self.start_node(node_id)
                else:
                    print("Invalid node ID")
            elif choice == '3':
                node_id = input("Node to partition (e.g., node1): ").strip()
                if node_id in self.nodes:
                    self.partition_node(node_id)
                else:
                    print("Invalid node ID")
            elif choice == '4':
                self._show_status()
            elif choice == '5':
                print("\n⚡ To trigger election, kill the current leader")
                print("   The remaining nodes will elect a new leader")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Raft cluster monitoring and testing tool'
    )
    
    parser.add_argument(
        'mode',
        choices=['monitor', 'test'],
        help='Run mode: monitor for visual monitoring, test for interactive testing'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'monitor':
        print("Starting Raft Monitor...")
        print("Make sure the cluster is running!")
        time.sleep(2)
        
        monitor = RaftMonitor(args.config)
        monitor.run()
    
    elif args.mode == 'test':
        tester = RaftTester(args.config)
        tester.run_interactive()


if __name__ == "__main__":
    main()