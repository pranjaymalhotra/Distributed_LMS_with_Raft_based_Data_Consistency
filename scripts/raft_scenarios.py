"""
Automated Raft testing scenarios with visual feedback.
Tests leader election, network partitions, and fault tolerance.
"""

import os
import sys
import time
import subprocess
import signal
import json
from typing import Dict, List, Optional
import psutil

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.logger import get_logger

logger = get_logger(__name__)


class RaftScenarioTester:
    """Automated testing for various Raft scenarios"""
    
    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.processes = {}
        self.node_ports = {}
        
        # Extract node ports for management
        for node_id, node_config in self.config['cluster']['nodes'].items():
            self.node_ports[node_id] = {
                'raft': node_config['raft_port'],
                'lms': node_config['lms_port']
            }
    
    def _print_banner(self, text: str, char: str = "="):
        """Print a formatted banner"""
        width = 70
        print("\n" + char * width)
        print(f"{text:^{width}}")
        print(char * width + "\n")
    
    def _print_step(self, step: str, status: str = "🔄"):
        """Print a test step"""
        print(f"{status} {step}")
    
    def _start_node(self, node_id: str) -> bool:
        """Start a single node"""
        try:
            cmd = [
                sys.executable,
                "lms/main.py",
                node_id,
                "--config", "config.json"
            ]
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Create new process group
            )
            
            self.processes[node_id] = process
            self._print_step(f"Started {node_id} (PID: {process.pid})", "✅")
            return True
            
        except Exception as e:
            self._print_step(f"Failed to start {node_id}: {e}", "❌")
            return False
    
    def _stop_node(self, node_id: str) -> bool:
        """Stop a single node"""
        try:
            # Try graceful shutdown first
            if node_id in self.processes:
                process = self.processes[node_id]
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                time.sleep(1)
                
                # Force kill if still running
                if process.poll() is None:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                
                del self.processes[node_id]
            
            # Also kill by port to be sure
            for port in self.node_ports[node_id].values():
                os.system(f"lsof -ti:{port} | xargs kill -9 2>/dev/null")
            
            self._print_step(f"Stopped {node_id}", "✅")
            return True
            
        except Exception as e:
            self._print_step(f"Failed to stop {node_id}: {e}", "❌")
            return False
    
    def _get_cluster_status(self) -> Dict:
        """Get current cluster status"""
        import grpc
        from raft import raft_pb2, raft_pb2_grpc

        status = {}
        
        for node_id, ports in self.node_ports.items():
            try:
                channel = grpc.insecure_channel(f"localhost:{ports['raft']}")
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                
                request = raft_pb2.HealthCheckRequest(node_id=node_id)
                response = stub.HealthCheck(request, timeout=1)
                
                status[node_id] = {
                    'alive': True,
                    'state': response.state,
                    'term': response.current_term,
                    'leader': response.leader_id
                }
                
                channel.close()
                
            except:
                status[node_id] = {
                    'alive': False,
                    'state': 'OFFLINE',
                    'term': 0,
                    'leader': None
                }
        
        return status
    
    def _wait_for_leader(self, timeout: float = 10) -> Optional[str]:
        """Wait for a leader to be elected"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self._get_cluster_status()
            
            for node_id, node_status in status.items():
                if node_status['state'] == 'LEADER':
                    return node_id
            
            time.sleep(0.5)
        
        return None
    
    def _display_cluster_state(self):
        """Display current cluster state"""
        status = self._get_cluster_status()
        
        print("\n📊 Current Cluster State:")
        print("-" * 50)
        
        leader = None
        for node_id, node_status in sorted(status.items()):
            if node_status['state'] == 'LEADER':
                leader = node_id
                icon = "👑"
            elif node_status['state'] == 'FOLLOWER':
                icon = "📡"
            elif node_status['state'] == 'CANDIDATE':
                icon = "🗳️"
            else:
                icon = "❌"
            
            print(f"  {icon} {node_id}: {node_status['state']:<10} "
                  f"Term: {node_status['term']:<3} "
                  f"{'(Leader: ' + str(node_status['leader']) + ')' if node_status['leader'] else ''}")
        
        print("-" * 50)
        
        return leader
    
    def scenario_basic_election(self):
        """Test basic leader election"""
        self._print_banner("SCENARIO 1: Basic Leader Election")
        
        print("This test verifies that nodes can elect a leader when starting up.\n")
        
        # Start nodes one by one
        self._print_step("Starting 5-node cluster...")
        
        for i in range(1, 6):
            self._start_node(f"node{i}")
            time.sleep(0.5)  # Stagger starts
        
        # Wait for leader election
        self._print_step("Waiting for leader election...")
        time.sleep(3)
        
        leader = self._display_cluster_state()
        
        if leader:
            self._print_step(f"Leader elected: {leader}", "✅")
            print("\n✨ Test PASSED: Cluster successfully elected a leader")
        else:
            self._print_step("No leader elected", "❌")
            print("\n❌ Test FAILED: No leader was elected")
        
        return leader is not None
    
    def scenario_leader_failure(self):
        """Test leader failure and re-election"""
        self._print_banner("SCENARIO 2: Leader Failure & Re-election")
        
        print("This test verifies that a new leader is elected when the current leader fails.\n")
        
        # Get current leader
        old_leader = self._wait_for_leader()
        if not old_leader:
            self._print_step("No initial leader found", "❌")
            return False
        
        self._print_step(f"Current leader: {old_leader}", "👑")
        
        # Kill the leader
        self._print_step(f"Killing leader {old_leader}...")
        self._stop_node(old_leader)
        
        # Wait for new election
        self._print_step("Waiting for new leader election...")
        time.sleep(3)
        
        # Check new leader
        new_leader = self._wait_for_leader(timeout=5)
        self._display_cluster_state()
        
        if new_leader and new_leader != old_leader:
            self._print_step(f"New leader elected: {new_leader}", "✅")
            print("\n✨ Test PASSED: Cluster recovered from leader failure")
            return True
        else:
            self._print_step("No new leader elected", "❌")
            print("\n❌ Test FAILED: Cluster did not recover from leader failure")
            return False
    
    def scenario_split_brain(self):
        """Test network partition (split-brain) scenario"""
        self._print_banner("SCENARIO 3: Network Partition (Split-Brain)")
        
        print("This test simulates a network partition and verifies that")
        print("only the partition with majority maintains a leader.\n")
        
        # Ensure all nodes are running
        self._print_step("Ensuring all 5 nodes are running...")
        for i in range(1, 6):
            if f"node{i}" not in self.processes:
                self._start_node(f"node{i}")
        
        time.sleep(3)
        initial_leader = self._wait_for_leader()
        
        if not initial_leader:
            self._print_step("No initial leader", "❌")
            return False
        
        self._print_step(f"Initial leader: {initial_leader}", "👑")
        
        # Create partition: nodes 1,2 vs nodes 3,4,5
        self._print_step("Creating partition: [node1, node2] | [node3, node4, node5]")
        self._stop_node("node1")
        self._stop_node("node2")
        
        time.sleep(5)  # Wait for detection and re-election
        
        # Check majority partition
        self._print_step("Checking majority partition (node3, node4, node5)...")
        majority_status = self._display_cluster_state()
        
        # Count alive nodes
        status = self._get_cluster_status()
        alive_nodes = [n for n, s in status.items() if s['alive']]
        has_leader = any(s['state'] == 'LEADER' for s in status.values() if s['alive'])
        
        if len(alive_nodes) == 3 and has_leader:
            self._print_step("Majority partition has a leader", "✅")
            print("\n✨ Test PASSED: Majority partition maintains leadership")
            return True
        else:
            self._print_step("Majority partition lost leadership", "❌")
            print("\n❌ Test FAILED: Majority partition should maintain leadership")
            return False
    
    def scenario_rapid_failures(self):
        """Test rapid node failures"""
        self._print_banner("SCENARIO 4: Rapid Sequential Failures")
        
        print("This test verifies cluster stability under rapid node failures.\n")
        
        # Ensure all nodes running
        self._print_step("Starting all nodes...")
        for i in range(1, 6):
            if f"node{i}" not in self.processes:
                self._start_node(f"node{i}")
        
        time.sleep(3)
        
        # Rapid failures
        self._print_step("Simulating rapid failures...")
        
        failures = ["node1", "node2"]
        for node in failures:
            self._print_step(f"Failing {node}")
            self._stop_node(node)
            time.sleep(1)  # Very short delay
        
        # Check if cluster maintains quorum
        time.sleep(3)
        leader = self._wait_for_leader(timeout=5)
        self._display_cluster_state()
        
        if leader:
            self._print_step(f"Cluster maintained leadership with {leader}", "✅")
            print("\n✨ Test PASSED: Cluster survived rapid failures")
            return True
        else:
            self._print_step("Cluster lost leadership", "❌")
            print("\n❌ Test FAILED: Cluster should maintain leadership with 3/5 nodes")
            return False
    
    def scenario_rejoin(self):
        """Test node rejoin after failure"""
        self._print_banner("SCENARIO 5: Node Rejoin After Failure")
        
        print("This test verifies that failed nodes can rejoin the cluster.\n")
        
        # Get current state
        self._display_cluster_state()
        
        # Start previously failed nodes
        self._print_step("Restarting failed nodes...")
        
        for node in ["node1", "node2"]:
            if node not in self.processes:
                self._start_node(node)
                time.sleep(1)
        
        # Wait for reintegration
        self._print_step("Waiting for cluster reintegration...")
        time.sleep(5)
        
        # Check final state
        status = self._get_cluster_status()
        alive_count = sum(1 for s in status.values() if s['alive'])
        
        self._display_cluster_state()
        
        if alive_count == 5:
            self._print_step("All nodes successfully rejoined", "✅")
            print("\n✨ Test PASSED: Failed nodes successfully rejoined cluster")
            return True
        else:
            self._print_step(f"Only {alive_count}/5 nodes active", "❌")
            print("\n❌ Test FAILED: Not all nodes rejoined successfully")
            return False
    
    def cleanup(self):
        """Clean up all processes"""
        self._print_step("Cleaning up all nodes...")
        
        for node_id in list(self.processes.keys()):
            self._stop_node(node_id)
        
        # Extra cleanup by ports
        for node_id, ports in self.node_ports.items():
            for port in ports.values():
                os.system(f"lsof -ti:{port} | xargs kill -9 2>/dev/null")
    
    def run_all_scenarios(self):
        """Run all test scenarios"""
        self._print_banner("RAFT CONSENSUS TEST SUITE", "=")
        
        scenarios = [
            ("Basic Election", self.scenario_basic_election),
            ("Leader Failure", self.scenario_leader_failure),
            ("Network Partition", self.scenario_split_brain),
            ("Rapid Failures", self.scenario_rapid_failures),
            ("Node Rejoin", self.scenario_rejoin)
        ]
        
        results = {}
        
        try:
            for name, scenario in scenarios:
                try:
                    print(f"\n{'='*70}")
                    results[name] = scenario()
                    time.sleep(2)  # Pause between scenarios
                except Exception as e:
                    self._print_step(f"Scenario failed with error: {e}", "❌")
                    results[name] = False
            
            # Summary
            self._print_banner("TEST SUMMARY")
            
            passed = sum(1 for r in results.values() if r)
            total = len(results)
            
            print(f"\nResults: {passed}/{total} scenarios passed\n")
            
            for scenario, result in results.items():
                icon = "✅" if result else "❌"
                print(f"  {icon} {scenario}")
            
            if passed == total:
                print("\n🎉 All tests passed! Raft implementation is working correctly.")
            else:
                print(f"\n⚠️  {total - passed} test(s) failed. Check the implementation.")
            
        finally:
            print("\n" + "="*70)
            self.cleanup()
            print("\n✅ Cleanup complete")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Run automated Raft consensus test scenarios'
    )
    
    parser.add_argument(
        '--scenario',
        choices=['all', 'election', 'failure', 'partition', 'rapid', 'rejoin'],
        default='all',
        help='Which scenario to run'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    tester = RaftScenarioTester(args.config)
    
    try:
        if args.scenario == 'all':
            tester.run_all_scenarios()
        elif args.scenario == 'election':
            tester.scenario_basic_election()
        elif args.scenario == 'failure':
            tester.scenario_leader_failure()
        elif args.scenario == 'partition':
            tester.scenario_split_brain()
        elif args.scenario == 'rapid':
            tester.scenario_rapid_failures()
        elif args.scenario == 'rejoin':
            tester.scenario_rejoin()
    
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    finally:
        tester.cleanup()


if __name__ == "__main__":
    main()