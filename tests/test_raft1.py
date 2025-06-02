"""
Script to test Raft consensus implementation.
Tests various scenarios including leader election, network partitions, and node failures.
"""

import os
import sys
import time
import threading
import random
from typing import Dict, List

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raft.node import RaftNode
from raft.rpc import RaftRPCServer
from common.logger import setup_logging, get_logger
from common.utils import generate_id


class RaftTestCluster:
    """Test harness for Raft cluster"""
    
    def __init__(self, num_nodes: int = 5):
        self.num_nodes = num_nodes
        self.nodes = {}
        self.rpc_servers = {}
        self.config = self._create_test_config()
        
        # Setup logging
        setup_logging(self.config['logging'])
        self.logger = get_logger(__name__)
    
    def _create_test_config(self) -> Dict:
        """Create test configuration"""
        return {
            'raft': {
                'election_timeout_min_ms': 150,
                'election_timeout_max_ms': 300,
                'heartbeat_interval_ms': 50,
                'snapshot_interval': 10,
                'max_log_entries': 100,
                'sync_interval_ms': 10,
                'request_timeout_ms': 5000,
                'max_append_entries': 10
            },
            'storage': {
                'raft_logs_dir': 'test_data/raft_logs',
                'raft_snapshots_dir': 'test_data/raft_snapshots'
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': 'test_data/raft_test.log'
            }
        }
    
    def start_cluster(self):
        """Start all nodes in the cluster"""
        self.logger.info(f"Starting {self.num_nodes}-node Raft cluster")
        
        # Create peer mappings
        peers_map = {}
        for i in range(1, self.num_nodes + 1):
            node_id = f"node{i}"
            peers = {}
            for j in range(1, self.num_nodes + 1):
                if i != j:
                    peer_id = f"node{j}"
                    peers[peer_id] = f"localhost:{5000 + j}"
            peers_map[node_id] = peers
        
        # Create and start nodes
        for i in range(1, self.num_nodes + 1):
            node_id = f"node{i}"
            port = 5000 + i
            
            # Create node
            node = RaftNode(node_id, peers_map[node_id], self.config)
            self.nodes[node_id] = node
            
            # Create and start RPC server
            rpc_server = RaftRPCServer(node)
            rpc_server.start(port)
            self.rpc_servers[node_id] = rpc_server
            
            # Start node
            node.start()
            
            self.logger.info(f"Started {node_id} on port {port}")
            time.sleep(0.1)  # Stagger starts
    
    def stop_cluster(self):
        """Stop all nodes in the cluster"""
        self.logger.info("Stopping cluster")
        
        for node_id, node in self.nodes.items():
            node.stop()
            self.rpc_servers[node_id].stop()
    
    def get_leader(self) -> RaftNode:
        """Get current leader node"""
        for node in self.nodes.values():
            if node.state.name == "LEADER":
                return node
        return None
    
    def get_followers(self) -> List[RaftNode]:
        """Get all follower nodes"""
        return [node for node in self.nodes.values() 
                if node.state.name == "FOLLOWER"]
    
    def wait_for_leader(self, timeout: float = 5.0) -> bool:
        """Wait for a leader to be elected"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.get_leader():
                return True
            time.sleep(0.1)
        return False
    
    def partition_node(self, node_id: str):
        """Simulate network partition for a node"""
        node = self.nodes[node_id]
        # Save original clients and clear
        node._original_rpc_clients = node.rpc_clients
        node.rpc_clients = {}
        self.logger.info(f"Partitioned {node_id}")
    
    def heal_partition(self, node_id: str):
        """Heal network partition for a node"""
        node = self.nodes[node_id]
        if hasattr(node, '_original_rpc_clients'):
            node.rpc_clients = node._original_rpc_clients
            delattr(node, '_original_rpc_clients')
            self.logger.info(f"Healed partition for {node_id}")
    
    def crash_node(self, node_id: str):
        """Simulate node crash"""
        node = self.nodes[node_id]
        node.stop()
        self.rpc_servers[node_id].stop()
        self.logger.info(f"Crashed {node_id}")
    
    def submit_command(self, command: Dict) -> bool:
        """Submit a command to the cluster"""
        leader = self.get_leader()
        if not leader:
            return False
        
        success, result, error = leader.handle_client_request(
            command,
            client_id="test_client",
            request_id=generate_id()
        )
        
        return success
    
    def check_log_consistency(self) -> bool:
        """Check if all logs are consistent"""
        if not self.nodes:
            return True
        
        # Get all logs
        logs = {}
        for node_id, node in self.nodes.items():
            logs[node_id] = []
            for i in range(1, node.log.get_last_index() + 1):
                entry = node.log.get_entry(i)
                if entry:
                    logs[node_id].append((entry.term, entry.command))
        
        # Find the shortest log
        min_length = min(len(log) for log in logs.values()) if logs else 0
        
        # Check consistency up to min_length
        for i in range(min_length):
            entries = [log[i] for log in logs.values()]
            if len(set(str(e) for e in entries)) > 1:
                self.logger.error(f"Inconsistency at index {i+1}")
                return False
        
        return True
    
    def print_cluster_status(self):
        """Print current cluster status"""
        print("\n" + "="*60)
        print("CLUSTER STATUS")
        print("="*60)
        
        for node_id, node in sorted(self.nodes.items()):
            status = node.get_status()
            print(f"\n{node_id}:")
            print(f"  State: {status['state']}")
            print(f"  Term: {status['term']}")
            print(f"  Leader: {status['leader_id']}")
            print(f"  Log Length: {status['log_length']}")
            print(f"  Commit Index: {status['commit_index']}")
            print(f"  Last Applied: {status['last_applied']}")


def test_basic_operations(cluster: RaftTestCluster):
    """Test basic Raft operations"""
    print("\n" + "="*60)
    print("TEST: Basic Operations")
    print("="*60)
    
    # Wait for leader election
    print("Waiting for leader election...")
    if not cluster.wait_for_leader():
        print("❌ No leader elected")
        return False
    
    leader = cluster.get_leader()
    print(f"✅ Leader elected: {leader.node_id}")
    
    # Submit some commands
    print("\nSubmitting commands...")
    for i in range(5):
        command = {
            'type': 'CREATE_ASSIGNMENT',
            'assignment_id': f'test_{i}',
            'title': f'Test Assignment {i}'
        }
        
        if cluster.submit_command(command):
            print(f"✅ Command {i} submitted")
        else:
            print(f"❌ Command {i} failed")
    
    # Wait for replication
    time.sleep(1)
    
    # Check consistency
    if cluster.check_log_consistency():
        print("\n✅ All logs are consistent")
    else:
        print("\n❌ Log inconsistency detected")
    
    cluster.print_cluster_status()
    return True


def test_leader_failure(cluster: RaftTestCluster):
    """Test leader failure and re-election"""
    print("\n" + "="*60)
    print("TEST: Leader Failure")
    print("="*60)
    
    # Get current leader
    old_leader = cluster.get_leader()
    if not old_leader:
        print("❌ No leader to test failure")
        return False
    
    print(f"Current leader: {old_leader.node_id}")
    
    # Submit a command before failure
    command = {'type': 'TEST', 'data': 'before_failure'}
    cluster.submit_command(command)
    
    # Crash the leader
    print(f"\nCrashing leader {old_leader.node_id}...")
    cluster.crash_node(old_leader.node_id)
    
    # Wait for new leader
    print("Waiting for new leader election...")
    time.sleep(2)
    
    new_leader = cluster.get_leader()
    if new_leader:
        print(f"✅ New leader elected: {new_leader.node_id}")
    else:
        print("❌ No new leader elected")
        return False
    
    # Submit command to new leader
    command = {'type': 'TEST', 'data': 'after_failure'}
    if cluster.submit_command(command):
        print("✅ New leader accepting commands")
    else:
        print("❌ New leader not accepting commands")
    
    cluster.print_cluster_status()
    return True


def test_network_partition(cluster: RaftTestCluster):
    """Test network partition scenarios"""
    print("\n" + "="*60)
    print("TEST: Network Partition")
    print("="*60)
    
    # Get leader and partition a follower
    leader = cluster.get_leader()
    followers = cluster.get_followers()
    
    if not leader or len(followers) < 2:
        print("❌ Not enough nodes for partition test")
        return False
    
    # Partition one follower
    partitioned_node = followers[0]
    print(f"\nPartitioning {partitioned_node.node_id}...")
    cluster.partition_node(partitioned_node.node_id)
    
    # Submit commands while node is partitioned
    print("\nSubmitting commands during partition...")
    for i in range(3):
        command = {'type': 'TEST', 'data': f'partition_test_{i}'}
        if cluster.submit_command(command):
            print(f"✅ Command {i} submitted")
    
    time.sleep(1)
    
    # Check partitioned node hasn't received updates
    partitioned_log_length = partitioned_node.log.get_last_index()
    leader_log_length = leader.log.get_last_index()
    
    print(f"\nPartitioned node log length: {partitioned_log_length}")
    print(f"Leader log length: {leader_log_length}")
    
    if partitioned_log_length < leader_log_length:
        print("✅ Partitioned node correctly behind")
    else:
        print("❌ Partitioned node unexpectedly up to date")
    
    # Heal partition
    print(f"\nHealing partition for {partitioned_node.node_id}...")
    cluster.heal_partition(partitioned_node.node_id)
    
    # Wait for catch-up
    print("Waiting for log catch-up...")
    time.sleep(2)
    
    # Check consistency
    if cluster.check_log_consistency():
        print("✅ Logs consistent after healing partition")
    else:
        print("❌ Logs inconsistent after healing partition")
    
    cluster.print_cluster_status()
    return True


def test_concurrent_clients(cluster: RaftTestCluster):
    """Test concurrent client requests"""
    print("\n" + "="*60)
    print("TEST: Concurrent Clients")
    print("="*60)
    
    if not cluster.wait_for_leader():
        print("❌ No leader for concurrent test")
        return False
    
    # Submit many commands concurrently
    num_clients = 5
    commands_per_client = 10
    threads = []
    results = {}
    
    def client_thread(client_id: int):
        successes = 0
        for i in range(commands_per_client):
            command = {
                'type': 'TEST',
                'client': client_id,
                'seq': i
            }
            if cluster.submit_command(command):
                successes += 1
            time.sleep(random.uniform(0.01, 0.05))
        results[client_id] = successes
    
    print(f"\nStarting {num_clients} concurrent clients...")
    
    for i in range(num_clients):
        thread = threading.Thread(target=client_thread, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all clients to finish
    for thread in threads:
        thread.join()
    
    # Print results
    total_success = sum(results.values())
    total_expected = num_clients * commands_per_client
    
    print(f"\nResults:")
    print(f"Total commands: {total_expected}")
    print(f"Successful: {total_success}")
    print(f"Success rate: {total_success/total_expected*100:.1f}%")
    
    # Check consistency
    time.sleep(1)
    if cluster.check_log_consistency():
        print("✅ Logs remain consistent")
    else:
        print("❌ Log inconsistency detected")
    
    return True


def test_log_compaction(cluster: RaftTestCluster):
    """Test log compaction via snapshots"""
    print("\n" + "="*60)
    print("TEST: Log Compaction")
    print("="*60)
    
    if not cluster.wait_for_leader():
        print("❌ No leader for compaction test")
        return False
    
    # Submit many commands to trigger snapshot
    print("\nSubmitting many commands to trigger snapshot...")
    for i in range(20):
        command = {'type': 'TEST', 'data': f'compaction_test_{i}'}
        cluster.submit_command(command)
        
        if i % 5 == 0:
            print(f"  Submitted {i+1} commands...")
    
    # Wait for snapshots
    time.sleep(2)
    
    # Check for snapshots
    leader = cluster.get_leader()
    if leader.metrics['snapshots_created'] > 0:
        print(f"✅ Created {leader.metrics['snapshots_created']} snapshots")
    else:
        print("❌ No snapshots created")
    
    # Verify logs are compacted
    for node_id, node in cluster.nodes.items():
        if node.log.snapshot_index > 0:
            print(f"{node_id}: Compacted up to index {node.log.snapshot_index}")
    
    return True


def main():
    """Run all Raft tests"""
    print("\n" + "="*60)
    print("RAFT CONSENSUS TESTING")
    print("="*60)
    
    # Create test cluster
    cluster = RaftTestCluster(num_nodes=5)
    
    try:
        # Start cluster
        print("\nStarting test cluster...")
        cluster.start_cluster()
        time.sleep(2)  # Wait for initial election
        
        # Run tests
        tests = [
            test_basic_operations,
            test_leader_failure,
            test_network_partition,
            test_concurrent_clients,
            test_log_compaction
        ]
        
        passed = 0
        for test_func in tests:
            try:
                if test_func(cluster):
                    passed += 1
            except Exception as e:
                print(f"\n❌ Test failed with exception: {e}")
        
        # Print summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        print(f"Tests passed: {passed}/{len(tests)}")
        
        if passed == len(tests):
            print("\n✅ All tests passed!")
        else:
            print("\n❌ Some tests failed")
    
    finally:
        # Clean up
        print("\nStopping cluster...")
        cluster.stop_cluster()


if __name__ == "__main__":
    main()