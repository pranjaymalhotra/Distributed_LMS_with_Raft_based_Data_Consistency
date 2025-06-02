"""
Tests for Raft consensus implementation.
Tests leader election, log replication, and fault tolerance.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch

from raft.node import RaftNode
from raft.state import NodeState
from raft.log import LogEntry
from common.utils import generate_id


class TestRaftNode:
    """Test cases for Raft node implementation"""
    
    @pytest.fixture
    def config(self):
        """Test configuration"""
        return {
            'raft': {
                'election_timeout_min_ms': 150,
                'election_timeout_max_ms': 300,
                'heartbeat_interval_ms': 50,
                'snapshot_interval': 100,
                'max_log_entries': 1000,
                'sync_interval_ms': 10,
                'request_timeout_ms': 5000,
                'max_append_entries': 100
            },
            'storage': {
                'raft_logs_dir': 'test_data/raft_logs',
                'raft_snapshots_dir': 'test_data/raft_snapshots'
            }
        }
    
    @pytest.fixture
    def three_node_cluster(self, config):
        """Create a three-node test cluster"""
        nodes = {}
        
        # Create nodes
        for i in range(1, 4):
            node_id = f"node{i}"
            peers = {f"node{j}": f"localhost:500{j}" 
                    for j in range(1, 4) if j != i}
            
            node = RaftNode(node_id, peers, config)
            nodes[node_id] = node
        
        yield nodes
        
        # Cleanup
        for node in nodes.values():
            node.stop()
    
    def test_initial_state(self, config):
        """Test node starts in follower state"""
        node = RaftNode("test_node", {}, config)
        
        assert node.state == NodeState.FOLLOWER
        assert node.persistent_state.current_term == 0
        assert node.persistent_state.voted_for is None
        assert node.volatile_state.commit_index == 0
        
        node.stop()
    
    def test_single_node_becomes_leader(self, config):
        """Test single node becomes leader"""
        node = RaftNode("single_node", {}, config)
        node.start()
        
        # Wait for election timeout
        time.sleep(0.5)
        
        # Single node should become leader
        assert node.state == NodeState.LEADER
        assert node.volatile_state.leader_id == "single_node"
        
        node.stop()
    
    def test_leader_election(self, three_node_cluster):
        """Test leader election in a three-node cluster"""
        nodes = three_node_cluster
        
        # Start all nodes
        for node in nodes.values():
            node.start()
        
        # Wait for election
        time.sleep(1.0)
        
        # Exactly one node should be leader
        leaders = [n for n in nodes.values() if n.state == NodeState.LEADER]
        assert len(leaders) == 1
        
        leader = leaders[0]
        followers = [n for n in nodes.values() if n.state == NodeState.FOLLOWER]
        assert len(followers) == 2
        
        # All nodes should agree on the leader
        for node in nodes.values():
            assert node.volatile_state.leader_id == leader.node_id
    
    def test_log_replication(self, three_node_cluster):
        """Test log replication across cluster"""
        nodes = three_node_cluster
        
        # Start nodes and wait for leader
        for node in nodes.values():
            node.start()
        time.sleep(1.0)
        
        # Find leader
        leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)
        
        # Submit a command
        command = {
            'type': 'CREATE_ASSIGNMENT',
            'assignment_id': generate_id(),
            'title': 'Test Assignment'
        }
        
        success, result, error = leader.handle_client_request(
            command, "test_client", "request_1"
        )
        
        assert success
        
        # Wait for replication
        time.sleep(0.5)
        
        # All nodes should have the entry
        for node in nodes.values():
            assert node.log.get_last_index() == 1
            entry = node.log.get_entry(1)
            assert entry is not None
            assert entry.command == command
    
    def test_leader_failure_triggers_new_election(self, three_node_cluster):
        """Test new election when leader fails"""
        nodes = three_node_cluster
        
        # Start nodes and wait for leader
        for node in nodes.values():
            node.start()
        time.sleep(1.0)
        
        # Find and stop leader
        old_leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)
        old_leader_id = old_leader.node_id
        old_leader.stop()
        
        # Wait for new election
        time.sleep(1.0)
        
        # New leader should be elected
        remaining_nodes = [n for n in nodes.values() if n.node_id != old_leader_id]
        new_leaders = [n for n in remaining_nodes if n.state == NodeState.LEADER]
        
        assert len(new_leaders) == 1
        assert new_leaders[0].node_id != old_leader_id
    
    def test_split_vote_prevention(self, config):
        """Test randomized timeouts prevent split votes"""
        # Create 5 nodes
        nodes = {}
        for i in range(1, 6):
            node_id = f"node{i}"
            peers = {f"node{j}": f"localhost:500{j}" 
                    for j in range(1, 6) if j != i}
            nodes[node_id] = RaftNode(node_id, peers, config)
        
        # Start all nodes simultaneously
        for node in nodes.values():
            node.start()
        
        # Wait for election
        time.sleep(1.5)
        
        # Should have exactly one leader
        leaders = [n for n in nodes.values() if n.state == NodeState.LEADER]
        assert len(leaders) == 1
        
        # Cleanup
        for node in nodes.values():
            node.stop()
    
    def test_log_consistency_after_partition(self, three_node_cluster):
        """Test log remains consistent after network partition"""
        nodes = three_node_cluster
        
        # Start nodes
        for node in nodes.values():
            node.start()
        time.sleep(1.0)
        
        # Find leader
        leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)
        followers = [n for n in nodes.values() if n.state == NodeState.FOLLOWER]
        
        # Simulate partition - isolate one follower
        isolated_follower = followers[0]
        
        # Mock RPC to simulate network partition
        original_clients = isolated_follower.rpc_clients
        isolated_follower.rpc_clients = {}
        
        # Submit commands while follower is isolated
        for i in range(3):
            command = {
                'type': 'CREATE_ASSIGNMENT',
                'assignment_id': f"assignment_{i}",
                'title': f'Assignment {i}'
            }
            leader.handle_client_request(command, "client", f"req_{i}")
        
        time.sleep(0.5)
        
        # Isolated follower should not have new entries
        assert isolated_follower.log.get_last_index() == 0
        
        # Heal partition
        isolated_follower.rpc_clients = original_clients
        
        # Wait for catch-up
        time.sleep(1.0)
        
        # All nodes should now have same log
        for node in nodes.values():
            assert node.log.get_last_index() == 3
    
    def test_client_request_forwarding(self, three_node_cluster):
        """Test client requests to followers are handled correctly"""
        nodes = three_node_cluster
        
        # Start nodes
        for node in nodes.values():
            node.start()
        time.sleep(1.0)
        
        # Find a follower
        follower = next(n for n in nodes.values() if n.state == NodeState.FOLLOWER)
        leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)
        
        # Submit request to follower
        command = {'type': 'CREATE_ASSIGNMENT', 'assignment_id': 'test'}
        success, result, error = follower.handle_client_request(
            command, "client", "request"
        )
        
        # Should fail with leader hint
        assert not success
        assert leader.node_id in error


class TestRaftLog:
    """Test cases for Raft log implementation"""
    
    def test_append_entry(self, tmp_path):
        """Test appending entries to log"""
        log = RaftLog("test_node", str(tmp_path))
        
        entry1 = LogEntry(index=1, term=1, command={'type': 'test'})
        assert log.append_entry(entry1)
        
        assert log.get_last_index() == 1
        assert log.get_last_term() == 1
        
        retrieved = log.get_entry(1)
        assert retrieved.index == 1
        assert retrieved.term == 1
        assert retrieved.command == {'type': 'test'}
    
    def test_log_truncation(self, tmp_path):
        """Test log truncation on conflicts"""
        log = RaftLog("test_node", str(tmp_path))
        
        # Add initial entries
        for i in range(1, 6):
            entry = LogEntry(index=i, term=1, command={'id': i})
            log.append_entry(entry)
        
        # Simulate conflict at index 3
        conflicting_entries = [
            LogEntry(index=3, term=2, command={'id': 'new3'}),
            LogEntry(index=4, term=2, command={'id': 'new4'})
        ]
        
        success = log.append_entries(2, 1, conflicting_entries)
        assert success
        
        # Log should be truncated and new entries added
        assert log.get_last_index() == 4
        assert log.get_entry(3).term == 2
        assert log.get_entry(5) is None
    
    def test_log_compaction(self, tmp_path):
        """Test log compaction with snapshots"""
        log = RaftLog("test_node", str(tmp_path))
        
        # Add many entries
        for i in range(1, 101):
            entry = LogEntry(index=i, term=1, command={'id': i})
            log.append_entry(entry)
        
        # Compact log at index 50
        log.compact_log(50, 1)
        
        # Entries before 50 should be gone
        assert log.get_entry(49) is None
        assert log.get_entry(51) is not None
        assert log.get_last_index() == 100
        
        # Should handle queries for compacted entries
        assert log.get_term_at_index(50) == 1


class TestRaftSafety:
    """Test Raft safety properties"""
    
    def test_election_safety(self, three_node_cluster):
        """Test at most one leader per term"""
        nodes = three_node_cluster
        
        # Track leaders by term
        leaders_by_term = {}
        
        def record_leaders():
            for _ in range(10):
                time.sleep(0.5)
                for node in nodes.values():
                    if node.state == NodeState.LEADER:
                        term = node.persistent_state.current_term
                        if term not in leaders_by_term:
                            leaders_by_term[term] = set()
                        leaders_by_term[term].add(node.node_id)
        
        # Start nodes
        for node in nodes.values():
            node.start()
        
        # Record leaders over time
        record_thread = threading.Thread(target=record_leaders)
        record_thread.start()
        record_thread.join()
        
        # Check at most one leader per term
        for term, leaders in leaders_by_term.items():
            assert len(leaders) <= 1, f"Multiple leaders in term {term}: {leaders}"
    
    def test_log_matching_property(self, three_node_cluster):
        """Test log matching property - if entries have same index and term, 
        all preceding entries are identical"""
        nodes = three_node_cluster
        
        # Start nodes
        for node in nodes.values():
            node.start()
        time.sleep(1.0)
        
        # Submit multiple commands
        leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)
        
        for i in range(10):
            command = {'type': 'test', 'id': i}
            leader.handle_client_request(command, "client", f"req_{i}")
        
        time.sleep(1.0)
        
        # Check all logs match
        logs = [node.log for node in nodes.values()]
        
        for i in range(1, 11):
            entries = [log.get_entry(i) for log in logs]
            # All entries at same index should have same term
            terms = [e.term for e in entries if e]
            assert len(set(terms)) == 1
            
            # All entries should have same command
            commands = [e.command for e in entries if e]
            assert len(set(str(c) for c in commands)) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])