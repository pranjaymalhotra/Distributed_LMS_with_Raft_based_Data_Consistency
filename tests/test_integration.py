"""
Integration tests for the distributed LMS system.
Tests the full system including Raft, LMS, and client interactions.
"""

import pytest
import time
import os
import tempfile
import shutil
from typing import Dict, List

from raft.node import RaftNode
from raft.rpc import RaftRPCServer
from lms.server import LMSServer
from client.student_client import StudentClient
from client.instructor_client import InstructorClient
from common.constants import *


class TestLMSIntegration:
    """Integration tests for the full LMS system"""
    
    @pytest.fixture
    def test_config(self):
        """Create test configuration"""
        temp_dir = tempfile.mkdtemp()
        
        config = {
            'cluster': {
                'nodes': {
                    'test_node1': {
                        'id': 'test_node1',
                        'host': 'localhost',
                        'raft_port': 15001,
                        'lms_port': 16001,
                        'role': 'LMS_WITH_RAFT'
                    },
                    'test_node2': {
                        'id': 'test_node2',
                        'host': 'localhost',
                        'raft_port': 15002,
                        'lms_port': 16002,
                        'role': 'LMS_WITH_RAFT'
                    },
                    'test_node3': {
                        'id': 'test_node3',
                        'host': 'localhost',
                        'raft_port': 15003,
                        'lms_port': 16003,
                        'role': 'LMS_WITH_RAFT'
                    }
                },
                'tutoring_server': {
                    'host': 'localhost',
                    'port': 17001,
                    'model': 'deepseek-r1:1.5b',
                    'context_window': 2048
                }
            },
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
                'raft_logs_dir': os.path.join(temp_dir, 'raft_logs'),
                'raft_snapshots_dir': os.path.join(temp_dir, 'raft_snapshots'),
                'course_materials_dir': os.path.join(temp_dir, 'course_materials'),
                'assignments_dir': os.path.join(temp_dir, 'assignments'),
                'database_dir': os.path.join(temp_dir, 'database')
            },
            'auth': {
                'token_expiry_hours': 24,
                'default_users': [
                    {
                        'username': 'test_instructor',
                        'password': 'test123',
                        'type': 'INSTRUCTOR',
                        'name': 'Test Instructor'
                    },
                    {
                        'username': 'test_student',
                        'password': 'test123',
                        'type': 'STUDENT',
                        'name': 'Test Student'
                    }
                ]
            },
            'logging': {
                'level': 'WARNING',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': os.path.join(temp_dir, 'test.log')
            }
        }
        
        yield config
        
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def lms_cluster(self, test_config):
        """Create a test LMS cluster"""
        nodes = []
        servers = []
        
        # Create Raft nodes and LMS servers
        for node_id, node_config in test_config['cluster']['nodes'].items():
            # Create peers mapping
            peers = {}
            for peer_id, peer_config in test_config['cluster']['nodes'].items():
                if peer_id != node_id:
                    peers[peer_id] = f"{peer_config['host']}:{peer_config['raft_port']}"
            
            # Create Raft node
            raft_node = RaftNode(node_id, peers, test_config)
            
            # Create Raft RPC server
            raft_rpc = RaftRPCServer(raft_node)
            raft_rpc.start(node_config['raft_port'])
            
            # Create LMS server
            lms_server = LMSServer(node_id, test_config, raft_node)
            lms_server.set_raft_node(raft_node)
            lms_server.start(node_config['lms_port'])
            
            # Start Raft node
            raft_node.start()
            
            nodes.append((raft_node, raft_rpc))
            servers.append(lms_server)
        
        # Wait for leader election
        time.sleep(2)
        
        yield servers
        
        # Cleanup
        for raft_node, raft_rpc in nodes:
            raft_node.stop()
            raft_rpc.stop()
        
        for server in servers:
            server.stop()
    
    def test_basic_workflow(self, lms_cluster):
        """Test basic student-instructor workflow"""
        # Create clients
        server_addresses = ["localhost:16001", "localhost:16002", "localhost:16003"]
        
        instructor_client = InstructorClient(server_addresses)
        student_client = StudentClient(server_addresses)
        
        # Login
        assert instructor_client.login("test_instructor", "test123")
        assert student_client.login("test_student", "test123")
        
        # Instructor creates assignment
        success, assignment_id = instructor_client.post_data(
            DATA_TYPE_ASSIGNMENT,
            {
                'title': 'Test Assignment',
                'description': 'This is a test',
                'due_date': '2024-12-31 23:59'
            }
        )
        assert success
        assert assignment_id is not None
        
        # Student views assignments
        assignments = student_client.get_data(DATA_TYPE_ASSIGNMENT)
        assert len(assignments) == 1
        assert assignments[0]['title'] == 'Test Assignment'
        
        # Student submits assignment
        success, submission_id = student_client.post_data(
            DATA_TYPE_SUBMISSION,
            {
                'assignment_id': assignments[0]['assignment_id'],
                'content': 'My test submission'
            }
        )
        assert success
        
        # Instructor views submissions
        submissions = instructor_client.get_data(
            DATA_TYPE_SUBMISSION,
            {'assignment_id': assignments[0]['assignment_id']}
        )
        assert len(submissions) == 1
        
        # Logout
        assert instructor_client.logout()
        assert student_client.logout()
    
    def test_consistency_across_nodes(self, lms_cluster):
        """Test data consistency across all nodes"""
        # Connect to different nodes
        clients = []
        for i in range(3):
            client = InstructorClient([f"localhost:1600{i+1}"])
            assert client.login("test_instructor", "test123")
            clients.append(client)
        
        # Create assignment on node 1
        success, assignment_id = clients[0].post_data(
            DATA_TYPE_ASSIGNMENT,
            {
                'title': 'Consistency Test',
                'description': 'Testing consistency',
                'due_date': '2024-12-31 23:59'
            }
        )
        assert success
        
        # Wait for replication
        time.sleep(1)
        
        # Check all nodes have the assignment
        for i, client in enumerate(clients):
            assignments = client.get_data(DATA_TYPE_ASSIGNMENT)
            assert len(assignments) == 1
            assert assignments[0]['title'] == 'Consistency Test'
            print(f"Node {i+1}: Assignment found ✓")
        
        # Cleanup
        for client in clients:
            client.logout()
    
    def test_node_failure_recovery(self, lms_cluster):
        """Test system continues working after node failure"""
        # Initial setup
        client = StudentClient(["localhost:16001", "localhost:16002", "localhost:16003"])
        assert client.login("test_student", "test123")
        
        # Submit initial query
        success, query_id1 = client.post_data(
            DATA_TYPE_QUERY,
            {
                'query': 'Test query before failure',
                'use_llm': False
            }
        )
        assert success
        
        # Simulate node failure (stop first server)
        lms_cluster[0].stop()
        time.sleep(2)  # Wait for failure detection
        
        # Submit query after failure
        success, query_id2 = client.post_data(
            DATA_TYPE_QUERY,
            {
                'query': 'Test query after failure',
                'use_llm': False
            }
        )
        assert success  # Should still work with remaining nodes
        
        # Verify both queries exist
        queries = client.get_data(DATA_TYPE_QUERY)
        assert len(queries) == 2
        
        client.logout()
    
    def test_concurrent_operations(self, lms_cluster):
        """Test concurrent operations from multiple clients"""
        import threading
        
        results = {'success': 0, 'failure': 0}
        lock = threading.Lock()
        
        def student_operations(student_id: int):
            client = StudentClient(["localhost:16001", "localhost:16002", "localhost:16003"])
            
            try:
                # Login
                if not client.login(f"test_student", "test123"):
                    with lock:
                        results['failure'] += 1
                    return
                
                # Post queries
                for i in range(5):
                    success, _ = client.post_data(
                        DATA_TYPE_QUERY,
                        {
                            'query': f'Query {i} from student {student_id}',
                            'use_llm': False
                        }
                    )
                    
                    with lock:
                        if success:
                            results['success'] += 1
                        else:
                            results['failure'] += 1
                    
                    time.sleep(0.1)
                
                client.logout()
                
            except Exception as e:
                with lock:
                    results['failure'] += 1
        
        # Start concurrent clients
        threads = []
        for i in range(3):
            thread = threading.Thread(target=student_operations, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Check results
        total_operations = results['success'] + results['failure']
        success_rate = results['success'] / total_operations * 100 if total_operations > 0 else 0
        
        print(f"\nConcurrent operations: {results['success']}/{total_operations} succeeded")
        print(f"Success rate: {success_rate:.1f}%")
        
        assert success_rate > 80  # Allow some failures in concurrent scenario
    
    def test_query_workflow(self, lms_cluster):
        """Test student query workflow"""
        # Setup clients
        student = StudentClient(["localhost:16001", "localhost:16002", "localhost:16003"])
        instructor = InstructorClient(["localhost:16001", "localhost:16002", "localhost:16003"])
        
        assert student.login("test_student", "test123")
        assert instructor.login("test_instructor", "test123")
        
        # Student posts query for instructor
        success, query_id = student.post_data(
            DATA_TYPE_QUERY,
            {
                'query': 'What is the deadline for the final project?',
                'use_llm': False
            }
        )
        assert success
        
        # Instructor views queries
        queries = instructor.get_data(DATA_TYPE_QUERY)
        assert len(queries) == 1
        assert queries[0]['query'] == 'What is the deadline for the final project?'
        
        # Verify query appears as unanswered
        assert queries[0].get('answer') is None
        
        # Cleanup
        student.logout()
        instructor.logout()


class TestDataPersistence:
    """Test data persistence across restarts"""
    
    def test_data_survives_restart(self, test_config):
        """Test that data persists after cluster restart"""
        # Phase 1: Create data
        nodes1 = []
        servers1 = []
        
        # Start initial cluster
        for node_id, node_config in test_config['cluster']['nodes'].items():
            peers = {peer_id: f"{peer_config['host']}:{peer_config['raft_port']}"
                    for peer_id, peer_config in test_config['cluster']['nodes'].items()
                    if peer_id != node_id}
            
            raft_node = RaftNode(node_id, peers, test_config)
            raft_rpc = RaftRPCServer(raft_node)
            raft_rpc.start(node_config['raft_port'])
            
            lms_server = LMSServer(node_id, test_config, raft_node)
            lms_server.set_raft_node(raft_node)
            lms_server.start(node_config['lms_port'])
            
            raft_node.start()
            
            nodes1.append((raft_node, raft_rpc))
            servers1.append(lms_server)
        
        time.sleep(2)
        
        # Create some data
        client = InstructorClient(["localhost:16001"])
        assert client.login("test_instructor", "test123")
        
        success, assignment_id = client.post_data(
            DATA_TYPE_ASSIGNMENT,
            {
                'title': 'Persistent Assignment',
                'description': 'This should survive restart',
                'due_date': '2024-12-31 23:59'
            }
        )
        assert success
        
        client.logout()
        
        # Stop cluster
        for raft_node, raft_rpc in nodes1:
            raft_node.stop()
            raft_rpc.stop()
        for server in servers1:
            server.stop()
        
        time.sleep(1)
        
        # Phase 2: Restart and verify
        nodes2 = []
        servers2 = []
        
        # Start new cluster with same config
        for node_id, node_config in test_config['cluster']['nodes'].items():
            peers = {peer_id: f"{peer_config['host']}:{peer_config['raft_port']}"
                    for peer_id, peer_config in test_config['cluster']['nodes'].items()
                    if peer_id != node_id}
            
            raft_node = RaftNode(node_id, peers, test_config)
            raft_rpc = RaftRPCServer(raft_node)
            raft_rpc.start(node_config['raft_port'])
            
            lms_server = LMSServer(node_id, test_config, raft_node)
            lms_server.set_raft_node(raft_node)
            lms_server.start(node_config['lms_port'])
            
            raft_node.start()
            
            nodes2.append((raft_node, raft_rpc))
            servers2.append(lms_server)
        
        time.sleep(2)
        
        # Verify data exists
        client2 = InstructorClient(["localhost:16001"])
        assert client2.login("test_instructor", "test123")
        
        assignments = client2.get_data(DATA_TYPE_ASSIGNMENT)
        assert len(assignments) == 1
        assert assignments[0]['title'] == 'Persistent Assignment'
        
        client2.logout()
        
        # Cleanup
        for raft_node, raft_rpc in nodes2:
            raft_node.stop()
            raft_rpc.stop()
        for server in servers2:
            server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])