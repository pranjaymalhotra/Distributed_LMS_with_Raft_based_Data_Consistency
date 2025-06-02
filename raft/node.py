"""
Core Raft node implementation.
Handles leader election, log replication, and client requests.
"""

import time
import random
import threading
import grpc
from concurrent import futures
from typing import Dict, List, Optional, Tuple, Set
import json

from raft.state import NodeState, PersistentState, VolatileState, StateMachine
from raft.log import RaftLog, LogEntry
from raft.storage import RaftStorage
from raft.rpc import RaftRPCServer
from common.logger import get_logger
from raft import raft_pb2
from raft import raft_pb2_grpc

logger = get_logger(__name__)


class RaftNode:
    """
    A Raft consensus node implementation.
    Handles all aspects of the Raft protocol including:
    - Leader election
    - Log replication
    - Snapshot management
    - Membership changes
    - Client request handling
    """
    
    def __init__(self, node_id: str, peers: Dict[str, str], config: Dict):
        """
        Initialize a Raft node.
        
        Args:
            node_id: Unique identifier for this node
            peers: Dict mapping peer node IDs to their addresses
            config: Configuration dict with timeouts and other settings
        """
        self.node_id = node_id
        self.peers = peers.copy()  # Don't include self in peers
        self.config = config
        
        # Remove self from peers if present
        self.peers.pop(node_id, None)
        
        # Initialize state
        self.state = NodeState.FOLLOWER
        self.persistent_state = PersistentState()
        self.volatile_state = VolatileState()
        
        # Initialize cluster configuration
        self.volatile_state.current_configuration = set([node_id] + list(self.peers.keys()))
        
        # Storage and log
        storage_dir = config['storage']['raft_logs_dir']
        self.storage = RaftStorage(node_id, storage_dir)
        self.log = RaftLog(node_id, storage_dir)
        
        # State machine
        self.state_machine = StateMachine()
        
        # Load persistent state
        self._load_persistent_state()
        
        # Threading and synchronization
        self.lock = threading.RLock()
        self.commit_cv = threading.Condition(self.lock)
        self.running = False
        
        # Election timeout
        self._reset_election_timeout()
        
        # Client request handling
        self.pending_requests = {}  # request_id -> (client_id, future)
        
        # RPC clients
        self.rpc_clients = {}
        self._init_rpc_clients()
        
        # Threads
        self.election_thread = None
        self.heartbeat_thread = None
        self.commit_thread = None
        
        # Metrics
        self.metrics = {
            'elections_started': 0,
            'elections_won': 0,
            'heartbeats_sent': 0,
            'entries_appended': 0,
            'entries_committed': 0,
            'snapshots_created': 0
        }
    
    def start(self):
        """Start the Raft node"""
        logger.info(f"Starting Raft node {self.node_id}")
        
        with self.lock:
            self.running = True
            
            # Start background threads
            self.election_thread = threading.Thread(target=self._election_timer, daemon=True)
            self.election_thread.start()
            
            self.commit_thread = threading.Thread(target=self._commit_applier, daemon=True)
            self.commit_thread.start()
            
            logger.info(f"Raft node {self.node_id} started as {self.state.value}")
    
    def stop(self):
        """Stop the Raft node"""
        logger.info(f"Stopping Raft node {self.node_id}")
        
        with self.lock:
            self.running = False
            self.commit_cv.notify_all()
        
        # Wait for threads to stop
        if self.election_thread:
            self.election_thread.join(timeout=2)
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=2)
        if self.commit_thread:
            self.commit_thread.join(timeout=2)
        
        # Close RPC connections
        for channel in self.rpc_clients.values():
            channel.close()
        
        logger.info(f"Raft node {self.node_id} stopped")
    
    def handle_client_request(self, command: Dict, client_id: str, request_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Handle a client request.
        Returns (success, result, error).
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                # Not the leader, return leader hint
                return False, None, f"Not leader. Current leader: {self.volatile_state.leader_id}"
            
            # Create log entry
            entry = LogEntry(
                index=self.log.get_last_index() + 1,
                term=self.persistent_state.current_term,
                command=command,
                client_id=client_id,
                request_id=request_id
            )
            
            # Append to our log
            if not self.log.append_entry(entry):
                return False, None, "Failed to append entry to log"
            
            # Create future for this request
            future = threading.Event()
            self.pending_requests[request_id] = (client_id, future, None)
            
            # Start replication immediately
            self._send_append_entries()
            
            # Wait for commit (with timeout)
            if future.wait(timeout=self.config['raft']['request_timeout_ms'] / 1000):
                _, _, result = self.pending_requests.get(request_id, (None, None, None))
                self.pending_requests.pop(request_id, None)
                
                if result and result.get('success'):
                    return True, json.dumps(result), None
                else:
                    return False, None, result.get('error', 'Unknown error')
            else:
                self.pending_requests.pop(request_id, None)
                return False, None, "Request timeout"
    
    def request_vote(self, request: raft_pb2.RequestVoteRequest) -> raft_pb2.RequestVoteResponse:
        """Handle RequestVote RPC"""
        with self.lock:
            # Update term if necessary
            if request.term > self.persistent_state.current_term:
                self._update_term(request.term)
                self._become_follower()
            
            # Default response
            response = raft_pb2.RequestVoteResponse(
                from_node=self.node_id,
                to_node=request.from_node,
                term=self.persistent_state.current_term,
                vote_granted=False
            )
            
            # Don't grant vote if term is old
            if request.term < self.persistent_state.current_term:
                return response
            
            # Check if we can grant vote
            can_vote = (
                self.persistent_state.voted_for is None or
                self.persistent_state.voted_for == request.from_node
            )
            
            # Check if candidate's log is at least as up-to-date as ours
            last_index = self.log.get_last_index()
            last_term = self.log.get_last_term()
            
            log_ok = (
                request.last_log_term > last_term or
                (request.last_log_term == last_term and request.last_log_index >= last_index)
            )
            
            if can_vote and log_ok:
                self.persistent_state.voted_for = request.from_node
                self.storage.save_state(
                    self.persistent_state.current_term,
                    self.persistent_state.voted_for
                )
                response.vote_granted = True
                self._reset_election_timeout()
                
                logger.info(f"Node {self.node_id} voted for {request.from_node} in term {request.term}")
            
            return response
    
    def append_entries(self, request: raft_pb2.AppendEntriesRequest) -> raft_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC"""
        with self.lock:
            # Update term if necessary
            if request.term > self.persistent_state.current_term:
                self._update_term(request.term)
                self._become_follower()
            
            # Default response
            response = raft_pb2.AppendEntriesResponse(
                from_node=self.node_id,
                to_node=request.from_node,
                term=self.persistent_state.current_term,
                success=False,
                match_index=0,
                conflict_index=0,
                conflict_term=0
            )
            
            # Reject if term is old
            if request.term < self.persistent_state.current_term:
                return response
            
            # Reset election timeout
            self._reset_election_timeout()
            
            # Update leader info
            self.volatile_state.leader_id = request.from_node
            
            # Ensure we're a follower
            if self.state != NodeState.FOLLOWER:
                self._become_follower()
            
            # Check if we have the previous entry
            if request.prev_log_index > 0:
                if not self.log.has_entry(request.prev_log_index, request.prev_log_term):
                    # Find conflict for faster reconciliation
                    conflict_index, conflict_term = self.log.find_conflict_index(
                        request.prev_log_index, request.prev_log_term
                    )
                    response.conflict_index = conflict_index
                    response.conflict_term = conflict_term
                    return response
            
            # Append entries
            entries = [self._proto_to_log_entry(e) for e in request.entries]
            if not self.log.append_entries(request.prev_log_index, request.prev_log_term, entries):
                return response
            
            # Update commit index
            if request.leader_commit > self.volatile_state.commit_index:
                last_new_index = request.prev_log_index + len(request.entries)
                self.volatile_state.commit_index = min(request.leader_commit, last_new_index)
                self.commit_cv.notify()
            
            response.success = True
            response.match_index = self.log.get_last_index()
            
            return response
    
    def _become_follower(self):
        """Transition to follower state"""
        if self.state == NodeState.FOLLOWER:
            return
        
        logger.info(f"Node {self.node_id} becoming FOLLOWER")
        self.state = NodeState.FOLLOWER
        
        # Stop heartbeat thread if running
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread = None
    
    def _become_candidate(self):
        """Transition to candidate state and start election"""
        logger.info(f"Node {self.node_id} becoming CANDIDATE")
        
        self.state = NodeState.CANDIDATE
        self.metrics['elections_started'] += 1
        
        # Increment term
        self.persistent_state.current_term += 1
        
        # Vote for self
        self.persistent_state.voted_for = self.node_id
        self.storage.save_state(
            self.persistent_state.current_term,
            self.persistent_state.voted_for
        )
        
        # Reset election timeout
        self._reset_election_timeout()
        
        # Request votes from all peers
        self._request_votes()
    
    def _become_leader(self):
        """Transition to leader state"""
        logger.info(f"Node {self.node_id} becoming LEADER in term {self.persistent_state.current_term}")
        
        self.state = NodeState.LEADER
        self.volatile_state.leader_id = self.node_id
        self.metrics['elections_won'] += 1
        
        # Initialize leader state
        last_index = self.log.get_last_index()
        for peer in self.volatile_state.current_configuration:
            if peer != self.node_id:
                self.volatile_state.next_index[peer] = last_index + 1
                self.volatile_state.match_index[peer] = 0
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_sender, daemon=True)
        self.heartbeat_thread.start()
        
        # Send initial heartbeats
        self._send_append_entries()
    
    def _election_timer(self):
        """Background thread that handles election timeouts"""
        while self.running:
            with self.lock:
                if self.state != NodeState.LEADER:
                    current_time = time.time()
                    if current_time >= self.volatile_state.election_timeout:
                        logger.info(f"Election timeout on node {self.node_id}")
                        self._become_candidate()
            
            time.sleep(0.01)  # Check every 10ms
    
    def _heartbeat_sender(self):
        """Background thread that sends heartbeats when leader"""
        heartbeat_interval = self.config['raft']['heartbeat_interval_ms'] / 1000
        
        while self.running and self.state == NodeState.LEADER:
            self._send_append_entries()
            time.sleep(heartbeat_interval)
    
    def _commit_applier(self):
        """Background thread that applies committed entries to state machine"""
        while self.running:
            with self.lock:
                while self.volatile_state.last_applied < self.volatile_state.commit_index:
                    self.volatile_state.last_applied += 1
                    entry = self.log.get_entry(self.volatile_state.last_applied)
                    
                    if entry:
                        # Apply to state machine
                        result = self.state_machine.apply_command(entry.command)
                        self.metrics['entries_committed'] += 1
                        
                        # Notify waiting client if this is the leader
                        if self.state == NodeState.LEADER and entry.request_id in self.pending_requests:
                            client_id, future, _ = self.pending_requests[entry.request_id]
                            self.pending_requests[entry.request_id] = (client_id, future, result)
                            future.set()
                    
                    # Check if we should create a snapshot
                    if self.volatile_state.last_applied % self.config['raft']['snapshot_interval'] == 0:
                        self._create_snapshot()
                
                # Wait for more commits
                self.commit_cv.wait(timeout=0.1)
    
    def _request_votes(self):
        """Request votes from all peers"""
        votes_received = 1  # Vote for self
        votes_needed = (len(self.volatile_state.current_configuration) + 1) // 2
        
        # Prepare request
        request = raft_pb2.RequestVoteRequest(
            from_node=self.node_id,
            to_node="",  # Will be filled per peer
            term=self.persistent_state.current_term,
            last_log_index=self.log.get_last_index(),
            last_log_term=self.log.get_last_term()
        )
        
        # Send vote requests in parallel
        vote_futures = []
        for peer in self.volatile_state.current_configuration:
            if peer != self.node_id and peer in self.rpc_clients:
                future = threading.Thread(
                    target=self._send_vote_request,
                    args=(peer, request, votes_received, votes_needed),
                    daemon=True
                )
                future.start()
                vote_futures.append(future)
        
        # Wait for responses (with timeout)
        for future in vote_futures:
            future.join(timeout=0.1)
    
    def _send_vote_request(self, peer: str, base_request: raft_pb2.RequestVoteRequest, 
                          votes_received: int, votes_needed: int):
        """Send vote request to a single peer"""
        try:
            stub = self.rpc_clients.get(peer)
            if not stub:
                return
            
            request = raft_pb2.RequestVoteRequest()
            request.CopyFrom(base_request)
            request.to_node = peer
            
            response = stub.RequestVote(request, timeout=0.1)
            
            with self.lock:
                # Check if we're still a candidate and in the same term
                if (self.state != NodeState.CANDIDATE or 
                    self.persistent_state.current_term != request.term):
                    return
                
                # Handle response
                if response.term > self.persistent_state.current_term:
                    self._update_term(response.term)
                    self._become_follower()
                elif response.vote_granted:
                    votes_received += 1
                    if votes_received >= votes_needed:
                        self._become_leader()
                        
        except grpc.RpcError as e:
            logger.debug(f"Vote request to {peer} failed: {e}")
    
    def _send_append_entries(self):
        """Send AppendEntries to all peers"""
        if self.state != NodeState.LEADER:
            return
        
        for peer in self.volatile_state.current_configuration:
            if peer != self.node_id and peer in self.rpc_clients:
                threading.Thread(
                    target=self._send_append_entries_to_peer,
                    args=(peer,),
                    daemon=True
                ).start()
    
    def _send_append_entries_to_peer(self, peer: str):
        """Send AppendEntries to a single peer"""
        try:
            stub = self.rpc_clients.get(peer)
            if not stub:
                return
            
            with self.lock:
                if self.state != NodeState.LEADER:
                    return
                
                # Get entries to send
                next_index = self.volatile_state.next_index.get(peer, 1)
                prev_index = next_index - 1
                prev_term = self.log.get_term_at_index(prev_index) if prev_index > 0 else 0
                
                # Get entries to send (limit batch size)
                max_entries = self.config['raft']['max_append_entries']
                entries = self.log.get_entries_from(next_index, max_entries)
                
                # Create request
                request = raft_pb2.AppendEntriesRequest(
                    from_node=self.node_id,
                    to_node=peer,
                    term=self.persistent_state.current_term,
                    prev_log_index=prev_index,
                    prev_log_term=prev_term,
                    entries=[self._log_entry_to_proto(e) for e in entries],
                    leader_commit=self.volatile_state.commit_index
                )
            
            # Send request (outside lock)
            response = stub.AppendEntries(request, timeout=0.1)
            
            # Handle response
            with self.lock:
                if self.state != NodeState.LEADER:
                    return
                
                if response.term > self.persistent_state.current_term:
                    self._update_term(response.term)
                    self._become_follower()
                elif response.success:
                    # Update match and next indices
                    self.volatile_state.match_index[peer] = response.match_index
                    self.volatile_state.next_index[peer] = response.match_index + 1
                    
                    # Check if we can advance commit index
                    self._advance_commit_index()
                else:
                    # Log mismatch, use conflict info for faster reconciliation
                    if response.conflict_index > 0:
                        # Skip to the beginning of the conflicting term
                        self.volatile_state.next_index[peer] = response.conflict_index
                    else:
                        # Decrement next_index
                        self.volatile_state.next_index[peer] = max(1, self.volatile_state.next_index[peer] - 1)
                        
        except grpc.RpcError as e:
            logger.debug(f"AppendEntries to {peer} failed: {e}")
    
    def _advance_commit_index(self):
        """Advance commit index based on match indices"""
        if self.state != NodeState.LEADER:
            return
        
        # Find the highest index that a majority has
        match_indices = [self.log.get_last_index()]  # Include self
        for peer, match_index in self.volatile_state.match_index.items():
            if peer in self.volatile_state.current_configuration:
                match_indices.append(match_index)
        
        match_indices.sort(reverse=True)
        majority_index = len(self.volatile_state.current_configuration) // 2
        
        new_commit_index = match_indices[majority_index]
        
        # Only commit entries from current term
        if (new_commit_index > self.volatile_state.commit_index and
            self.log.get_term_at_index(new_commit_index) == self.persistent_state.current_term):
            self.volatile_state.commit_index = new_commit_index
            self.commit_cv.notify()
    
    def _create_snapshot(self):
        """Create a snapshot of the current state"""
        try:
            # Get snapshot from state machine
            snapshot_data = self.state_machine.get_snapshot()
            
            # Save snapshot
            self.storage.save_snapshot(
                snapshot_data,
                self.volatile_state.last_applied,
                self.log.get_term_at_index(self.volatile_state.last_applied),
                list(self.volatile_state.current_configuration)
            )
            
            # Compact log
            self.log.compact_log(
                self.volatile_state.last_applied,
                self.log.get_term_at_index(self.volatile_state.last_applied)
            )
            
            self.metrics['snapshots_created'] += 1
            logger.info(f"Created snapshot at index {self.volatile_state.last_applied}")
            
        except Exception as e:
            logger.error(f"Failed to create snapshot: {e}")
    
    def _reset_election_timeout(self):
        """Reset the election timeout to a random value"""
        min_timeout = self.config['raft']['election_timeout_min_ms'] / 1000
        max_timeout = self.config['raft']['election_timeout_max_ms'] / 1000
        timeout = random.uniform(min_timeout, max_timeout)
        self.volatile_state.election_timeout = time.time() + timeout
    
    def _update_term(self, new_term: int):
        """Update current term and reset voted_for"""
        self.persistent_state.current_term = new_term
        self.persistent_state.voted_for = None
        self.storage.save_state(
            self.persistent_state.current_term,
            self.persistent_state.voted_for
        )
    
    def _init_rpc_clients(self):
        """Initialize RPC clients for all peers"""
        for peer_id, peer_addr in self.peers.items():
            try:
                channel = grpc.insecure_channel(peer_addr)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                self.rpc_clients[peer_id] = stub
                logger.info(f"Connected to peer {peer_id} at {peer_addr}")
            except Exception as e:
                logger.error(f"Failed to connect to peer {peer_id}: {e}")
    
    def _load_persistent_state(self):
        """Load persistent state from disk"""
        # Load Raft state
        state_data = self.storage.load_state()
        self.persistent_state.current_term = state_data['current_term']
        self.persistent_state.voted_for = state_data['voted_for']
        
        # Load configuration
        config = self.storage.load_configuration()
        if config:
            self.volatile_state.current_configuration = set(config)
        
        # Load latest snapshot
        snapshot = self.storage.load_latest_snapshot()
        if snapshot:
            self.state_machine.restore_from_snapshot(snapshot['state_machine_data'])
            self.volatile_state.last_applied = snapshot['last_included_index']
            self.volatile_state.commit_index = snapshot['last_included_index']
            
            # Update configuration from snapshot
            if 'configuration' in snapshot:
                self.volatile_state.current_configuration = set(snapshot['configuration'])
    
    def _log_entry_to_proto(self, entry: LogEntry) -> raft_pb2.LogEntry:
        """Convert LogEntry to protobuf"""
        return raft_pb2.LogEntry(
            index=entry.index,
            term=entry.term,
            command=json.dumps(entry.command),
            client_id=entry.client_id,
            request_id=entry.request_id
        )
    
    def _proto_to_log_entry(self, proto: raft_pb2.LogEntry) -> LogEntry:
        """Convert protobuf to LogEntry"""
        return LogEntry(
            index=proto.index,
            term=proto.term,
            command=json.loads(proto.command),
            client_id=proto.client_id,
            request_id=proto.request_id
        )
    
    def get_status(self) -> Dict:
        """Get current node status"""
        with self.lock:
            return {
                'node_id': self.node_id,
                'state': self.state.value,
                'term': self.persistent_state.current_term,
                'leader_id': self.volatile_state.leader_id,
                'commit_index': self.volatile_state.commit_index,
                'last_applied': self.volatile_state.last_applied,
                'log_length': self.log.get_last_index(),
                'configuration': list(self.volatile_state.current_configuration),
                'metrics': self.metrics.copy()
            }