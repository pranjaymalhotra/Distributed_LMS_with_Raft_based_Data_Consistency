"""
Raft RPC server implementation.
Handles incoming RPC requests for the Raft protocol.
"""

import grpc
from concurrent import futures
import threading
from typing import Optional

# import raft_pb2
# import raft_pb2_grpc

from raft import raft_pb2
from raft import raft_pb2_grpc
from common.logger import get_logger

logger = get_logger(__name__)


class RaftRPCServer(raft_pb2_grpc.RaftServiceServicer):
    """
    gRPC server for Raft protocol.
    Delegates actual handling to the RaftNode instance.
    """
    
    def __init__(self, raft_node):
        """
        Initialize RPC server with a reference to the Raft node.
        
        Args:
            raft_node: The RaftNode instance that will handle requests
        """
        self.raft_node = raft_node
        self.server = None
        self.port = None
    
    def RequestVote(self, request, context):
        """Handle RequestVote RPC"""
        try:
            logger.debug(f"Received RequestVote from {request.from_node} for term {request.term}")
            return self.raft_node.request_vote(request)
        except Exception as e:
            logger.error(f"Error handling RequestVote: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC"""
        try:
            logger.debug(f"Received AppendEntries from {request.from_node} with {len(request.entries)} entries")
            return self.raft_node.append_entries(request)
        except Exception as e:
            logger.error(f"Error handling AppendEntries: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    def InstallSnapshot(self, request, context):
        """Handle InstallSnapshot RPC"""
        try:
            logger.debug(f"Received InstallSnapshot from {request.from_node}")
            return self.raft_node.install_snapshot(request)
        except Exception as e:
            logger.error(f"Error handling InstallSnapshot: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    def ForwardRequest(self, request, context):
        """Handle client request forwarding from other nodes"""
        try:
            logger.debug(f"Received ForwardRequest for client {request.client_id}")
            
            # Parse command
            import json
            command = json.loads(request.command)
            
            # Handle request
            success, result, error = self.raft_node.handle_client_request(
                command, request.client_id, request.request_id
            )
            
            return raft_pb2.ForwardRequestResponse(
                success=success,
                leader_id=self.raft_node.volatile_state.leader_id,
                error=error or "",
                result=result or ""
            )
            
        except Exception as e:
            logger.error(f"Error handling ForwardRequest: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    def AddServer(self, request, context):
        """Handle adding a new server to the cluster"""
        try:
            logger.info(f"Received AddServer request for {request.new_server}")
            
            # Only leader can handle membership changes
            if self.raft_node.state != self.raft_node.NodeState.LEADER:
                return raft_pb2.AddServerResponse(
                    success=False,
                    error=f"Not leader. Current leader: {self.raft_node.volatile_state.leader_id}"
                )
            
            # Add server (implementation depends on your membership change strategy)
            success, error = self.raft_node.add_server(request.new_server, request.server_address)
            
            return raft_pb2.AddServerResponse(
                success=success,
                error=error or ""
            )
            
        except Exception as e:
            logger.error(f"Error handling AddServer: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    def RemoveServer(self, request, context):
        """Handle removing a server from the cluster"""
        try:
            logger.info(f"Received RemoveServer request for {request.server_id}")
            
            # Only leader can handle membership changes
            if self.raft_node.state != self.raft_node.NodeState.LEADER:
                return raft_pb2.RemoveServerResponse(
                    success=False,
                    error=f"Not leader. Current leader: {self.raft_node.volatile_state.leader_id}"
                )
            
            # Remove server
            success, error = self.raft_node.remove_server(request.server_id)
            
            return raft_pb2.RemoveServerResponse(
                success=success,
                error=error or ""
            )
            
        except Exception as e:
            logger.error(f"Error handling RemoveServer: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    def HealthCheck(self, request, context):
        """Handle health check requests"""
        try:
            status = self.raft_node.get_status()
            
            return raft_pb2.HealthCheckResponse(
                healthy=True,
                state=status['state'],
                current_term=status['term'],
                leader_id=status.get('leader_id', '')
            )
            
        except Exception as e:
            logger.error(f"Error handling HealthCheck: {e}")
            return raft_pb2.HealthCheckResponse(
                healthy=False,
                state="ERROR",
                current_term=0,
                leader_id=""
            )
    
    def start(self, port: int):
        """Start the gRPC server"""
        self.port = port
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[
                ('grpc.max_send_message_length', 50 * 1024 * 1024),  # 50MB
                ('grpc.max_receive_message_length', 50 * 1024 * 1024),  # 50MB
            ]
        )
        
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, self.server)
        
        self.server.add_insecure_port(f'[::]:{port}')
        self.server.start()
        
        logger.info(f"Raft RPC server started on port {port}")
    
    def stop(self):
        """Stop the gRPC server"""
        if self.server:
            self.server.stop(grace=5)
            logger.info("Raft RPC server stopped")


class RaftClient:
    """
    Client for making RPC calls to other Raft nodes.
    Provides a higher-level interface for Raft RPCs.
    """
    
    def __init__(self, target_address: str):
        """
        Initialize client for a specific target node.
        
        Args:
            target_address: Address of the target node (e.g., "localhost:5001")
        """
        self.target_address = target_address
        self.channel = grpc.insecure_channel(target_address)
        self.stub = raft_pb2_grpc.RaftServiceStub(self.channel)
    
    def request_vote(self, request: raft_pb2.RequestVoteRequest, timeout: float = 1.0):
        """Send RequestVote RPC"""
        try:
            return self.stub.RequestVote(request, timeout=timeout)
        except grpc.RpcError as e:
            logger.debug(f"RequestVote to {self.target_address} failed: {e}")
            raise
    
    def append_entries(self, request: raft_pb2.AppendEntriesRequest, timeout: float = 1.0):
        """Send AppendEntries RPC"""
        try:
            return self.stub.AppendEntries(request, timeout=timeout)
        except grpc.RpcError as e:
            logger.debug(f"AppendEntries to {self.target_address} failed: {e}")
            raise
    
    def install_snapshot(self, request: raft_pb2.InstallSnapshotRequest, timeout: float = 5.0):
        """Send InstallSnapshot RPC"""
        try:
            return self.stub.InstallSnapshot(request, timeout=timeout)
        except grpc.RpcError as e:
            logger.debug(f"InstallSnapshot to {self.target_address} failed: {e}")
            raise
    
    def forward_request(self, request: raft_pb2.ForwardRequestMessage, timeout: float = 5.0):
        """Forward a client request to another node"""
        try:
            return self.stub.ForwardRequest(request, timeout=timeout)
        except grpc.RpcError as e:
            logger.debug(f"ForwardRequest to {self.target_address} failed: {e}")
            raise
    
    def health_check(self, timeout: float = 1.0):
        """Check health of the target node"""
        try:
            request = raft_pb2.HealthCheckRequest(node_id="client")
            return self.stub.HealthCheck(request, timeout=timeout)
        except grpc.RpcError as e:
            logger.debug(f"HealthCheck to {self.target_address} failed: {e}")
            raise
    
    def close(self):
        """Close the client connection"""
        self.channel.close()