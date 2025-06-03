"""
Enhanced base client with automatic token refresh capability.
"""

import grpc
import json
import os
import time
import threading
from typing import Dict, List, Optional, Tuple, Callable
from abc import ABC, abstractmethod

from lms import lms_pb2, lms_pb2_grpc
from common.logger import get_logger
from common.constants import *
from common.utils import chunk_file

logger = get_logger(__name__)

class TokenManager:
    """Manages token refresh logic"""
    
    def __init__(self, client_instance):
        self.client = client_instance
        self.lock = threading.RLock()
        self.refresh_threshold = 300  # Refresh 5 minutes before expiry
        self.refresh_timer = None

        
    def schedule_refresh(self, expires_in: int):
        """Schedule automatic token refresh"""
        if self.refresh_timer:
            self.refresh_timer.cancel()
        
        # Schedule refresh before expiry
        refresh_delay = max(expires_in - self.refresh_threshold, 60)  # Minimum 1 minute
        
        self.refresh_timer = threading.Timer(refresh_delay, self._auto_refresh)
        self.refresh_timer.daemon = True
        self.refresh_timer.start()
        
        logger.info(f"Token refresh scheduled in {refresh_delay} seconds")
    
    def _auto_refresh(self):
        """Automatically refresh token"""
        try:
            logger.info("Attempting automatic token refresh...")
            success = self.client._refresh_token()
            if success:
                logger.info("Automatic token refresh successful")
            else:
                logger.warning("Automatic token refresh failed")
        except Exception as e:
            logger.error(f"Auto refresh error: {e}")
    
    def cancel_refresh(self):
        """Cancel scheduled refresh"""
        if self.refresh_timer:
            self.refresh_timer.cancel()
            self.refresh_timer = None

class EnhancedBaseLMSClient(ABC):
    """Enhanced base client with automatic token refresh"""
    
    def __init__(self, server_addresses: List[str], user_type: str):
        self.server_addresses = server_addresses
        self.user_type = user_type
        self.current_server_index = 0
        self.config = None
        self.node_address_map = {}
        
        # Authentication state
        self.token = None
        self.refresh_token = None
        self.user_id = None
        self.username = None
        self.token_expires_at = 0
        
        # gRPC channel and stub
        self.channel = None
        self.stub = None
        
        # Token management
        self.token_manager = TokenManager(self)
        self.auth_lock = threading.RLock()
        
        # Connect to a server
        self._connect_to_server()
    def set_config(self, config: Dict):
        """Set configuration and build node address mapping"""
        self.config = config
        self._build_node_address_map()

    def _build_node_address_map(self):
        """Build mapping from node ID to address from config"""
        if not self.config:
            return
            
        try:
            nodes = self.config.get('cluster', {}).get('nodes', {})
            self.node_address_map = {}
            
            for node_id, node_config in nodes.items():
                address = f"{node_config['host']}:{node_config['lms_port']}"
                self.node_address_map[node_id] = address
                
            logger.info(f"Built node address map: {self.node_address_map}")
            
        except Exception as e:
            logger.error(f"Failed to build node address map: {e}")

    def _get_node_address_from_config(self, node_id: str) -> Optional[str]:
        """Get node address from configuration"""
        return self.node_address_map.get(node_id)


    def _find_leader_sequentially(self) -> bool:
        """Try all configured nodes to find the current leader"""
        if not self.node_address_map:
            # Fallback to original behavior
            return self._connect_to_server()
        
        original_address = self.server_addresses[self.current_server_index] if self.server_addresses else None
        
        # Try each node in config
        for node_id, address in self.node_address_map.items():
            try:
                logger.info(f"Testing node {node_id} at {address} for leadership...")
                
                if self._connect_to_specific_node(node_id):
                    # Test if this is the leader with a lightweight request
                    if self._test_node_leadership():
                        logger.info(f"Found leader: {node_id}")
                        return True
                        
            except Exception as e:
                logger.debug(f"Node {node_id} test failed: {e}")
                continue
        
        logger.error("Could not find any responsive leader in configured nodes")
        return False

    def _connect_to_server(self) -> bool:
        """Connect to an available LMS server"""
        for i in range(len(self.server_addresses)):
            server_index = (self.current_server_index + i) % len(self.server_addresses)
            address = self.server_addresses[server_index]
            
            try:
                if self.channel:
                    self.channel.close()
                
                self.channel = grpc.insecure_channel(address)
                self.stub = lms_pb2_grpc.LMSServiceStub(self.channel)
                
                # Test connection
                try:
                    request = lms_pb2.LoginRequest(
                        username="test",
                        password="test",
                        user_type=self.user_type
                    )
                    self.stub.Login(request, timeout=2)
                    self.current_server_index = server_index
                    logger.info(f"Connected to LMS server at {address}")
                    return True
                except:
                    self.current_server_index = server_index
                    logger.info(f"Connected to LMS server at {address}")
                    return True
                    
            except Exception as e:
                logger.warning(f"Failed to connect to {address}: {e}")
                continue
        
        logger.error("Failed to connect to any LMS server")
        return False

    def _handle_rpc_error(self, e: grpc.RpcError) -> bool:
        """Enhanced error handling with comprehensive debugging"""
        error_message = str(e.details()) if hasattr(e, 'details') else str(e)
        
        # Get status code safely
        status_code = None
        try:
            if hasattr(e, 'code') and callable(getattr(e, 'code')):
                status_code = e.code()
            elif hasattr(e, '_state') and hasattr(e._state, 'code'):
                status_code = e._state.code
        except Exception:
            logger.warning("Could not determine RPC error status code")

        print(f"🔄 DEBUG: Handling RPC error (status: {status_code}): {error_message}")
        logger.info(f"🔄 Handling RPC error (status: {status_code}): {error_message}")

        # Handle "Not leader" errors with smart leader targeting
        if "not leader" in error_message.lower() or "current leader:" in error_message.lower():
            print(f"🚫 DEBUG: 'Not leader' error detected")
            
            # Extract leader from error message
            leader_id_hint = None
            if "current leader:" in error_message.lower():
                try:
                    # Split on "current leader:" and get the part after it
                    leader_part = error_message.lower().split("current leader:")[-1].strip()
                    # Extract just the node ID (first word)
                    leader_id_hint = leader_part.split()[0].strip()
                    print(f"🎯 DEBUG: Extracted leader hint: '{leader_id_hint}'")
                except Exception as ex:
                    print(f"❌ DEBUG: Could not extract leader from error: {ex}")
            
            # Try to connect to hinted leader first
            if leader_id_hint and self.config and leader_id_hint in self.config['cluster']['nodes']:
                print(f"🔄 DEBUG: Attempting to connect to hinted leader '{leader_id_hint}'")
                if self._connect_to_specific_leader(leader_id_hint):
                    print(f"✅ DEBUG: Successfully connected to hinted leader '{leader_id_hint}'")
                    return True
            
            # If hinted leader failed, try to find leader through sequential search
            print(f"🔄 DEBUG: Hinted leader failed, trying sequential leader discovery")
            if self._find_and_connect_to_current_leader():
                print(f"✅ DEBUG: Found leader through sequential search")
                return True
            
            # Final fallback: rotate servers
            print(f"🔄 DEBUG: Falling back to server rotation")
            self.current_server_index = (self.current_server_index + 1) % len(self.server_addresses)
            return self._connect_to_server()
        
        # Handle token expiration
        if status_code == grpc.StatusCode.UNAUTHENTICATED:
            if "Invalid or expired token" in error_message:
                logger.warning("Token expired, attempting refresh...")
                
                with self.auth_lock:
                    # Try to refresh token
                    if self.refresh_token:
                        success = self._refresh_token()
                        if success:
                            logger.info("Token refreshed successfully, retrying request")
                            return True
                        else:
                            logger.error("Token refresh failed")
                    
                    # Clear authentication state
                    self._clear_auth_state()
                    print("\n⚠️ Your session has expired. Please log in again.")
                    return False
        
        # Handle server unavailable
        if status_code == grpc.StatusCode.UNAVAILABLE or "unavailable" in error_message.lower():
            logger.warning("Server unavailable, attempting to connect to another server")
            self.current_server_index = (self.current_server_index + 1) % len(self.server_addresses)
            return self._connect_to_server()
        
        return False

    def _connect_to_specific_leader(self, leader_id: str) -> bool:
        """Connect directly to a specific leader node"""
        try:
            if not self.config or leader_id not in self.config['cluster']['nodes']:
                print(f"❌ DEBUG: Leader {leader_id} not found in config")
                return False
            
            leader_config = self.config['cluster']['nodes'][leader_id]
            leader_address = f"{leader_config['host']}:{leader_config['lms_port']}"
            
            print(f"🔗 DEBUG: Connecting to leader {leader_id} at {leader_address}")
            
            # Close existing connection
            if self.channel:
                self.channel.close()
            
            # Create new connection
            self.channel = grpc.insecure_channel(leader_address)
            self.stub = lms_pb2_grpc.LMSServiceStub(self.channel)
            
            # Brief pause for connection to stabilize
            time.sleep(0.1)
            
            # Verify this is actually the leader
            try:
                leadership_request = lms_pb2.LeadershipRequest()
                leadership_response = self.stub.CheckLeadership(leadership_request, timeout=2)
                
                if not leadership_response.is_leader:
                    print(f"❌ DEBUG: {leader_id} reports it's not the leader (leader: {leadership_response.leader_id})")
                    return False
                
                if leadership_response.leader_id != leader_id:
                    print(f"❌ DEBUG: {leader_id} reports different leader: {leadership_response.leader_id}")
                    return False
                
                print(f"✅ DEBUG: Verified {leader_id} is the current leader")
                
                # Test with an authenticated request if we have a token
                if self.token:
                    try:
                        test_request = lms_pb2.GetRequest(
                            token=self.token,
                            type="assignments"
                        )
                        test_response = self.stub.Get(test_request, timeout=3)
                        print(f"✅ DEBUG: Authentication test successful on {leader_id}")
                        return True
                    except grpc.RpcError as e:
                        print(f"❌ DEBUG: Authentication test failed on {leader_id}: {e}")
                        return False
                else:
                    # No token to test with, but leadership is confirmed
                    return True
                    
            except grpc.RpcError as e:
                print(f"❌ DEBUG: Failed to verify leadership of {leader_id}: {e}")
                return False
                
        except Exception as e:
            print(f"❌ DEBUG: Error connecting to leader {leader_id}: {e}")
            return False

    def _find_and_connect_to_current_leader(self) -> bool:
        """Find and connect to current leader with proper connection update"""
        print(f"🔍 DEBUG: Searching for current leader...")
        
        # Testing all nodes to identify leader
        for i, address in enumerate(self.server_addresses):
            try:
                channel = grpc.insecure_channel(address)
                stub = lms_pb2_grpc.LMSServiceStub(channel)
                
                # Check leadership - using LeadershipRequest if available
                try:
                    leadership_response = stub.CheckLeadership(
                        lms_pb2.LeadershipRequest(), 
                        timeout=5
                    )
                    
                    if leadership_response.is_leader:
                        # Found leader - update connection details
                        if self.channel:
                            self.channel.close()
                        
                        print(f"✅ DEBUG: Connected to leader at {address}")
                        self.current_server_index = i
                        self.channel = channel
                        self.stub = stub  # ✅ This is critical - update the stub!
                        return True
                        
                    elif leadership_response.leader_id:
                        # Node indicated another node is leader
                        leader_id = leadership_response.leader_id
                        
                        # Find leader address from config
                        for j, addr in enumerate(self.server_addresses):
                            # Try to connect to the indicated leader
                            leader_channel = grpc.insecure_channel(addr)
                            leader_stub = lms_pb2_grpc.LMSServiceStub(leader_channel)
                            
                            try:
                                leader_check = leader_stub.CheckLeadership(
                                    lms_pb2.LeadershipRequest(), timeout=5
                                )
                                
                                if leader_check.is_leader:
                                    # Found the real leader
                                    if self.channel:
                                        self.channel.close()
                                    
                                    print(f"✅ DEBUG: Connected to leader at {addr}")
                                    self.current_server_index = j
                                    self.channel = leader_channel
                                    self.stub = leader_stub  # ✅ Critical update
                                    return True
                            except:
                                continue
                except:
                    # Older protocol without CheckLeadership
                    # Try with a Get request to see if we get "not leader" error
                    try:
                        test_req = lms_pb2.GetRequest(
                            token=self.token or "test_token",
                            type="ASSIGNMENTS",
                            filter=""
                        )
                        stub.Get(test_req, timeout=2)
                        
                        # If we get here, this node might be the leader
                        # or it's forwarding requests properly
                        if self.channel:
                            self.channel.close()
                        
                        print(f"✅ DEBUG: Connected to node at {address}")
                        self.current_server_index = i
                        self.channel = channel
                        self.stub = stub  # ✅ Update the stub
                        return True
                    except:
                        pass
                        
            except Exception as e:
                print(f"🔧 DEBUG: Error checking {address}: {e}")
                continue
                
        print("❌ DEBUG: Could not connect to any leader")
        return False

    def _force_connect_to_leader_after_login(self):
        """Force connection to leader after successful login"""
        if not self.config or not self.token:
            return False
        
        print("🔄 DEBUG: Attempting to connect to leader after login...")
        
        # Try to find and connect to the current leader
        for node_id, node_config in self.config['cluster']['nodes'].items():
            address = f"{node_config['host']}:{node_config['lms_port']}"
            
            try:
                print(f"🧪 DEBUG: Testing {node_id} at {address} for leadership...")
                
                # Create temporary channel for this test
                channel = grpc.insecure_channel(address)
                stub = lms_pb2_grpc.LMSServiceStub(channel)
                
                # Test leadership first (this doesn't require auth)
                request = lms_pb2.LeadershipRequest()
                response = stub.CheckLeadership(request, timeout=5)
                
                if response.is_leader:
                    print(f"✅ DEBUG: Found leader {node_id}, connecting...")
                    
                    # Close the temporary channel
                    channel.close()
                    
                    # Now connect our main client to this leader
                    if self.channel:
                        self.channel.close()
                    
                    self.channel = grpc.insecure_channel(address)
                    self.stub = lms_pb2_grpc.LMSServiceStub(self.channel)
                    
                    # Test that our authenticated connection works
                    try:
                        # Use a simple, safe get request
                        get_request = lms_pb2.GetRequest(
                            token=self.token,
                            type="assignments",
                            filter=""  # ✅ Empty filter is valid
                        )
                        test_response = self.stub.Get(get_request, timeout=3)
                        
                        # Don't check for success, just check it doesn't crash
                        print(f"✅ DEBUG: Successfully connected to leader {node_id} with authentication")
                        return True
                    except grpc.RpcError as e:
                        print(f"❌ DEBUG: Auth test failed on {node_id}: {e}")
                        # Continue to next node
                    
                else:
                    print(f"🔵 DEBUG: {node_id} is not the leader (leader: {response.leader_id})")
                    channel.close()
                    
            except Exception as e:
                print(f"❌ DEBUG: Error testing {node_id}: {e}")
                if 'channel' in locals():
                    channel.close()
        
        print("❌ DEBUG: Could not connect to any leader")
        return False

    def debug_cluster_status(self):
        """Debug method to check the status of all nodes"""
        print("\n" + "="*60)
        print("🔍 COMPREHENSIVE CLUSTER STATUS DEBUG")
        print("="*60)
        
        if not self.config:
            print("❌ No configuration loaded")
            return
        
        for node_id, node_config in self.config['cluster']['nodes'].items():
            address = f"{node_config['host']}:{node_config['lms_port']}"
            print(f"\n📍 Testing {node_id} at {address}")
            
            try:
                # Test basic connectivity
                channel = grpc.insecure_channel(address)
                stub = lms_pb2_grpc.LMSServiceStub(channel)
                
                # Test leadership
                request = lms_pb2.LeadershipRequest()
                response = stub.CheckLeadership(request, timeout=5)
                
                status = "🟢 LEADER" if response.is_leader else "🔵 FOLLOWER"
                print(f"   {status} (leader_id: {response.leader_id})")
                
                # Only test request handling if this is the leader AND we have a valid token
                if response.is_leader and self.token:
                    print(f"   🧪 Testing authenticated request handling...")
                    get_request = lms_pb2.GetRequest(
                        token=self.token,
                        type="assignments"  # Remove filter field
                    )
                    get_response = stub.Get(get_request, timeout=3)
                    if get_response.success or "not leader" not in get_response.error.lower():
                        print(f"   ✅ Can handle authenticated requests")
                    else:
                        print(f"   ❌ Cannot handle requests: {get_response.error}")
                elif response.is_leader:
                    print(f"   ⚠️ Leader found but no token available for testing")
                
                channel.close()
                
            except Exception as e:
                print(f"   ❌ UNREACHABLE ({e})")
        
        print("="*60)

    def _refresh_token(self) -> bool:
        """Attempt to refresh the access token"""
        try:
            if not self.refresh_token:
                print("🔧 DEBUG: No refresh token available")
                return False
            
            print("🔄 Refreshing access token...")
            
            request = lms_pb2.RefreshTokenRequest(
                refresh_token=self.refresh_token
            )
            
            response = self.stub.RefreshToken(request, timeout=10)
            
            if response.success:
                with self.auth_lock:
                    self.token = response.access_token
                    self.refresh_token = response.refresh_token
                    self.token_expires_at = time.time() + response.expires_in
                
                print("✅ Token refreshed successfully")
                return True
            else:
                print(f"❌ Token refresh failed: {response.error}")
                return False
                
        except grpc.RpcError as e:
            print(f"❌ Token refresh RPC error: {e}")
            return False
        except Exception as e:
            print(f"❌ Token refresh error: {e}")
            return False
    
    def _clear_auth_state(self):
        """Clear authentication state"""
        with self.auth_lock:
            self.token = None
            self.refresh_token = None
            self.user_id = None
            self.username = None
            self.token_expires_at = 0
            self.token_manager.cancel_refresh()

    def _is_token_expired(self) -> bool:
        """Check if current token is expired or about to expire"""
        if not self.token or not self.token_expires_at:
            return True
        
        # Consider token expired if it expires within 1 minute
        return time.time() >= (self.token_expires_at - 60)

    def _ensure_valid_token(self) -> bool:
        """Ensure we have a valid token"""
        with self.auth_lock:
            if not self.token:
                print("🔧 DEBUG: No token available")
                return False
            
            # Check if token is expired (simple time-based check)
            if self.token_expires_at and time.time() > self.token_expires_at:
                print("🔧 DEBUG: Token expired, attempting refresh...")
                return self._refresh_token()
            
            print(f"🔧 DEBUG: Token appears valid: {self.token[:20]}...")
            return True

    def _execute_with_retry(self, operation: Callable, *args, **kwargs):
        """Execute operation with automatic retry on auth failure and leader change"""
        max_retries = 5  # Increased retries for leader changes
        
        for attempt in range(max_retries):
            try:
                print(f"🔄 DEBUG: Attempt {attempt + 1}/{max_retries}")
                
                # Ensure we have valid authentication
                if not self._ensure_valid_token():
                    print(f"❌ DEBUG: No valid token for attempt {attempt + 1}")
                    return False, None
                
                # Execute the operation
                return operation(*args, **kwargs)
                
            except grpc.RpcError as e:
                print(f"🚨 DEBUG: RPC error on attempt {attempt + 1}: {e}")
                
                # Try to handle the error
                if self._handle_rpc_error(e):
                    print(f"✅ DEBUG: Error handled, retrying...")
                    time.sleep(0.5)  # Brief pause before retry
                    continue
                else:
                    print(f"❌ DEBUG: Could not handle error: {e}")
                    break
                    
            except Exception as e:
                print(f"❌ DEBUG: Unexpected error on attempt {attempt + 1}: {e}")
                break
        
        print(f"❌ DEBUG: Max retries ({max_retries}) exceeded")
        raise Exception(f"Operation failed after {max_retries} attempts")
    
    def login(self, username: str, password: str) -> bool:
        """Login with improved token handling"""
        try:
            if not self.is_connected():
                print("❌ Not connected to any server")
                return False

            print(f"🔧 DEBUG: Attempting login to: {self.server_addresses[self.current_server_index]}")
            
            request = lms_pb2.LoginRequest(
                username=username,
                password=password,
                user_type=self.user_type
            )
            
            response = self.stub.Login(request, timeout=10)
            
            if response.success:
                print(f"🔧 DEBUG: Login successful, received token: {response.token[:20]}...")
                
                # Store authentication state
                with self.auth_lock:
                    self.token = response.token
                    self.user_id = response.user_id
                    self.username = username
                    self.token_expires_at = time.time() + (1 * 3600)  # 1 hour from config
                
                print(f"✅ Successfully logged in as {username}")
                
                # Force connection to leader after successful login
                self._force_connect_to_leader_after_login()
                
                return True
            else:
                print(f"❌ Login failed: {response.error}")
                return False
                
        except grpc.RpcError as e:
            print(f"❌ Login RPC error: {e}")
            return False
        except Exception as e:
            print(f"❌ Login error: {e}")
            return False

    def logout(self) -> bool:
        """Enhanced logout"""
        if not self.token:
            return True
        
        try:
            request = lms_pb2.LogoutRequest(token=self.token)
            response = self.stub.Logout(request)
            
            success = response.success
            if not success:
                logger.error(f"Logout failed: {response.error}")
            
        except grpc.RpcError as e:
            logger.error(f"Logout RPC error: {e}")
            success = True  # Consider it successful even if server call fails
        
        finally:
            self._clear_auth_state()
            logger.info("Logged out successfully")
        
        return success

    def _extract_leader_from_error(self, error_message: str) -> Optional[str]:
        """Extract leader node ID from error message"""
        try:
            if "Current leader:" in error_message:
                leader_part = error_message.split("Current leader:")[-1].strip()
                leader_node = leader_part.split()[0].strip()
                logger.info(f"🔍 Extracted leader node: '{leader_node}' from error: '{error_message}'")
                print(f"🔍 DEBUG: Extracted leader node: '{leader_node}'")
                return leader_node
        except Exception as e:
            logger.debug(f"Could not extract leader from error: {e}")
            print(f"❌ DEBUG: Could not extract leader from error: {e}")
        return None
    def _connect_to_leader_from_hint(self, error_message: str) -> bool:
        """Connect to leader using hint from error message"""
        try:
            leader_node = self._extract_leader_from_error(error_message)
            if leader_node:
                logger.info(f"🎯 Attempting to connect to identified leader: {leader_node}")
                logger.info(f"📊 Current node address map: {self.node_address_map}")
                
                success = self._connect_to_specific_node(leader_node)
                if success:
                    logger.info(f"✅ Successfully connected to leader {leader_node}")
                    return True
                else:
                    logger.warning(f"❌ Failed to connect to leader {leader_node}")
                    
            else:
                logger.warning("❌ Could not extract leader node from error message")
                
        except Exception as e:
            logger.error(f"Failed to connect using leader hint: {e}")
        return False

    def _test_node_leadership(self) -> bool:
        """Test if current node is actually the leader using dedicated endpoint"""
        if not self.stub:
            print(f"❌ DEBUG: No stub available for leadership test")
            return False
            
        try:
            print(f"🧪 DEBUG: Testing leadership with dedicated endpoint")
            
            # Use the dedicated leadership check endpoint
            request = lms_pb2.LeadershipRequest()
            response = self.stub.CheckLeadership(request, timeout=3)
            
            print(f"🧪 DEBUG: Leadership response - is_leader: {response.is_leader}, leader_id: {response.leader_id}")
            
            return response.is_leader
            
        except grpc.RpcError as e:
            error_message = str(e.details()) if hasattr(e, 'details') else str(e)
            print(f"❌ DEBUG: Leadership test RPC error: {error_message}")
            
            # Fallback to checking error message
            if "not leader" in error_message.lower():
                return False
            # If endpoint doesn't exist, assume this could be leader
            return True
        except Exception as e:
            print(f"❌ DEBUG: Leadership test failed with exception: {e}")
            return False

    def _verify_leader_connection(self, node_id: str) -> bool:
        """Comprehensively verify that we can communicate with the leader"""
        if not self.stub:
            print(f"❌ DEBUG: No stub available for {node_id}")
            return False
            
        try:
            print(f"🧪 DEBUG: Step 1 - Testing basic connectivity to {node_id}")
            
            # Test 1: Basic leadership check
            request = lms_pb2.LeadershipRequest()
            response = self.stub.CheckLeadership(request, timeout=5)
            
            print(f"🧪 DEBUG: Leadership response - is_leader: {response.is_leader}, leader_id: {response.leader_id}")
            
            if not response.is_leader:
                print(f"❌ DEBUG: {node_id} reports it's not the leader (leader_id: {response.leader_id})")
                return False
            
            print(f"✅ DEBUG: Step 1 passed - {node_id} confirms it's the leader")
            
            # Test 2: Authentication check (if we have a token)
            if self.token:
                print(f"🧪 DEBUG: Step 2 - Testing authenticated request to {node_id}")
                
                # Try a simple GET request
                get_request = lms_pb2.GetRequest(
                    token=self.token,
                    type="ASSIGNMENT",
                    filter=""
                )
                
                get_response = self.stub.Get(get_request, timeout=5)
                
                if get_response.success or "not leader" not in get_response.error.lower():
                    print(f"✅ DEBUG: Step 2 passed - {node_id} handled authenticated request")
                    return True
                else:
                    print(f"❌ DEBUG: Step 2 failed - {node_id} auth request error: {get_response.error}")
                    return False
            else:
                print(f"⚠️ DEBUG: No token available, skipping auth test - assuming leadership check is sufficient")
                return True
                
        except grpc.RpcError as e:
            error_message = str(e.details()) if hasattr(e, 'details') else str(e)
            print(f"❌ DEBUG: {node_id} RPC error during verification: {error_message}")
            
            # Check if it's still a "not leader" error
            if "not leader" in error_message.lower():
                print(f"❌ DEBUG: {node_id} still reports not leader via RPC error")
                return False
            
            # For other RPC errors, the node might still be the leader but having issues
            print(f"⚠️ DEBUG: {node_id} has RPC issues but might still be leader")
            return False
            
        except Exception as e:
            print(f"❌ DEBUG: {node_id} verification failed with exception: {e}")
            return False
    
    def _connect_to_specific_node(self, node_id: str) -> bool:
        """Connect directly to a specific node by ID with comprehensive verification"""
        try:
            target_address = self._get_node_address_from_config(node_id)
            if not target_address:
                logger.warning(f"❌ Unknown node ID in config: {node_id}")
                print(f"❌ DEBUG: Unknown node ID '{node_id}' in config")
                print(f"📊 DEBUG: Available nodes: {self.node_address_map}")
                return False

            logger.info(f"🔗 Attempting to connect to node {node_id} at {target_address}")
            print(f"🔗 DEBUG: Connecting to {node_id} at {target_address}")

            # Close existing connection
            if self.channel:
                self.channel.close()

            # Connect to target node
            self.channel = grpc.insecure_channel(target_address)
            self.stub = lms_pb2_grpc.LMSServiceStub(self.channel)

            # Update server index
            if target_address in self.server_addresses:
                old_index = self.current_server_index
                self.current_server_index = self.server_addresses.index(target_address)
                print(f"📍 DEBUG: Updated server index from {old_index} to {self.current_server_index}")

            # CRITICAL: Verify this is actually the leader and ready
            success = self._verify_leader_connection(node_id)
            
            if success:
                logger.info(f"✅ Successfully connected and verified leader {node_id}")
                print(f"✅ DEBUG: Confirmed {node_id} is ready for requests")
                return True
            else:
                logger.warning(f"❌ Node {node_id} leadership verification failed")
                print(f"❌ DEBUG: {node_id} failed leadership verification")
                return False

        except Exception as e:
            logger.error(f"❌ Failed to connect to node {node_id}: {e}")
            print(f"❌ DEBUG: Exception connecting to {node_id}: {e}")
            return False
    def _test_connection_to_node(self, node_id: str) -> bool:
        """Test connection to a specific node"""
        try:
            # Try a simple Get request to test if this node can handle requests
            request = lms_pb2.GetRequest(
                token=self.token or "test",
                type="ASSIGNMENT",
                filter=""
            )
            
            response = self.stub.Get(request, timeout=5)
            
            # Check if we get a "not leader" error
            if response.success:
                logger.info(f"✅ Node {node_id} responded successfully - likely the leader")
                return True
            elif "not leader" in response.error.lower():
                logger.warning(f"❌ Node {node_id} is not the leader: {response.error}")
                return False
            else:
                # Other errors might indicate this could still be the leader
                logger.info(f"⚠️ Node {node_id} responded with error (might still be leader): {response.error}")
                return True
                
        except grpc.RpcError as e:
            error_message = str(e.details()) if hasattr(e, 'details') else str(e)
            if "not leader" in error_message.lower():
                logger.warning(f"❌ Node {node_id} RPC error indicates not leader: {error_message}")
                return False
            logger.warning(f"⚠️ Node {node_id} RPC error (connection might be OK): {error_message}")
            return True
        except Exception as e:
            logger.error(f"❌ Connection test failed for node {node_id}: {e}")
            return False
        
    def post_data(self, data_type: str, data: Dict, file_path: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Enhanced post data with automatic retry"""
        def _post_operation():
            file_data = b""
            filename = ""
            if file_path and os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                filename = os.path.basename(file_path)
            
            request = lms_pb2.PostRequest(
                token=self.token,
                type=data_type,
                data=json.dumps(data),
                file_data=file_data,
                filename=filename
            )
            
            print(f"📤 DEBUG: Sending {data_type} request to current server")
            response = self.stub.Post(request, timeout=150)  # Increased timeout from 30 to 15
            
            if response.success:
                print(f"✅ DEBUG: Successfully posted {data_type}")
                return True, response.id
            else:
                print(f"❌ DEBUG: Post failed: {response.error}")
                # This will trigger error handling in _execute_with_retry
                if "not leader" in response.error.lower():
                    raise Exception(f"Not leader: {response.error}")
                else:
                    raise Exception(response.error)
        
        try:
            return self._execute_with_retry(_post_operation)
        except Exception as e:
            logger.error(f"Post data failed: {e}")
            return False, None
    
    def get_data(self, data_type: str, filter_data: Optional[Dict] = None) -> List[Dict]:
        """Enhanced get data with automatic retry"""
        def _get_operation():
            request = lms_pb2.GetRequest(
                token=self.token,
                type=data_type,
                filter=json.dumps(filter_data) if filter_data else ""  # ✅ Correct field name
            )
            
            response = self.stub.Get(request, timeout=10)
            
            if response.success:
                result = []
                for item in response.items:
                    try:
                        data = json.loads(item.data)
                        result.append(data)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in response item: {item.data}")
                return result
            else:
                if "not leader" in response.error.lower():
                    raise Exception(f"Not leader: {response.error}")
                else:
                    raise Exception(response.error)
        
        try:
            return self._execute_with_retry(_get_operation)
        except Exception as e:
            logger.error(f"Get data failed: {e}")
            return []
    # Abstract methods remain the same
    @abstractmethod
    def show_menu(self):
        pass

    @abstractmethod
    def handle_menu_choice(self, choice: str):
        pass

    # Utility methods
    def is_connected(self) -> bool:
        return self.channel is not None and self.stub is not None

    def is_authenticated(self) -> bool:
        return self.token is not None and not self._is_token_expired()

    def get_connection_status(self) -> Dict:
        return {
            'connected': self.is_connected(),
            'authenticated': self.is_authenticated(),
            'current_server': self.server_addresses[self.current_server_index] if self.server_addresses else None,
            'username': self.username,
            'token_expires_at': self.token_expires_at
        }
