"""
Base client implementation for LMS.
Provides common functionality for student and instructor clients.
"""

import grpc
import json
import os
import time
from typing import Dict, List, Optional, Tuple
from abc import ABC, abstractmethod

# import lms_pb2
# import lms_pb2_grpc
from lms import lms_pb2,lms_pb2_grpc

from common.logger import get_logger
from common.constants import *
from common.utils import chunk_file

logger = get_logger(__name__)


class BaseLMSClient(ABC):
    """
    Base class for LMS clients.
    Provides common functionality for both student and instructor clients.
    """
    
    def __init__(self, server_addresses: List[str], user_type: str):
        """
        Initialize base client.
        
        Args:
            server_addresses: List of LMS server addresses
            user_type: Type of user (STUDENT or INSTRUCTOR)
        """
        self.server_addresses = server_addresses
        self.user_type = user_type
        self.current_server_index = 0
        
        # Authentication state
        self.token = None
        self.user_id = None
        self.username = None
        
        # gRPC channel and stub
        self.channel = None
        self.stub = None
        
        # Connect to a server
        self._connect_to_server()
    
    def _connect_to_server(self) -> bool:
        """Connect to an available LMS server"""
        for i in range(len(self.server_addresses)):
            server_index = (self.current_server_index + i) % len(self.server_addresses)
            address = self.server_addresses[server_index]
            
            try:
                # Close existing connection if any
                if self.channel:
                    self.channel.close()
                
                # Create new connection
                self.channel = grpc.insecure_channel(address)
                self.stub = lms_pb2_grpc.LMSServiceStub(self.channel)
                
                # Test connection with a simple request
                # We'll use login with empty credentials just to check connectivity
                request = lms_pb2.LoginRequest(
                    username="test",
                    password="test",
                    user_type=self.user_type
                )
                
                # Try with a short timeout
                try:
                    self.stub.Login(request, timeout=2)
                    self.current_server_index = server_index
                    logger.info(f"Connected to LMS server at {address}")
                    return True
                except:
                    # Server is reachable, even if login fails
                    self.current_server_index = server_index
                    logger.info(f"Connected to LMS server at {address}")
                    return True
                    
            except Exception as e:
                logger.warning(f"Failed to connect to {address}: {e}")
                continue
        
        logger.error("Failed to connect to any LMS server")
        return False

    def _find_leader(self) -> bool:
        """Find and connect to the current leader"""
        logger.info("Searching for cluster leader...")
        
        # Try each server to find the leader
        for address in self.server_addresses:
            try:
                # Create temporary connection
                channel = grpc.insecure_channel(address)
                
                # Check if this is an LMS server by trying a simple operation
                # We'll use a dummy login request to check server status
                stub = lms_pb2_grpc.LMSServiceStub(channel)
                
                # Try to get server status (this will fail but tell us about leader)
                request = lms_pb2.LoginRequest(
                    username="__status_check__",
                    password="__status_check__",
                    user_type=self.user_type
                )
                
                try:
                    response = stub.Login(request, timeout=2)
                    # If we get here, this server is responsive
                    # It might tell us who the leader is in the error
                    
                    # Connect to this server for now
                    if self.channel:
                        self.channel.close()
                    
                    self.channel = channel
                    self.stub = stub
                    
                    # Try to find leader hint in any operation
                    # For now, we'll just use this server
                    logger.info(f"Connected to LMS server at {address}")
                    return True
                    
                except grpc.RpcError as e:
                    # Server is reachable, might have leader info
                    error_details = str(e.details()) if hasattr(e, 'details') else str(e)
                    
                    # Look for leader hint in error message
                    if "leader:" in error_details.lower():
                        # Extract leader info and try to connect
                        logger.debug(f"Got leader hint from {address}: {error_details}")
                    
                    # For now, if server is reachable, use it
                    if e.code() != grpc.StatusCode.UNAVAILABLE:
                        if self.channel:
                            self.channel.close()
                        
                        self.channel = channel
                        self.stub = stub
                        logger.info(f"Connected to LMS server at {address}")
                        return True
                        
            except Exception as e:
                logger.debug(f"Failed to check {address}: {e}")
                continue
        
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
            
            response = self.stub.Post(request, timeout=30)
            
            if response.success:
                return True, response.id
            else:
                if "not leader" in response.error.lower():
                    # Create a custom exception instead of using grpc.RpcError constructor
                    raise Exception(f"Not leader: {response.error}")
                raise Exception(response.error)
        
        try:
            return self._execute_with_retry(_post_operation)
        except Exception as e:
            logger.error(f"Post data failed: {e}")
            return False, None

    def _is_token_valid(self) -> bool:
        """Check if current token is still valid"""
        if not self.token:
            return False
        
        # You'd need to store token creation time or parse JWT expiry
        # This is a simplified version
        return True  # Implement actual expiry check

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
    
    def login(self, username: str, password: str) -> bool:
        """
        Login to the LMS.
        
        Args:
            username: Username
            password: Password
            
        Returns:
            Success status
        """
        try:
            request = lms_pb2.LoginRequest(
                username=username,
                password=password,
                user_type=self.user_type
            )
            
            response = self.stub.Login(request)
            
            if response.success:
                self.token = response.token
                self.user_id = response.user_id
                self.username = username
                logger.info(f"Successfully logged in as {username}")
                return True
            else:
                logger.error(f"Login failed: {response.error}")
                return False
                
        except grpc.RpcError as e:
            logger.error(f"Login RPC error: {e}")
            if self._handle_rpc_error(e):
                return self.login(username, password)  # Retry
            return False
    
    def logout(self) -> bool:
        """Logout from the LMS"""
        if not self.token:
            return True
        
        try:
            request = lms_pb2.LogoutRequest(token=self.token)
            response = self.stub.Logout(request)
            
            if response.success:
                self.token = None
                self.user_id = None
                self.username = None
                logger.info("Successfully logged out")
                return True
            else:
                logger.error(f"Logout failed: {response.error}")
                return False
                
        except grpc.RpcError as e:
            logger.error(f"Logout RPC error: {e}")
            # Even if logout fails, clear local state
            self.token = None
            self.user_id = None
            self.username = None
            return True
    
    def post_data(self, data_type: str, data: Dict, 
                file_path: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """
        Post data to the LMS with automatic retry and leader finding.
        
        Args:
            data_type: Type of data to post
            data: Data dictionary
            file_path: Optional file to upload
            
        Returns:
            (success, id) tuple
        """
        if not self._ensure_valid_token():
            return False, None
        if not self.token:
            logger.error("Not authenticated")
            return False, None
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Read file if provided
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
                
                response = self.stub.Post(request, timeout=10)
                
                if response.success:
                    logger.info(f"Successfully posted {data_type}")
                    return True, response.id
                else:
                    # Check if error indicates we need to find leader
                    if "not leader" in response.error.lower() or "leader:" in response.error.lower():
                        logger.warning(f"Not connected to leader: {response.error}")
                        
                        # Try to find and connect to leader
                        if retry_count < max_retries - 1:
                            logger.info("Attempting to find cluster leader...")
                            time.sleep(1)  # Brief pause before retry
                            
                            # Rotate to next server
                            self.current_server_index = (self.current_server_index + 1) % len(self.server_addresses)
                            
                            if self._connect_to_server():
                                retry_count += 1
                                continue
                    
                    logger.error(f"Post failed: {response.error}")
                    return False, None
                    
            except grpc.RpcError as e:
                logger.error(f"Post RPC error: {e}")
                
                # If server unavailable, try to reconnect
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    if retry_count < max_retries - 1:
                        logger.info("Server unavailable, trying next server...")
                        self.current_server_index = (self.current_server_index + 1) % len(self.server_addresses)
                        
                        if self._connect_to_server():
                            retry_count += 1
                            continue
                
                return False, None
            
            except Exception as e:
                logger.error(f"Unexpected error in post_data: {e}")
                return False, None
        
        logger.error(f"Failed after {max_retries} retries")
        return False, None

    def get_data(self, data_type: str, filter_data: Optional[Dict] = None) -> List[Dict]:
        """
        Get data from the LMS.
        
        Args:
            data_type: Type of data to retrieve
            filter_data: Optional filter criteria
            
        Returns:
            List of data items
        """
        if not self.token:
            logger.error("Not authenticated")
            return []
        
        try:
            request = lms_pb2.GetRequest(
                token=self.token,
                type=data_type,
                filter=json.dumps(filter_data) if filter_data else ""
            )
            
            response = self.stub.Get(request)
            
            if response.success:
                items = []
                for item in response.items:
                    try:
                        data = json.loads(item.data)
                        data['_id'] = item.id
                        data['_type'] = item.type
                        items.append(data)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse item data: {item.data}")
                return items
            else:
                logger.error(f"Get failed: {response.error}")
                return []
                
        except grpc.RpcError as e:
            logger.error(f"Get RPC error: {e}")
            if self._handle_rpc_error(e):
                return self.get_data(data_type, filter_data)  # Retry
            return []
    
    def upload_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
        """
        Upload a file to the LMS.
        
        Args:
            file_path: Path to file to upload
            
        Returns:
            (success, file_id) tuple
        """
        if not self.token or not os.path.exists(file_path):
            return False, None
        
        try:
            # Create file chunks
            chunks = chunk_file(file_path)
            filename = os.path.basename(file_path)
            
            # Generator for streaming
            def chunk_generator():
                for i, chunk in enumerate(chunks):
                    yield lms_pb2.FileChunk(
                        filename=filename,
                        content=chunk,
                        chunk_number=i,
                        is_last=(i == len(chunks) - 1)
                    )
            
            response = self.stub.UploadFile(chunk_generator())
            
            if response.success:
                logger.info(f"Successfully uploaded file: {filename}")
                return True, response.file_id
            else:
                logger.error(f"Upload failed: {response.error}")
                return False, None
                
        except grpc.RpcError as e:
            logger.error(f"Upload RPC error: {e}")
            return False, None

    def get_connection_status(self) -> Dict:
        """Get current connection status"""
        return {
            'connected': self.is_connected(),
            'authenticated': self.is_authenticated(),
            'current_server': self.server_addresses[self.current_server_index] if self.server_addresses else None,
            'username': self.username
        }
    
    def download_file(self, filename: str, file_type: str, save_path: str) -> bool:
        """
        Download a file from the LMS.
        
        Args:
            filename: Name or ID of file to download
            file_type: Type of file (COURSE_MATERIAL, ASSIGNMENT)
            save_path: Path to save downloaded file
            
        Returns:
            Success status
        """
        if not self.token:
            return False
        
        try:
            request = lms_pb2.FileRequest(
                token=self.token,
                filename=filename,
                file_type=file_type
            )
            
            # Receive file chunks
            chunks = []
            for chunk in self.stub.DownloadFile(request):
                chunks.append(chunk.content)
            
            # Save file
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, 'wb') as f:
                for chunk in chunks:
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded file to: {save_path}")
            return True
            
        except grpc.RpcError as e:
            logger.error(f"Download RPC error: {e}")
            return False
    
    def is_connected(self) -> bool:
        """Check if client is connected to a server"""
        return self.channel is not None and self.stub is not None
    
    def is_authenticated(self) -> bool:
        """Check if client is authenticated"""
        return self.token is not None
    
    @abstractmethod
    def show_menu(self):
        """Show user-specific menu (to be implemented by subclasses)"""
        pass
    
    @abstractmethod
    def handle_menu_choice(self, choice: str):
        """Handle menu choice (to be implemented by subclasses)"""
        pass