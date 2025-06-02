"""
Base client implementation for LMS.
Provides common functionality for student and instructor clients.
"""

import grpc
import json
import os
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
    
    def _handle_rpc_error(self, e: grpc.RpcError) -> bool:
        """
        Handle RPC errors and attempt reconnection if needed.
        
        Returns:
            True if should retry, False otherwise
        """
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            logger.warning("Server unavailable, attempting to connect to another server")
            self.current_server_index = (self.current_server_index + 1) % len(self.server_addresses)
            return self._connect_to_server()
        
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
        Post data to the LMS.
        
        Args:
            data_type: Type of data to post
            data: Data dictionary
            file_path: Optional file to upload
            
        Returns:
            (success, id) tuple
        """
        if not self.token:
            logger.error("Not authenticated")
            return False, None
        
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
            
            response = self.stub.Post(request)
            
            if response.success:
                logger.info(f"Successfully posted {data_type}")
                return True, response.id
            else:
                logger.error(f"Post failed: {response.error}")
                return False, None
                
        except grpc.RpcError as e:
            logger.error(f"Post RPC error: {e}")
            if self._handle_rpc_error(e):
                return self.post_data(data_type, data, file_path)  # Retry
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