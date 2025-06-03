"""
Enhanced authentication module with refresh token support.
"""

import time
import threading
from typing import Optional, Dict, Tuple
from dataclasses import dataclass
from functools import wraps
import grpc
from functools import wraps


from common.utils import generate_token, is_token_expired
from common.constants import *
from common.logger import get_logger
from lms.database import User

logger = get_logger(__name__)

@dataclass
class TokenPair:
    """Token pair containing access and refresh tokens"""
    access_token: str
    refresh_token: str
    expires_at: float
    created_at: float

class AuthManager:
    """Enhanced auth manager with refresh token support"""
    
    def __init__(self, database, config: Dict):
        self.database = database
        self.config = config
        self.token_expiry_hours = config.get('auth', {}).get('token_expiry_hours', DEFAULT_TOKEN_EXPIRY_HOURS)
        self.refresh_token_expiry_days = config.get('auth', {}).get('refresh_token_expiry_days', 30)
        
        # Token storage: {access_token: TokenPair}
        self.active_tokens = {}
        self.lock = threading.RLock()

    def login(self, username: str, password: str, user_type: str) -> Tuple[bool, Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Authenticate user and generate token pair.
        Returns: (success, access_token, refresh_token, user_id, error)
        """
        try:
            # Authenticate user
            user = self.database.authenticate_user(username, password)
            if not user:
                return False, None, None, None, "Invalid username or password"

            if user.user_type != user_type:
                return False, None, None, None, f"User is not a {user_type}"

            # Generate token pair
            access_token = generate_token()
            refresh_token = generate_token()
            
            current_time = time.time()
            expires_at = current_time + (self.token_expiry_hours * 3600)
            
            token_pair = TokenPair(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=expires_at,
                created_at=current_time
            )
            
            with self.lock:
                self.active_tokens[access_token] = token_pair
                
            # Store tokens in database
            self.database.create_token_pair(user.user_id, access_token, refresh_token, expires_at)
            
            logger.info(f"User {username} logged in successfully")
            return True, access_token, refresh_token, user.user_id, None

        except Exception as e:
            logger.error(f"Login error: {e}")
            return False, None, None, None, str(e)

    def refresh_token(self, refresh_token: str) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
        """
        Refresh access token using refresh token.
        Returns: (success, new_access_token, new_refresh_token, error)
        """
        try:
            # Validate refresh token
            user_id = self.database.get_user_id_by_refresh_token(refresh_token)
            if not user_id:
                return False, None, None, "Invalid refresh token"
            
            # Check if refresh token is expired
            if self.database.is_refresh_token_expired(refresh_token, self.refresh_token_expiry_days):
                self.database.delete_refresh_token(refresh_token)
                return False, None, None, "Refresh token expired"
            
            # Generate new token pair
            new_access_token = generate_token()
            new_refresh_token = generate_token()
            
            current_time = time.time()
            expires_at = current_time + (self.token_expiry_hours * 3600)
            
            token_pair = TokenPair(
                access_token=new_access_token,
                refresh_token=new_refresh_token,
                expires_at=expires_at,
                created_at=current_time
            )
            
            with self.lock:
                # Remove old access token if exists
                old_access_token = self.database.get_access_token_by_refresh_token(refresh_token)
                if old_access_token and old_access_token in self.active_tokens:
                    del self.active_tokens[old_access_token]
                
                # Add new token pair
                self.active_tokens[new_access_token] = token_pair
            
            # Update database (invalidate old tokens and store new ones)
            self.database.refresh_token_pair(user_id, refresh_token, new_access_token, new_refresh_token, expires_at)
            
            logger.info(f"Token refreshed for user {user_id}")
            return True, new_access_token, new_refresh_token, None
            
        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            return False, None, None, str(e)

    def validate_token(self, token: str) -> Optional[User]:
        """Validate access token and return associated user"""
        if not token:
            return None
            
        with self.lock:
            token_pair = self.active_tokens.get(token)
            if token_pair and time.time() < token_pair.expires_at:
                return self.database.get_user_by_token(token)
        
        # Check database if not in memory
        if self.database.is_token_valid(token, self.token_expiry_hours):
            return self.database.get_user_by_token(token)
        
        return None
    
    def can_create_assignment(self, user) -> bool:
        """Check if user can create assignments"""
        return user.user_type == USER_TYPE_INSTRUCTOR
    
    def can_submit_assignment(self, user) -> bool:
        """Check if user can submit assignments"""
        return user.user_type == USER_TYPE_STUDENT
    
    def can_post_query(self, user) -> bool:
        """Check if user can post queries"""
        return user.user_type == USER_TYPE_STUDENT
    
    def can_upload_material(self, user) -> bool:
        """Check if user can upload course materials"""
        return user.user_type == USER_TYPE_INSTRUCTOR
    
    def can_view_all_submissions(self, user) -> bool:
        """Check if user can view all submissions"""
        return user.user_type == USER_TYPE_INSTRUCTOR
    
    def can_view_all_students(self, user) -> bool:
        """Check if user can view all students"""
        return user.user_type == USER_TYPE_INSTRUCTOR
    
    def is_instructor(self, user) -> bool:
        """Check if user is an instructor"""
        return user.user_type == USER_TYPE_INSTRUCTOR
    
    def is_student(self, user) -> bool:
        """Check if user is a student"""
        return user.user_type == USER_TYPE_STUDENT

    def is_token_expired(self, token: str) -> bool:
        """Check if access token is expired"""
        with self.lock:
            token_pair = self.active_tokens.get(token)
            if token_pair:
                return time.time() >= token_pair.expires_at
        
        return not self.database.is_token_valid(token, self.token_expiry_hours)

    def logout(self, token: str) -> bool:
        """Logout user by invalidating token pair"""
        try:
            with self.lock:
                if token in self.active_tokens:
                    del self.active_tokens[token]
            
            self.database.delete_token(token)
            logger.info("Token invalidated successfully")
            return True
        except Exception as e:
            logger.error(f"Logout error: {e}")
            return False

def require_auth(auth_manager_getter):
    """Decorator to require authentication"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, request, context):
            # Get auth manager instance
            auth_manager = auth_manager_getter(self) if callable(auth_manager_getter) else auth_manager_getter
            
            # Extract token from request
            token = getattr(request, 'token', None)
            if not token:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token required")
            
            # Validate token
            user = auth_manager.validate_token(token)
            if not user:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or expired token")
            
            # Add user to context
            context.user = user
            
            return func(self, request, context)
        return wrapper
    return decorator

def require_instructor(auth_manager_getter):
    """Decorator to require instructor authentication"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, request, context):
            # Get auth manager instance
            auth_manager = auth_manager_getter(self) if callable(auth_manager_getter) else auth_manager_getter
            
            # Extract token from request
            token = getattr(request, 'token', None)
            if not token:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token required")
            
            # Validate token
            user = auth_manager.validate_token(token)
            if not user:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or expired token")
            
            # Check if user is instructor
            if user.user_type != USER_TYPE_INSTRUCTOR:
                context.abort(grpc.StatusCode.PERMISSION_DENIED, "Instructor access required")
            
            # Add user to context
            context.user = user
            
            return func(self, request, context)
        return wrapper
    return decorator

def require_student(auth_manager_getter):
    """Decorator to require student authentication"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, request, context):
            # Get auth manager instance
            auth_manager = auth_manager_getter(self) if callable(auth_manager_getter) else auth_manager_getter
            
            # Extract token from request
            token = getattr(request, 'token', None)
            if not token:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token required")
            
            # Validate token
            user = auth_manager.validate_token(token)
            if not user:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or expired token")
            
            # Check if user is student
            if user.user_type != USER_TYPE_STUDENT:
                context.abort(grpc.StatusCode.PERMISSION_DENIED, "Student access required")
            
            # Add user to context
            context.user = user
            
            return func(self, request, context)
        return wrapper
    return decorator