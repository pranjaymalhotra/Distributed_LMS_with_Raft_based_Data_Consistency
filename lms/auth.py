"""
Authentication and authorization module for LMS.
Handles user authentication, token management, and permission checks.
"""

from typing import Optional, Dict
from functools import wraps
import grpc

from common.utils import generate_token, is_token_expired
from common.constants import *
from common.logger import get_logger
from lms.database import User

logger = get_logger(__name__)


class AuthManager:
    """
    Manages authentication and authorization for the LMS.
    """
    
    def __init__(self, database, config: Dict):
        """
        Initialize the auth manager.
        
        Args:
            database: LMSDatabase instance
            config: Configuration dictionary
        """
        self.database = database
        self.config = config
        self.token_expiry_hours = config.get('auth', {}).get('token_expiry_hours', DEFAULT_TOKEN_EXPIRY_HOURS)
    
    def login(self, username: str, password: str, user_type: str) -> tuple:
        """
        Authenticate a user and generate a token.
        
        Returns:
            (success, token, user_id, error)
        """
        try:
            # Authenticate user
            user = self.database.authenticate_user(username, password)
            
            if not user:
                return False, None, None, "Invalid username or password"
            
            # Check user type
            if user.user_type != user_type:
                return False, None, None, f"User is not a {user_type}"
            
            # Generate token
            token = generate_token()
            
            # Store token
            self.database.create_token(user.user_id, token)
            
            logger.info(f"User {username} logged in successfully")
            return True, token, user.user_id, None
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False, None, None, str(e)
    
    def logout(self, token: str) -> bool:
        """
        Logout a user by invalidating their token.
        
        Returns:
            Success status
        """
        try:
            self.database.delete_token(token)
            logger.info(f"Token invalidated successfully")
            return True
        except Exception as e:
            logger.error(f"Logout error: {e}")
            return False
    
    def validate_token(self, token: str) -> Optional[User]:
        """
        Validate a token and return the associated user.
        
        Returns:
            User object if valid, None otherwise
        """
        if not token:
            return None
        
        # Check if token exists and is not expired
        if not self.database.is_token_valid(token, self.token_expiry_hours):
            return None
        
        # Get user
        return self.database.get_user_by_token(token)
    
    def is_instructor(self, user: User) -> bool:
        """Check if user is an instructor"""
        return user.user_type == USER_TYPE_INSTRUCTOR
    
    def is_student(self, user: User) -> bool:
        """Check if user is a student"""
        return user.user_type == USER_TYPE_STUDENT
    
    def can_create_assignment(self, user: User) -> bool:
        """Check if user can create assignments"""
        return self.is_instructor(user)
    
    def can_grade_assignment(self, user: User) -> bool:
        """Check if user can grade assignments"""
        return self.is_instructor(user)
    
    def can_upload_material(self, user: User) -> bool:
        """Check if user can upload course materials"""
        return self.is_instructor(user)
    
    def can_submit_assignment(self, user: User) -> bool:
        """Check if user can submit assignments"""
        return self.is_student(user)
    
    def can_post_query(self, user: User) -> bool:
        """Check if user can post queries"""
        return self.is_student(user)
    
    def can_answer_query(self, user: User) -> bool:
        """Check if user can answer queries"""
        return self.is_instructor(user)
    
    def can_view_all_submissions(self, user: User) -> bool:
        """Check if user can view all submissions"""
        return self.is_instructor(user)
    
    def can_view_submission(self, user: User, submission) -> bool:
        """Check if user can view a specific submission"""
        # Instructors can view all submissions
        if self.is_instructor(user):
            return True
        
        # Students can only view their own submissions
        return submission.student_id == user.user_id
    
    def can_view_all_students(self, user: User) -> bool:
        """Check if user can view all students"""
        return self.is_instructor(user)


# def require_auth(auth_manager: AuthManager):
#     """
#     Decorator for gRPC methods that require authentication.
#     """
#     def decorator(func):
#         @wraps(func)
#         def wrapper(self, request, context):
#             # Extract token from request
#             token = getattr(request, 'token', None)
            
#             if not token:
#                 context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_NOT_AUTHENTICATED)
            
#             # Validate token
#             user = auth_manager.validate_token(token)
            
#             if not user:
#                 context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_INVALID_TOKEN)
            
#             # Add user to context
#             context.user = user
            
#             # Call the actual method
#             return func(self, request, context)
        
#         return wrapper
#     return decorator


# def require_instructor(auth_manager: AuthManager):
#     """
#     Decorator for gRPC methods that require instructor privileges.
#     """
#     def decorator(func):
#         @wraps(func)
#         def wrapper(self, request, context):
#             # First check authentication
#             token = getattr(request, 'token', None)
            
#             if not token:
#                 context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_NOT_AUTHENTICATED)
            
#             # Validate token
#             user = auth_manager.validate_token(token)
            
#             if not user:
#                 context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_INVALID_TOKEN)
            
#             # Check if user is instructor
#             if not auth_manager.is_instructor(user):
#                 context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
            
#             # Add user to context
#             context.user = user
            
#             # Call the actual method
#             return func(self, request, context)
        
#         return wrapper
#     return decorator

def require_auth(auth_manager_provider):
    """
    Decorator for gRPC methods that require authentication.
    `auth_manager_provider` can be:
      • an AuthManager instance, or
      • a callable (e.g. lambda self: self.auth_manager) returning AuthManager.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, request, context):
            # Determine the real AuthManager
            if callable(auth_manager_provider):
                manager = auth_manager_provider(self)
            else:
                manager = auth_manager_provider

            # Extract token from request
            token = getattr(request, "token", None)
            if not token:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_NOT_AUTHENTICATED)

            # Validate token via AuthManager
            user = manager.validate_token(token)
            if not user:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_INVALID_TOKEN)

            # Store the authenticated user on the context and call the RPC
            context.user = user
            return func(self, request, context)
        return wrapper
    return decorator


def require_instructor(auth_manager_provider):
    """
    Decorator for gRPC methods that require instructor privileges.
    `auth_manager_provider` can be:
      • an AuthManager instance, or
      • a callable (e.g. lambda self: self.auth_manager) returning AuthManager.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, request, context):
            # Determine the real AuthManager
            if callable(auth_manager_provider):
                manager = auth_manager_provider(self)
            else:
                manager = auth_manager_provider

            # Extract token from request
            token = getattr(request, "token", None)
            if not token:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_NOT_AUTHENTICATED)

            # Validate token via AuthManager
            user = manager.validate_token(token)
            if not user:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_INVALID_TOKEN)

            # Check instructor role
            if not manager.is_instructor(user):
                context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)

            # Store the authenticated user on the context and call the RPC
            context.user = user
            return func(self, request, context)
        return wrapper
    return decorator



def require_student(auth_manager: AuthManager):
    """
    Decorator for gRPC methods that require student privileges.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, request, context):
            # First check authentication
            token = getattr(request, 'token', None)
            
            if not token:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_NOT_AUTHENTICATED)
            
            # Validate token
            user = auth_manager.validate_token(token)
            
            if not user:
                context.abort(grpc.StatusCode.UNAUTHENTICATED, ERROR_INVALID_TOKEN)
            
            # Check if user is student
            if not auth_manager.is_student(user):
                context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
            
            # Add user to context
            context.user = user
            
            # Call the actual method
            return func(self, request, context)
        
        return wrapper
    return decorator