"""
Database layer for LMS.
Provides an interface between the LMS server and the Raft state machine.
"""

import os
import json
import time
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime

from common.utils import (
    generate_id, hash_password, verify_password, 
    timestamp, load_json_file, save_json_file
)
from common.constants import *
from common.logger import get_logger

logger = get_logger(__name__)


@dataclass
class User:
    """User data model"""
    user_id: str
    username: str
    password_hash: str
    user_type: str
    name: str
    created_at: float
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Assignment:
    """Assignment data model"""
    assignment_id: str
    title: str
    description: str
    due_date: str
    created_by: str
    created_at: float
    file_path: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Submission:
    """Assignment submission data model"""
    submission_id: str
    assignment_id: str
    student_id: str
    content: str
    file_path: Optional[str]
    submitted_at: float
    grade: Optional[float] = None
    feedback: Optional[str] = None
    graded_by: Optional[str] = None
    graded_at: Optional[float] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Query:
    """Student query data model"""
    query_id: str
    student_id: str
    query: str
    use_llm: bool
    posted_at: float
    answer: Optional[str] = None
    answered_by: Optional[str] = None
    answered_at: Optional[float] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class CourseMaterial:
    """Course material data model"""
    material_id: str
    filename: str
    file_type: str
    topic: str
    file_path: str
    uploaded_by: str
    uploaded_at: float
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class StudentProgress:
    """Student progress data model"""
    student_id: str
    total_assignments: int = 0
    submitted_assignments: int = 0
    total_grades: int = 0
    sum_grades: float = 0.0
    average_grade: float = 0.0
    level: str = LEVEL_BEGINNER
    
    def to_dict(self) -> Dict:
        return asdict(self)


class LMSDatabase:
    """
    Database interface for LMS.
    This class provides methods to interact with the Raft state machine
    and local storage for non-replicated data.
    """
    
    def __init__(self, node_id: str, data_dir: str, raft_node=None):
        self.node_id = node_id
        self.data_dir = data_dir
        self.raft_node = raft_node
        self.lock = threading.RLock()
        
        # Local storage paths
        self.users_file = os.path.join(data_dir, "users.json")
        self.tokens_file = os.path.join(data_dir, "tokens.json")
        
        # Create data directory
        os.makedirs(data_dir, exist_ok=True)
        
        # Load local data
        self._load_local_data()

    def create_token_pair(self, user_id: str, access_token: str, refresh_token: str, expires_at: float):
        """Create access and refresh token pair"""
        with self.lock:
            self.tokens[access_token] = {
                'user_id': user_id,
                'created_at': timestamp(),
                'expires_at': expires_at,
                'refresh_token': refresh_token
            }
            self._save_tokens()

    def get_user_id_by_refresh_token(self, refresh_token: str) -> Optional[str]:
        """Get user ID by refresh token"""
        with self.lock:
            for token_data in self.tokens.values():
                if token_data.get('refresh_token') == refresh_token:
                    return token_data['user_id']
            return None

    def get_access_token_by_refresh_token(self, refresh_token: str) -> Optional[str]:
        """Get access token by refresh token"""
        with self.lock:
            for access_token, token_data in self.tokens.items():
                if token_data.get('refresh_token') == refresh_token:
                    return access_token
            return None

    def is_refresh_token_expired(self, refresh_token: str, expiry_days: int) -> bool:
        """Check if refresh token is expired"""
        with self.lock:
            for token_data in self.tokens.values():
                if token_data.get('refresh_token') == refresh_token:
                    created_at = token_data['created_at']
                    return timestamp() > created_at + (expiry_days * 24 * 3600)
            return True

    def refresh_token_pair(self, user_id: str, old_refresh_token: str, 
                        new_access_token: str, new_refresh_token: str, expires_at: float):
        """Replace old token pair with new one"""
        with self.lock:
            # Remove old tokens
            tokens_to_remove = []
            for access_token, token_data in self.tokens.items():
                if token_data.get('refresh_token') == old_refresh_token:
                    tokens_to_remove.append(access_token)
            
            for token in tokens_to_remove:
                del self.tokens[token]
            
            # Add new token pair
            self.tokens[new_access_token] = {
                'user_id': user_id,
                'created_at': timestamp(),
                'expires_at': expires_at,
                'refresh_token': new_refresh_token
            }
            self._save_tokens()

    def delete_refresh_token(self, refresh_token: str):
        """Delete refresh token and associated access token"""
        with self.lock:
            tokens_to_remove = []
            for access_token, token_data in self.tokens.items():
                if token_data.get('refresh_token') == refresh_token:
                    tokens_to_remove.append(access_token)
            
            for token in tokens_to_remove:
                del self.tokens[token]
            self._save_tokens()
    
    def set_raft_node(self, raft_node):
        """Set the Raft node reference"""
        self.raft_node = raft_node
    
    def _load_local_data(self):
        """Load non-replicated data from local storage"""
        with self.lock:
            # Load users
            self.users = {}
            users_data = load_json_file(self.users_file)
            for user_id, user_dict in users_data.items():
                self.users[user_id] = User(**user_dict)
            
            # Load tokens
            self.tokens = load_json_file(self.tokens_file)
    
    def _save_users(self):
        """Save users to local storage"""
        users_data = {uid: user.to_dict() for uid, user in self.users.items()}
        save_json_file(users_data, self.users_file)
    
    def _save_tokens(self):
        """Save tokens to local storage"""
        save_json_file(self.tokens, self.tokens_file)
    
    # User management (local, not replicated)
    def create_user(self, username: str, password: str, user_type: str, name: str) -> User:
        """Create a new user"""
        with self.lock:
            # Check if username exists
            for user in self.users.values():
                if user.username == username:
                    raise ValueError("Username already exists")
            
            user = User(
                user_id=generate_id(),
                username=username,
                password_hash=hash_password(password),
                user_type=user_type,
                name=name,
                created_at=timestamp()
            )
            
            self.users[user.user_id] = user
            self._save_users()
            
            logger.info(f"Created user: {username} ({user_type})")
            return user
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username"""
        with self.lock:
            for user in self.users.values():
                if user.username == username:
                    return user
            return None
    
    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID"""
        with self.lock:
            return self.users.get(user_id)
    
    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate a user"""
        user = self.get_user_by_username(username)
        if user and verify_password(password, user.password_hash):
            return user
        return None
    
    # Token management (local, not replicated)
    def create_token(self, user_id: str, token: str):
        """Create a new token"""
        with self.lock:
            self.tokens[token] = {
                'user_id': user_id,
                'created_at': timestamp()
            }
            self._save_tokens()
    
    def get_user_by_token(self, token: str) -> Optional[User]:
        """Get user by token"""
        with self.lock:
            token_data = self.tokens.get(token)
            if token_data:
                return self.get_user_by_id(token_data['user_id'])
            return None
    
    def delete_token(self, token: str):
        """Delete a token"""
        with self.lock:
            if token in self.tokens:
                del self.tokens[token]
                self._save_tokens()
    
    def is_token_valid(self, token: str, expiry_hours: int = DEFAULT_TOKEN_EXPIRY_HOURS) -> bool:
        """Check if a token is valid"""
        with self.lock:
            token_data = self.tokens.get(token)
            if not token_data:
                return False
            
            # Check expiry
            created_at = token_data['created_at']
            if timestamp() > created_at + (expiry_hours * 3600):
                # Token expired, remove it
                del self.tokens[token]
                self._save_tokens()
                return False
            
            return True
    
    # Assignment operations (through Raft)
    def create_assignment(self, title: str, description: str, due_date: str, 
                         created_by: str, file_path: Optional[str] = None) -> str:
        """Create a new assignment"""
        assignment_id = generate_id()
        
        command = create_raft_command(RAFT_CMD_CREATE_ASSIGNMENT, {
            'assignment_id': assignment_id,
            'title': title,
            'description': description,
            'due_date': due_date,
            'created_by': created_by,
            'file_path': file_path
        })
        
        success, result, error = self._execute_raft_command(command)
        if success:
            return assignment_id
        else:
            raise Exception(error or "Failed to create assignment")
    
    def submit_assignment(self, assignment_id: str, student_id: str, 
                         content: str, file_path: Optional[str] = None) -> str:
        """Submit an assignment"""
        submission_id = generate_id()
        
        command = create_raft_command(RAFT_CMD_SUBMIT_ASSIGNMENT, {
            'submission_id': submission_id,
            'assignment_id': assignment_id,
            'student_id': student_id,
            'content': content,
            'file_path': file_path
        })
        
        success, result, error = self._execute_raft_command(command)
        if success:
            return submission_id
        else:
            raise Exception(error or "Failed to submit assignment")
    
    def grade_assignment(self, submission_id: str, grade: float, 
                        feedback: str, graded_by: str) -> bool:
        """Grade an assignment submission"""
        command = create_raft_command(RAFT_CMD_GRADE_ASSIGNMENT, {
            'submission_id': submission_id,
            'grade': grade,
            'feedback': feedback,
            'graded_by': graded_by
        })
        
        success, result, error = self._execute_raft_command(command)
        if not success:
            raise Exception(error or "Failed to grade assignment")
        return True
    
    # Query operations (through Raft)
    def post_query(self, student_id: str, query: str, use_llm: bool) -> str:
        """Post a student query"""
        query_id = generate_id()
        
        command = create_raft_command(RAFT_CMD_POST_QUERY, {
            'query_id': query_id,
            'student_id': student_id,
            'query': query,
            'use_llm': use_llm
        })
        
        success, result, error = self._execute_raft_command(command)
        if success:
            return query_id
        else:
            raise Exception(error or "Failed to post query")
    
    def answer_query(self, query_id: str, answer: str, answered_by: str) -> bool:
        """Answer a student query"""
        command = create_raft_command(RAFT_CMD_ANSWER_QUERY, {
            'query_id': query_id,
            'answer': answer,
            'answered_by': answered_by
        })
        
        success, result, error = self._execute_raft_command(command)
        if not success:
            raise Exception(error or "Failed to answer query")
        return True
    
    # Course material operations (through Raft)
    def upload_course_material(self, filename: str, file_type: str, 
                             topic: str, file_path: str, uploaded_by: str) -> str:
        """Upload course material"""
        material_id = generate_id()
        
        command = create_raft_command(RAFT_CMD_UPLOAD_MATERIAL, {
            'material_id': material_id,
            'filename': filename,
            'type': file_type,
            'topic': topic,
            'file_path': file_path,
            'uploaded_by': uploaded_by
        })
        
        success, result, error = self._execute_raft_command(command)
        if success:
            return material_id
        else:
            raise Exception(error or "Failed to upload course material")
    
    # Read operations (from state machine)
    def get_assignments(self) -> List[Assignment]:
        """Get all assignments"""
        state = self._get_state_machine_data()
        assignments = []
        
        for assignment_id, data in state.get('assignments', {}).items():
            assignments.append(Assignment(**data))
        
        return assignments
    
    def get_assignment(self, assignment_id: str) -> Optional[Assignment]:
        """Get assignment by ID"""
        state = self._get_state_machine_data()
        data = state.get('assignments', {}).get(assignment_id)
        
        if data:
            return Assignment(**data)
        return None
    
    def get_student_submissions(self, student_id: str) -> List[Submission]:
        """Get all submissions for a student"""
        state = self._get_state_machine_data()
        submissions = []
        
        for submission_id, data in state.get('submissions', {}).items():
            if data['student_id'] == student_id:
                submissions.append(Submission(**data))
        
        return submissions
    
    def get_assignment_submissions(self, assignment_id: str) -> List[Submission]:
        """Get all submissions for an assignment"""
        state = self._get_state_machine_data()
        submissions = []
        
        for submission_id, data in state.get('submissions', {}).items():
            if data['assignment_id'] == assignment_id:
                submissions.append(Submission(**data))
        
        return submissions
    
    def get_submission(self, submission_id: str) -> Optional[Submission]:
        """Get submission by ID"""
        state = self._get_state_machine_data()
        data = state.get('submissions', {}).get(submission_id)
        
        if data:
            return Submission(**data)
        return None
    
    def get_queries(self, student_id: Optional[str] = None) -> List[Query]:
        """Get queries, optionally filtered by student"""
        state = self._get_state_machine_data()
        queries = []
        
        for query_id, data in state.get('queries', {}).items():
            if student_id is None or data['student_id'] == student_id:
                queries.append(Query(**data))
        
        return queries
    
    def get_query(self, query_id: str) -> Optional[Query]:
        """Get query by ID"""
        state = self._get_state_machine_data()
        data = state.get('queries', {}).get(query_id)
        
        if data:
            return Query(**data)
        return None
    
    def get_course_materials(self) -> List[CourseMaterial]:
        """Get all course materials"""
        state = self._get_state_machine_data()
        materials = []
        
        for material_id, data in state.get('materials', {}).items():
            materials.append(CourseMaterial(**data))
        
        return materials
    
    def get_course_material(self, material_id: str) -> Optional[CourseMaterial]:
        """Get course material by ID"""
        state = self._get_state_machine_data()
        data = state.get('materials', {}).get(material_id)
        
        if data:
            return CourseMaterial(**data)
        return None
    
    def get_student_progress(self, student_id: str) -> StudentProgress:
        """Get student progress"""
        state = self._get_state_machine_data()
        data = state.get('student_progress', {}).get(student_id)
        
        if data:
            return StudentProgress(**data)
        else:
            return StudentProgress(student_id=student_id)
    
    def get_student_level(self, student_id: str) -> str:
        """Get student's current level"""
        progress = self.get_student_progress(student_id)
        return progress.level
    
    def get_all_students(self) -> List[User]:
        """Get all students"""
        with self.lock:
            return [user for user in self.users.values() 
                   if user.user_type == USER_TYPE_STUDENT]
    
    # Helper methods
    def _execute_raft_command(self, command: Dict) -> tuple:
        """Execute a command through Raft"""
        if not self.raft_node:
            return False, None, "Raft node not initialized"
        
        return self.raft_node.handle_client_request(
            command,
            client_id=self.node_id,
            request_id=generate_id()
        )
    
    def _get_state_machine_data(self) -> Dict:
        """Get current state machine data"""
        if not self.raft_node:
            return {}
        
        return self.raft_node.state_machine.state
    
    def initialize_default_users(self, default_users: List[Dict]):
        """Initialize default users from config"""
        for user_data in default_users:
            try:
                self.create_user(
                    username=user_data['username'],
                    password=user_data['password'],
                    user_type=user_data['type'],
                    name=user_data['name']
                )
            except ValueError:
                # User already exists
                pass


def create_raft_command(operation: str, resource_type: str, data: Dict, user_id: str) -> Dict:
    """Create a Raft command for consensus"""
    print(f"🔧 DEBUG: Creating Raft command - about to call time.time()")
    import time
    try:
        timestamp = time.time()
        print(f"✅ DEBUG: time.time() succeeded: {timestamp}")
    except NameError as e:
        print(f"❌ DEBUG: time.time() failed in create_raft_command: {e}")
        raise
    
    return {
        'operation': operation,
        'resource_type': resource_type,
        'data': data,
        'user_id': user_id,
        'timestamp': timestamp,
        'command_id': generate_id()
    }