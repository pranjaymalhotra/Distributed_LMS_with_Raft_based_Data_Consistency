"""
Raft node states and state machine implementation.
Handles state transitions and ensures consistency.
"""

from enum import Enum
from typing import Optional, Dict, List, Set
import time
import threading
from dataclasses import dataclass, field
from common.logger import get_logger

logger = get_logger(__name__)


class NodeState(Enum):
    """Raft node states"""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class PersistentState:
    """Persistent state on all servers (updated on stable storage before responding to RPCs)"""
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PersistentState':
        """Create from dictionary"""
        return cls(
            current_term=data.get('current_term', 0),
            voted_for=data.get('voted_for'),
            log=data.get('log', [])
        )


@dataclass
class VolatileState:
    """Volatile state on all servers"""
    commit_index: int = 0
    last_applied: int = 0
    
    # Leader-specific volatile state
    next_index: Dict[str, int] = field(default_factory=dict)  # For each server, index of next log entry to send
    match_index: Dict[str, int] = field(default_factory=dict)  # For each server, index of highest log entry known to be replicated
    
    # Additional state for optimization
    leader_id: Optional[str] = None
    last_heartbeat: float = 0
    election_timeout: float = 0
    
    # Membership
    current_configuration: Set[str] = field(default_factory=set)
    new_configuration: Optional[Set[str]] = None  # For joint consensus during membership changes


class StateMachine:
    """
    Application state machine that applies committed commands.
    This is where the actual LMS logic will be applied.
    """
    
    def __init__(self):
        self.state = {}
        self.lock = threading.RLock()
        
    def apply_command(self, command: Dict) -> Dict:
        """
        Apply a command to the state machine.
        Returns the result of applying the command.
        """
        with self.lock:
            cmd_type = command.get('type')
            
            if cmd_type == 'CREATE_ASSIGNMENT':
                return self._create_assignment(command)
            elif cmd_type == 'SUBMIT_ASSIGNMENT':
                return self._submit_assignment(command)
            elif cmd_type == 'GRADE_ASSIGNMENT':
                return self._grade_assignment(command)
            elif cmd_type == 'POST_QUERY':
                return self._post_query(command)
            elif cmd_type == 'ANSWER_QUERY':
                return self._answer_query(command)
            elif cmd_type == 'UPLOAD_MATERIAL':
                return self._upload_material(command)
            elif cmd_type == 'UPDATE_STUDENT_PROGRESS':
                return self._update_progress(command)
            else:
                return {'success': False, 'error': f'Unknown command type: {cmd_type}'}
    
    def _create_assignment(self, command: Dict) -> Dict:
        """Create a new assignment"""
        assignment_id = command.get('assignment_id')
        if 'assignments' not in self.state:
            self.state['assignments'] = {}
        
        self.state['assignments'][assignment_id] = {
            'id': assignment_id,
            'title': command.get('title'),
            'description': command.get('description'),
            'due_date': command.get('due_date'),
            'created_by': command.get('created_by'),
            'created_at': time.time()
        }
        
        return {'success': True, 'assignment_id': assignment_id}
    
    def _submit_assignment(self, command: Dict) -> Dict:
        """Submit an assignment"""
        submission_id = command.get('submission_id')
        if 'submissions' not in self.state:
            self.state['submissions'] = {}
        
        self.state['submissions'][submission_id] = {
            'id': submission_id,
            'assignment_id': command.get('assignment_id'),
            'student_id': command.get('student_id'),
            'content': command.get('content'),
            'submitted_at': time.time(),
            'grade': None,
            'feedback': None
        }
        
        return {'success': True, 'submission_id': submission_id}
    
    def _grade_assignment(self, command: Dict) -> Dict:
        """Grade a submitted assignment"""
        submission_id = command.get('submission_id')
        if 'submissions' not in self.state or submission_id not in self.state['submissions']:
            return {'success': False, 'error': 'Submission not found'}
        
        self.state['submissions'][submission_id].update({
            'grade': command.get('grade'),
            'feedback': command.get('feedback'),
            'graded_by': command.get('graded_by'),
            'graded_at': time.time()
        })
        
        # Update student progress
        student_id = self.state['submissions'][submission_id]['student_id']
        self._update_student_stats(student_id, command.get('grade'))
        
        return {'success': True}
    
    def _post_query(self, command: Dict) -> Dict:
        """Post a student query"""
        query_id = command.get('query_id')
        if 'queries' not in self.state:
            self.state['queries'] = {}
        
        self.state['queries'][query_id] = {
            'id': query_id,
            'student_id': command.get('student_id'),
            'query': command.get('query'),
            'use_llm': command.get('use_llm', False),
            'posted_at': time.time(),
            'answer': None,
            'answered_by': None
        }
        
        return {'success': True, 'query_id': query_id}
    
    def _answer_query(self, command: Dict) -> Dict:
        """Answer a student query"""
        query_id = command.get('query_id')
        if 'queries' not in self.state or query_id not in self.state['queries']:
            return {'success': False, 'error': 'Query not found'}
        
        self.state['queries'][query_id].update({
            'answer': command.get('answer'),
            'answered_by': command.get('answered_by'),
            'answered_at': time.time()
        })
        
        return {'success': True}
    
    def _upload_material(self, command: Dict) -> Dict:
        """Upload course material"""
        material_id = command.get('material_id')
        if 'materials' not in self.state:
            self.state['materials'] = {}
        
        self.state['materials'][material_id] = {
            'id': material_id,
            'filename': command.get('filename'),
            'type': command.get('type'),
            'topic': command.get('topic'),
            'uploaded_by': command.get('uploaded_by'),
            'uploaded_at': time.time()
        }
        
        return {'success': True, 'material_id': material_id}
    
    def _update_progress(self, command: Dict) -> Dict:
        """Update student progress"""
        student_id = command.get('student_id')
        if 'student_progress' not in self.state:
            self.state['student_progress'] = {}
        
        if student_id not in self.state['student_progress']:
            self.state['student_progress'][student_id] = {
                'total_assignments': 0,
                'submitted_assignments': 0,
                'average_grade': 0.0,
                'level': 'BEGINNER'
            }
        
        progress = command.get('progress', {})
        self.state['student_progress'][student_id].update(progress)
        
        return {'success': True}
    
    def _update_student_stats(self, student_id: str, grade: float):
        """Update student statistics after grading"""
        if 'student_progress' not in self.state:
            self.state['student_progress'] = {}
        
        if student_id not in self.state['student_progress']:
            self.state['student_progress'][student_id] = {
                'total_grades': 0,
                'sum_grades': 0.0,
                'average_grade': 0.0,
                'level': 'BEGINNER'
            }
        
        progress = self.state['student_progress'][student_id]
        progress['total_grades'] += 1
        progress['sum_grades'] += grade
        progress['average_grade'] = progress['sum_grades'] / progress['total_grades']
        
        # Update student level based on average grade
        avg = progress['average_grade']
        if avg >= 85:
            progress['level'] = 'ADVANCED'
        elif avg >= 70:
            progress['level'] = 'INTERMEDIATE'
        else:
            progress['level'] = 'BEGINNER'
    
    def get_snapshot(self) -> Dict:
        """Get a snapshot of the current state"""
        with self.lock:
            return self.state.copy()
    
    def restore_from_snapshot(self, snapshot: Dict):
        """Restore state from a snapshot"""
        with self.lock:
            self.state = snapshot.copy()
    
    def get_student_level(self, student_id: str) -> str:
        """Get student's current level for adaptive tutoring"""
        with self.lock:
            if 'student_progress' in self.state and student_id in self.state['student_progress']:
                return self.state['student_progress'][student_id].get('level', 'BEGINNER')
            return 'BEGINNER'