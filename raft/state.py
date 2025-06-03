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
            operation = command.get('operation')
            resource_type = command.get('resource_type')
            data = command.get('data', {})
            user_id = command.get('user_id')
            
            logger.info(f"Applying command: {operation} {resource_type} by {user_id}")
            
            if operation == "POST":
                if resource_type == "assignment":
                    return self._create_assignment(command)
                elif resource_type == "submission":
                    return self._submit_assignment(command)
                elif resource_type == "query":
                    return self._post_query(command)  # ✅ Correct method call
                elif resource_type == "course_material":
                    return self._upload_material(command)
                elif resource_type == "grade":
                    return self._grade_assignment(command)
                else:
                    return {'success': False, 'error': f'Unknown resource type: {resource_type}'}
            elif operation == "PUT":
                if resource_type == "query_answer":
                    return self._answer_query(command)
                elif resource_type == "grade":
                    return self._grade_assignment(command)
                else:
                    return {'success': False, 'error': f'Unknown PUT resource type: {resource_type}'}
            else:
                return {'success': False, 'error': f'Unknown operation: {operation}'}
    
    def _create_assignment(self, command: Dict) -> Dict:
        """Create a new assignment"""
        data = command['data']
        assignment_id = data.get('assignment_id', generate_id())
        
        if 'assignments' not in self.state:
            self.state['assignments'] = {}
        
        assignment = {
            'assignment_id': assignment_id,
            'title': data['title'],
            'description': data['description'],
            'due_date': data['due_date'],
            'created_by': data['created_by'],
            'created_at': time.time(),
            'file_path': data.get('file_path')
        }
        
        self.state['assignments'][assignment_id] = assignment
        logger.info(f"Created assignment: {assignment_id}")
        
        return {
            'success': True,
            'id': assignment_id,
            'data': assignment
        }
    
    def _submit_assignment(self, command: Dict) -> Dict:
        """Submit an assignment"""
        data = command['data']
        submission_id = data.get('submission_id', generate_id())
        
        if 'submissions' not in self.state:
            self.state['submissions'] = {}
        
        submission = {
            'submission_id': submission_id,
            'assignment_id': data['assignment_id'],
            'student_id': data['student_id'],
            'content': data['content'],
            'file_path': data.get('file_path'),
            'submitted_at': time.time(),
            'grade': None,
            'feedback': None,
            'graded_by': None,
            'graded_at': None
        }
        
        self.state['submissions'][submission_id] = submission
        
        # Update student progress
        self._update_progress(data['student_id'])
        
        logger.info(f"Submitted assignment: {submission_id}")
        
        return {
            'success': True,
            'id': submission_id,
            'data': submission
        }
    
    def _grade_assignment(self, command: Dict) -> Dict:
        """Grade a submitted assignment"""
        data = command['data']
        submission_id = data['submission_id']
        
        if 'submissions' not in self.state or submission_id not in self.state['submissions']:
            return {'success': False, 'error': 'Submission not found'}
        
        submission = self.state['submissions'][submission_id]
        submission['grade'] = data['grade']
        submission['feedback'] = data['feedback']
        submission['graded_by'] = data['graded_by']
        submission['graded_at'] = time.time()
        
        # Update student progress with new grade
        self._update_student_stats(submission['student_id'], data['grade'])
        
        logger.info(f"Graded submission: {submission_id}")
        
        return {
            'success': True,
            'id': submission_id,
            'data': submission
        }
    
    def _post_query(self, command: Dict) -> Dict:
        """Post a student query - STATE MACHINE METHOD"""
        data = command['data']
        query_id = data.get('query_id', generate_id())
        
        if 'queries' not in self.state:
            self.state['queries'] = {}
        
        query = {
            'query_id': query_id,
            'student_id': data['student_id'],
            'query': data['query'],
            'use_llm': data['use_llm'],
            'posted_at': time.time(),
            'answer': None,
            'answered_by': None,
            'answered_at': None
        }
        
        self.state['queries'][query_id] = query
        logger.info(f"Posted query: {query_id}")
        
        return {
            'success': True,
            'id': query_id,
            'data': query
        }
    
    def _answer_query(self, command: Dict) -> Dict:
        """Answer a student query"""
        data = command['data']
        query_id = data['query_id']
        
        if 'queries' not in self.state or query_id not in self.state['queries']:
            return {'success': False, 'error': 'Query not found'}
        
        query = self.state['queries'][query_id]
        query['answer'] = data['answer']
        query['answered_by'] = data['answered_by']
        query['answered_at'] = time.time()
        
        logger.info(f"Answered query: {query_id}")
        
        return {
            'success': True,
            'id': query_id,
            'data': query
        }
    
    def _upload_material(self, command: Dict) -> Dict:
        """Upload course material"""
        data = command['data']
        material_id = data.get('material_id', generate_id())
        
        if 'materials' not in self.state:
            self.state['materials'] = {}
        
        material = {
            'material_id': material_id,
            'filename': data['filename'],
            'file_type': data['type'],
            'topic': data['topic'],
            'file_path': data['file_path'],
            'uploaded_by': data['uploaded_by'],
            'uploaded_at': time.time()
        }
        
        self.state['materials'][material_id] = material
        logger.info(f"Uploaded material: {material_id}")
        
        return {
            'success': True,
            'id': material_id,
            'data': material
        }
    
    def _update_progress(self, student_id: str):
        """Update student progress stats"""
        if 'student_progress' not in self.state:
            self.state['student_progress'] = {}
            
        if student_id not in self.state['student_progress']:
            self.state['student_progress'][student_id] = {
                'student_id': student_id,
                'total_assignments': 0,
                'submitted_assignments': 0,
                'total_grades': 0,
                'sum_grades': 0.0,
                'average_grade': 0.0,
                'level': 'BEGINNER'
            }
        
        progress = self.state['student_progress'][student_id]
        
        # Count total assignments
        progress['total_assignments'] = len(self.state.get('assignments', {}))
        
        # Count submitted assignments
        submitted = sum(1 for s in self.state.get('submissions', {}).values() 
                       if s['student_id'] == student_id)
        progress['submitted_assignments'] = submitted
        
        # Calculate average grade
        grades = [s['grade'] for s in self.state.get('submissions', {}).values() 
                 if s['student_id'] == student_id and s['grade'] is not None]
        
        if grades:
            progress['total_grades'] = len(grades)
            progress['sum_grades'] = sum(grades)
            progress['average_grade'] = sum(grades) / len(grades)
            
            # Update level based on average grade
            avg = progress['average_grade']
            if avg >= 90:
                progress['level'] = 'ADVANCED'
            elif avg >= 75:
                progress['level'] = 'INTERMEDIATE'
            else:
                progress['level'] = 'BEGINNER'
    
    def _update_student_stats(self, student_id: str, grade: float):
        """Update student stats when a new grade is added"""
        self._update_progress(student_id)
    
    def get_snapshot(self) -> Dict:
        """Get current state for snapshots"""
        with self.lock:
            return dict(self.state)
    
    def restore_from_snapshot(self, snapshot: Dict):
        """Restore state from snapshot"""
        with self.lock:
            self.state = dict(snapshot)
    
    def get_student_level(self, student_id: str) -> str:
        """Get student's current level"""
        if 'student_progress' not in self.state:
            return 'BEGINNER'
        
        progress = self.state['student_progress'].get(student_id)
        if not progress:
            return 'BEGINNER'
        
        return progress.get('level', 'BEGINNER')
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
    
    def post_query(self):
        """Post a query"""
        print("\n--- POST QUERY ---")
        
        query = input("\nEnter your question: ").strip()
        if not query:
            print("❌ Query cannot be empty")
            return
        
        # Ask if they want LLM or instructor response
        print("\nWho should answer your query?")
        print("1. AI Tutor (instant response)")
        print("2. Instructor (may take time)")
        
        choice = input("Select (1 or 2): ").strip()
        use_llm = (choice == "1")
        
        # Use the enhanced base client's post_data method with automatic retry
        from common.utils import generate_id  # Make sure this import exists
        
        query_data = {
            'query_id': generate_id(),  # ✅ Add this line
            'query': query,
            'use_llm': use_llm,
            'student_id': self.user_id if hasattr(self, 'user_id') else None
        }
        
        success, query_id = self.post_data("query", query_data)
        
        if success:
            print(f"✅ Query posted successfully! Query ID: {query_id}")
            if use_llm:
                print("🤖 AI Tutor will respond shortly...")
            else:
                print("👨‍🏫 Your instructor will respond when available.")
        else:
            print("❌ Failed to post query.")

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