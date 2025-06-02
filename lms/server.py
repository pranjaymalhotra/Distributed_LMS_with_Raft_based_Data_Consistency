"""
LMS server implementation.
Handles client requests and coordinates with Raft for consistency.
"""

import grpc
import json
import os
import threading
from concurrent import futures
from typing import Dict, List, Optional

# import lms_pb2
# import lms_pb2_grpc
# import tutoring_pb2
# import tutoring_pb2_grpc
from lms import lms_pb2,lms_pb2_grpc
from tutoring import tutoring_pb2,tutoring_pb2_grpc

from lms.database import LMSDatabase
from lms.auth import AuthManager, require_auth, require_instructor, require_student
from common.logger import get_logger
from common.constants import *
from common.utils import (
    save_file, read_pdf_content, read_text_file,
    chunk_file, is_pdf_file, is_text_file, generate_id
)

logger = get_logger(__name__)


class LMSServer(lms_pb2_grpc.LMSServiceServicer):
    """
    gRPC server for the LMS service.
    Handles all LMS operations and coordinates with Raft for consistency.
    """
    
    def __init__(self, node_id: str, config: Dict, raft_node=None):
        """
        Initialize LMS server.
        
        Args:
            node_id: Unique identifier for this node
            config: Configuration dictionary
            raft_node: Reference to the Raft node
        """
        self.node_id = node_id
        self.config = config
        self.raft_node = raft_node
        
        # Initialize database
        data_dir = os.path.join(config['storage']['database_dir'], node_id)
        self.database = LMSDatabase(node_id, data_dir, raft_node)
        
        # Initialize auth manager
        self.auth_manager = AuthManager(self.database, config)
        
        # Initialize default users
        self.database.initialize_default_users(config['auth']['default_users'])
        
        # Tutoring server client
        self.tutoring_client = None
        self._init_tutoring_client()
        
        # File storage paths
        self.materials_dir = config['storage']['course_materials_dir']
        self.assignments_dir = config['storage']['assignments_dir']
        
        # Create directories
        os.makedirs(self.materials_dir, exist_ok=True)
        os.makedirs(self.assignments_dir, exist_ok=True)
        
        # Server instance
        self.server = None
        
        logger.info(f"LMS server {node_id} initialized")
    
    def set_raft_node(self, raft_node):
        """Set the Raft node reference"""
        self.raft_node = raft_node
        self.database.set_raft_node(raft_node)
    
    def _init_tutoring_client(self):
        """Initialize connection to tutoring server"""
        try:
            tutoring_config = self.config['cluster']['tutoring_server']
            address = f"{tutoring_config['host']}:{tutoring_config['port']}"
            
            channel = grpc.insecure_channel(address)
            self.tutoring_client = tutoring_pb2_grpc.TutoringServiceStub(channel)
            
            logger.info(f"Connected to tutoring server at {address}")
        except Exception as e:
            logger.error(f"Failed to connect to tutoring server: {e}")
    
    # Authentication endpoints
    def Login(self, request, context):
        """Handle login request"""
        try:
            success, token, user_id, error = self.auth_manager.login(
                request.username,
                request.password,
                request.user_type
            )
            
            return lms_pb2.LoginResponse(
                success=success,
                token=token if success else "",
                error=error if error else "",
                user_id=user_id if user_id else ""
            )
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return lms_pb2.LoginResponse(
                success=False,
                token="",
                error=str(e),
                user_id=""
            )
    
    def Logout(self, request, context):
        """Handle logout request"""
        try:
            success = self.auth_manager.logout(request.token)
            
            return lms_pb2.LogoutResponse(
                success=success,
                error="" if success else "Failed to logout"
            )
            
        except Exception as e:
            logger.error(f"Logout error: {e}")
            return lms_pb2.LogoutResponse(
                success=False,
                error=str(e)
            )
    
    # Data operations
    # @require_auth(auth_manager)
    @require_auth(lambda self: self.auth_manager)
    def Post(self, request, context):
        """Handle post request for creating data"""
        try:
            user = context.user
            data = json.loads(request.data) if request.data else {}
            
            if request.type == DATA_TYPE_ASSIGNMENT:
                # Only instructors can create assignments
                if not self.auth_manager.can_create_assignment(user):
                    context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
                
                # Handle file if provided
                file_path = None
                if request.file_data:
                    file_path = self._save_assignment_file(
                        request.filename,
                        request.file_data,
                        data.get('assignment_id', generate_id())
                    )
                
                assignment_id = self.database.create_assignment(
                    title=data['title'],
                    description=data['description'],
                    due_date=data['due_date'],
                    created_by=user.user_id,
                    file_path=file_path
                )
                
                return lms_pb2.PostResponse(
                    success=True,
                    error="",
                    id=assignment_id
                )
            
            elif request.type == DATA_TYPE_SUBMISSION:
                # Only students can submit assignments
                if not self.auth_manager.can_submit_assignment(user):
                    context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
                
                # Handle file if provided
                file_path = None
                if request.file_data:
                    file_path = self._save_submission_file(
                        request.filename,
                        request.file_data,
                        user.user_id,
                        data['assignment_id']
                    )
                
                submission_id = self.database.submit_assignment(
                    assignment_id=data['assignment_id'],
                    student_id=user.user_id,
                    content=data.get('content', ''),
                    file_path=file_path
                )
                
                return lms_pb2.PostResponse(
                    success=True,
                    error="",
                    id=submission_id
                )
            
            elif request.type == DATA_TYPE_QUERY:
                # Only students can post queries
                if not self.auth_manager.can_post_query(user):
                    context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
                
                query_id = self.database.post_query(
                    student_id=user.user_id,
                    query=data['query'],
                    use_llm=data.get('use_llm', False)
                )
                
                # If LLM requested, send to tutoring server
                if data.get('use_llm', False):
                    self._request_llm_answer(query_id, data['query'], user.user_id)
                
                return lms_pb2.PostResponse(
                    success=True,
                    error="",
                    id=query_id
                )
            
            elif request.type == DATA_TYPE_COURSE_MATERIAL:
                # Only instructors can upload materials
                if not self.auth_manager.can_upload_material(user):
                    context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
                
                # Validate file type
                if not (is_pdf_file(request.filename) or is_text_file(request.filename)):
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, ERROR_INVALID_FILE_TYPE)
                
                # Save file
                file_path = self._save_course_material(
                    request.filename,
                    request.file_data
                )
                
                material_id = self.database.upload_course_material(
                    filename=request.filename,
                    file_type=FILE_TYPE_PDF if is_pdf_file(request.filename) else FILE_TYPE_TXT,
                    topic=data.get('topic', 'General'),
                    file_path=file_path,
                    uploaded_by=user.user_id
                )
                
                # Process material for LLM context
                self._process_course_material(material_id, request.filename, 
                                            request.file_data, file_path)
                
                return lms_pb2.PostResponse(
                    success=True,
                    error="",
                    id=material_id
                )
            
            else:
                return lms_pb2.PostResponse(
                    success=False,
                    error=f"Unknown data type: {request.type}",
                    id=""
                )
                
        except Exception as e:
            logger.error(f"Post error: {e}")
            return lms_pb2.PostResponse(
                success=False,
                error=str(e),
                id=""
            )
    
    # @require_auth(auth_manager)
    @require_auth(lambda self: self.auth_manager)
    def Get(self, request, context):
        """Handle get request for retrieving data"""
        try:
            user = context.user
            filter_data = json.loads(request.filter) if request.filter else {}
            items = []
            
            if request.type == DATA_TYPE_ASSIGNMENT:
                # All users can view assignments
                assignments = self.database.get_assignments()
                
                for assignment in assignments:
                    items.append(lms_pb2.DataItem(
                        id=assignment.assignment_id,
                        type=DATA_TYPE_ASSIGNMENT,
                        data=json.dumps(assignment.to_dict())
                    ))
            
            elif request.type == DATA_TYPE_SUBMISSION:
                # Get submissions based on user type and filter
                if self.auth_manager.can_view_all_submissions(user):
                    # Instructors can view all submissions
                    if 'assignment_id' in filter_data:
                        submissions = self.database.get_assignment_submissions(
                            filter_data['assignment_id']
                        )
                    elif 'student_id' in filter_data:
                        submissions = self.database.get_student_submissions(
                            filter_data['student_id']
                        )
                    else:
                        # Get all submissions (this could be large)
                        submissions = []
                        for assignment in self.database.get_assignments():
                            submissions.extend(
                                self.database.get_assignment_submissions(
                                    assignment.assignment_id
                                )
                            )
                else:
                    # Students can only view their own submissions
                    submissions = self.database.get_student_submissions(user.user_id)
                
                for submission in submissions:
                    items.append(lms_pb2.DataItem(
                        id=submission.submission_id,
                        type=DATA_TYPE_SUBMISSION,
                        data=json.dumps(submission.to_dict())
                    ))
            
            elif request.type == DATA_TYPE_QUERY:
                # Get queries
                if self.auth_manager.is_instructor(user):
                    # Instructors see all queries
                    queries = self.database.get_queries()
                else:
                    # Students see only their queries
                    queries = self.database.get_queries(student_id=user.user_id)
                
                for query in queries:
                    items.append(lms_pb2.DataItem(
                        id=query.query_id,
                        type=DATA_TYPE_QUERY,
                        data=json.dumps(query.to_dict())
                    ))
            
            elif request.type == DATA_TYPE_COURSE_MATERIAL:
                # All users can view course materials
                materials = self.database.get_course_materials()
                
                for material in materials:
                    items.append(lms_pb2.DataItem(
                        id=material.material_id,
                        type=DATA_TYPE_COURSE_MATERIAL,
                        data=json.dumps(material.to_dict())
                    ))
            
            elif request.type == DATA_TYPE_STUDENTS:
                # Only instructors can view all students
                if not self.auth_manager.can_view_all_students(user):
                    context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
                
                students = self.database.get_all_students()
                
                for student in students:
                    # Include progress information
                    progress = self.database.get_student_progress(student.user_id)
                    student_data = student.to_dict()
                    student_data['progress'] = progress.to_dict()
                    
                    items.append(lms_pb2.DataItem(
                        id=student.user_id,
                        type=DATA_TYPE_STUDENTS,
                        data=json.dumps(student_data)
                    ))
            
            else:
                return lms_pb2.GetResponse(
                    success=False,
                    error=f"Unknown data type: {request.type}",
                    items=[]
                )
            
            return lms_pb2.GetResponse(
                success=True,
                error="",
                items=items
            )
            
        except Exception as e:
            logger.error(f"Get error: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    # @require_instructor(auth_manager)
    @require_instructor(lambda self: self.auth_manager)
    def SubmitGrade(self, request, context):
        """Handle grade submission"""
        try:
            user = context.user
            
            # Grade the assignment
            self.database.grade_assignment(
                submission_id=request.submission_id,
                grade=request.grade,
                feedback=request.feedback,
                graded_by=user.user_id
            )
            
            return lms_pb2.GradeResponse(
                success=True,
                error=""
            )
            
        except Exception as e:
            logger.error(f"Grade submission error: {e}")
            return lms_pb2.GradeResponse(
                success=False,
                error=str(e)
            )
    
    # @require_auth(auth_manager)
    @require_auth(lambda self: self.auth_manager)
    def PostQuery(self, request, context):
        """Handle query posting"""
        try:
            user = context.user
            
            # Only students can post queries
            if not self.auth_manager.can_post_query(user):
                context.abort(grpc.StatusCode.PERMISSION_DENIED, ERROR_NOT_AUTHORIZED)
            
            query_id = self.database.post_query(
                student_id=user.user_id,
                query=request.query,
                use_llm=request.use_llm
            )
            
            # If LLM requested, send to tutoring server
            if request.use_llm:
                self._request_llm_answer(query_id, request.query, user.user_id)
            
            return lms_pb2.QueryResponse(
                success=True,
                error="",
                query_id=query_id
            )
            
        except Exception as e:
            logger.error(f"Query posting error: {e}")
            return lms_pb2.QueryResponse(
                success=False,
                error=str(e),
                query_id=""
            )
    
    def GetLLMAnswer(self, request, context):
        """Handle LLM answer response from tutoring server"""
        try:
            # This is called by the tutoring server to provide answers
            self.database.answer_query(
                query_id=request.query_id,
                answer=request.answer,
                answered_by=QUERY_SOURCE_LLM
            )
            
            return lms_pb2.LLMQueryResponse(
                success=True,
                error="",
                answer=request.answer
            )
            
        except Exception as e:
            logger.error(f"LLM answer error: {e}")
            return lms_pb2.LLMQueryResponse(
                success=False,
                error=str(e),
                answer=""
            )
    
    # @require_auth(auth_manager)
    @require_auth(lambda self: self.auth_manager)
    def UploadFile(self, request_iterator, context):
        """Handle file upload streaming"""
        try:
            user = context.user
            
            # Collect file chunks
            filename = None
            chunks = []
            
            for chunk in request_iterator:
                if not filename:
                    filename = chunk.filename
                chunks.append(chunk.content)
            
            if not filename:
                return lms_pb2.UploadResponse(
                    success=False,
                    error="No filename provided",
                    file_id=""
                )
            
            # Combine chunks
            file_data = b''.join(chunks)
            
            # Check file size
            if len(file_data) > MAX_FILE_SIZE:
                return lms_pb2.UploadResponse(
                    success=False,
                    error=ERROR_FILE_TOO_LARGE,
                    file_id=""
                )
            
            # Save file based on type (determined by context)
            file_id = generate_id()
            file_path = os.path.join(self.materials_dir, f"{file_id}_{filename}")
            save_file(file_data, file_path)
            
            return lms_pb2.UploadResponse(
                success=True,
                error="",
                file_id=file_id
            )
            
        except Exception as e:
            logger.error(f"File upload error: {e}")
            return lms_pb2.UploadResponse(
                success=False,
                error=str(e),
                file_id=""
            )
    
    # @require_auth(auth_manager)
    @require_auth(lambda self: self.auth_manager)
    def DownloadFile(self, request, context):
        """Handle file download streaming"""
        try:
            user = context.user
            file_path = None
            
            # Determine file path based on type
            if request.file_type == DATA_TYPE_COURSE_MATERIAL:
                material = self.database.get_course_material(request.filename)
                if material:
                    file_path = material.file_path
            
            elif request.file_type == DATA_TYPE_ASSIGNMENT:
                # Find assignment or submission file
                # This would need more logic to properly identify files
                pass
            
            if not file_path or not os.path.exists(file_path):
                context.abort(grpc.StatusCode.NOT_FOUND, "File not found")
            
            # Stream file chunks
            chunks = chunk_file(file_path)
            for i, chunk in enumerate(chunks):
                yield lms_pb2.FileChunk(
                    filename=os.path.basename(file_path),
                    content=chunk,
                    chunk_number=i,
                    is_last=(i == len(chunks) - 1)
                )
                
        except Exception as e:
            logger.error(f"File download error: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
    
    # Helper methods
    def _save_assignment_file(self, filename: str, data: bytes, assignment_id: str) -> str:
        """Save assignment file"""
        file_path = os.path.join(self.assignments_dir, f"assignment_{assignment_id}_{filename}")
        save_file(data, file_path)
        return file_path
    
    def _save_submission_file(self, filename: str, data: bytes, 
                             student_id: str, assignment_id: str) -> str:
        """Save submission file"""
        file_path = os.path.join(
            self.assignments_dir, 
            f"submission_{assignment_id}_{student_id}_{filename}"
        )
        save_file(data, file_path)
        return file_path
    
    def _save_course_material(self, filename: str, data: bytes) -> str:
        """Save course material file"""
        material_id = generate_id()
        file_path = os.path.join(self.materials_dir, f"{material_id}_{filename}")
        save_file(data, file_path)
        return file_path
    
    def _request_llm_answer(self, query_id: str, query: str, student_id: str):
        """Request answer from LLM tutoring server"""
        if not self.tutoring_client:
            logger.warning("Tutoring client not initialized")
            return
        
        try:
            # Prepare context
            student_level = self.database.get_student_level(student_id)
            progress = self.database.get_student_progress(student_id)
            
            # Get recent grades
            submissions = self.database.get_student_submissions(student_id)
            recent_grades = []
            for submission in submissions[-5:]:  # Last 5 submissions
                if submission.grade is not None:
                    recent_grades.append(tutoring_pb2.GradeInfo(
                        assignment_id=submission.assignment_id,
                        grade=submission.grade,
                        topic=""  # Would need to get from assignment
                    ))
            
            # Get relevant course materials
            materials = self.database.get_course_materials()
            material_ids = [m.material_id for m in materials[:5]]  # Top 5 materials
            
            # Create context
            context = tutoring_pb2.StudentContext(
                student_level=student_level,
                relevant_materials=material_ids,
                recent_grades=recent_grades,
                course_context=json.dumps({
                    'average_grade': progress.average_grade,
                    'total_assignments': progress.total_assignments
                })
            )
            
            # Send request
            request = tutoring_pb2.LLMRequest(
                query_id=query_id,
                query=query,
                student_id=student_id,
                context=context
            )
            
            # Make async call
            threading.Thread(
                target=self._call_tutoring_server,
                args=(request,),
                daemon=True
            ).start()
            
        except Exception as e:
            logger.error(f"Failed to request LLM answer: {e}")
    
    def _call_tutoring_server(self, request):
        """Make async call to tutoring server"""
        try:
            response = self.tutoring_client.GetLLMAnswer(request, timeout=30)
            
            if response.success:
                # Update the query with the answer
                self.database.answer_query(
                    query_id=response.query_id,
                    answer=response.answer,
                    answered_by=QUERY_SOURCE_LLM
                )
                logger.info(f"Received LLM answer for query {response.query_id}")
            else:
                logger.error(f"LLM answer failed: {response.error}")
                
        except Exception as e:
            logger.error(f"Error calling tutoring server: {e}")
    
    def _process_course_material(self, material_id: str, filename: str, 
                                data: bytes, file_path: str):
        """Process course material for LLM context"""
        if not self.tutoring_client:
            return
        
        try:
            # Determine content type
            content_type = FILE_TYPE_PDF if is_pdf_file(filename) else FILE_TYPE_TXT
            
            # Send to tutoring server for processing
            request = tutoring_pb2.CourseMaterialRequest(
                material_id=material_id,
                filename=filename,
                content=data,
                content_type=content_type
            )
            
            threading.Thread(
                target=lambda: self.tutoring_client.ProcessCourseMaterial(request),
                daemon=True
            ).start()
            
        except Exception as e:
            logger.error(f"Failed to process course material: {e}")
    
    def start(self, port: int):
        """Start the gRPC server"""
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[
                ('grpc.max_send_message_length', 50 * 1024 * 1024),
                ('grpc.max_receive_message_length', 50 * 1024 * 1024),
            ]
        )
        
        lms_pb2_grpc.add_LMSServiceServicer_to_server(self, self.server)
        
        self.server.add_insecure_port(f'[::]:{port}')
        self.server.start()
        
        logger.info(f"LMS server started on port {port}")
    
    def stop(self):
        """Stop the gRPC server"""
        if self.server:
            self.server.stop(grace=5)
            logger.info("LMS server stopped")