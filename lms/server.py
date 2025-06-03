"""
LMS server implementation.
Handles client requests and coordinates with Raft for consistency.
"""

import grpc
import json
import os
import threading
import time
from concurrent import futures
from typing import Dict, List, Optional

# import lms_pb2
# import lms_pb2_grpc
# import tutoring_pb2
# import tutoring_pb2_grpc
from lms import lms_pb2,lms_pb2_grpc
from tutoring import tutoring_pb2,tutoring_pb2_grpc
from lms.database import create_raft_command
from lms.database import LMSDatabase
from lms.auth import AuthManager, require_auth, require_instructor, require_student
from raft.state import NodeState
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

    def CheckLeadership(self, request, context):
        """Check if this node is the current leader"""
        try:
            is_leader = (
                self.raft_node and 
                self.raft_node.state == NodeState.LEADER
            )
            
            leader_id = ""
            cluster_status = "unknown"
            
            if self.raft_node:
                leader_id = self.raft_node.volatile_state.leader_id or ""
                cluster_status = self.raft_node.state.value
            
            return lms_pb2.LeadershipResponse(
                is_leader=is_leader,
                leader_id=leader_id,
                node_id=self.node_id,
                #cluster_status=cluster_status
            )
        except Exception as e:
            logger.error(f"CheckLeadership error: {e}")
            return lms_pb2.LeadershipResponse(
                is_leader=False,
                leader_id="",
                node_id=self.node_id,
                #cluster_status="error"
            )

    
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

    def RefreshToken(self, request, context):
        """Handle token refresh request"""
        try:
            success, new_access_token, new_refresh_token, error = self.auth_manager.refresh_token(
                request.refresh_token
            )
            
            if success:
                expires_in = self.auth_manager.token_expiry_hours * 3600
                return lms_pb2.RefreshTokenResponse(
                    success=True,
                    access_token=new_access_token,
                    refresh_token=new_refresh_token,
                    expires_in=expires_in,
                    error=""
                )
            else:
                return lms_pb2.RefreshTokenResponse(
                    success=False,
                    access_token="",
                    refresh_token="",
                    expires_in=0,
                    error=error or "Token refresh failed"
                )
                
        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            return lms_pb2.RefreshTokenResponse(
                success=False,
                access_token="",
                refresh_token="",
                expires_in=0,
                error=str(e)
            )

    
    # Authentication endpoints
    def Login(self, request, context):
        """Handle login request"""
        try:
            # ✅ Updated to handle 5 return values
            success, access_token, refresh_token, user_id, error = self.auth_manager.login(
                request.username,
                request.password,
                request.user_type
            )

            return lms_pb2.LoginResponse(
                success=success,
                token=access_token if success else "",  # Use access_token instead of token
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


    def _forward_to_leader(self, command: Dict, client_id: str, request_id: str) -> tuple:
        """Forward request to the leader if we're not the leader"""
        if not self.raft_node:
            return False, None, "Raft node not initialized"
        
        # If we're the leader, process directly
        if self.raft_node.state == NodeState.LEADER:
            return self.raft_node.handle_client_request(command, client_id, request_id)
        
        # We're not the leader, forward to leader
        leader_id = self.raft_node.volatile_state.leader_id
        if not leader_id:
            return False, None, "No leader known"
        
        # Find leader's node config
        leader_config = None
        for node_id, node_info in self.config['cluster']['nodes'].items():
            if node_id == leader_id:
                leader_config = node_info
                break
        
        if not leader_config:
            return False, None, f"Leader {leader_id} not in configuration"
        
        # Connect to leader's Raft service
        leader_address = f"{leader_config['host']}:{leader_config['raft_port']}"
        
        try:
            print(f"🔧 DEBUG: Forwarding request to leader at {leader_address}")
            channel = grpc.insecure_channel(leader_address)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            # Convert command to JSON
            command_json = json.dumps(command)
            
            # Create forward request
            forward_request = raft_pb2.ForwardRequestMessage(
                client_id=client_id,
                request_id=request_id,
                command=command_json
            )
            
            # Send to leader
            response = stub.ForwardRequest(forward_request, timeout=60)
            
            # Close channel
            channel.close()
            
            if response.success:
                return True, response.result, None
            else:
                return False, None, response.error
                
        except Exception as e:
            return False, None, f"Forward error: {str(e)}"
            
    # Data operations
    # @require_auth(auth_manager)
    @require_auth(lambda self: self.auth_manager)
    def Post(self, request, context):
        """Handle Post request"""
        try:
            user = context.user
            
            # Check if we're the leader
            if not self.raft_node or self.raft_node.state != NodeState.LEADER:
                leader_id = self.raft_node.volatile_state.leader_id if self.raft_node else ""
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"Not leader. Please retry. Current leader: {leader_id}"
                )
            
            # Parse data
            try:
                data = json.loads(request.data)
            except json.JSONDecodeError:
                return lms_pb2.PostResponse(
                    success=False,
                    error="Invalid JSON data",
                    id=""
                )
            
            # Create Raft command with correct number of arguments
            command = create_raft_command("POST", request.type, data, user.user_id)
            
            # Forward to Raft for consensus
            success, result, error = self.raft_node.handle_client_request(
                command, user.user_id, generate_id()
            )
            
            if success:
                # Extract ID from result
                result_data = json.loads(result) if result else {}
                resource_id = result_data.get('id', '')
                
                # Handle file data if present
                if request.file_data and request.filename:
                    if request.type == "assignment":
                        file_path = self._save_assignment_file(
                            request.filename, request.file_data, resource_id
                        )
                    elif request.type == "submission":
                        file_path = self._save_submission_file(
                            request.filename, request.file_data, user.user_id, 
                            data.get('assignment_id', '')
                        )
                    elif request.type == "course_material":
                        file_path = self._save_course_material(
                            request.filename, request.file_data
                        )
                
                return lms_pb2.PostResponse(
                    success=True,
                    error="",
                    id=resource_id
                )
            else:
                return lms_pb2.PostResponse(
                    success=False,
                    error=error or "Unknown error",
                    id=""
                )
                
        except Exception as e:
            logger.error(f"Post request error: {e}")
            return lms_pb2.PostResponse(
                success=False,
                error=str(e),
                id=""
            )
    
    # @require_auth(auth_manager)
    @require_auth(lambda self: self.auth_manager)
    def Get(self, request, context):
        """Handle Get request"""
        try:
            user = context.user
            
            # Check if we're the leader for read operations (optional, depends on consistency requirements)
            if not self.raft_node or self.raft_node.state != NodeState.LEADER:
                # For reads, you might want to allow followers, but for strong consistency, redirect to leader
                leader_id = self.raft_node.volatile_state.leader_id if self.raft_node else ""
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"Not leader. Please retry. Current leader: {leader_id}"
                )
            
            # Parse filter
            filter_data = {}
            if request.filter:
                try:
                    filter_data = json.loads(request.filter)
                except json.JSONDecodeError:
                    pass
            
            # Get data based on type and user permissions
            items = []
            
            if request.type == "assignments":
                assignments = self.database.get_assignments(user.user_id, user.user_type)
                for assignment in assignments:
                    items.append(lms_pb2.DataItem(
                        id=assignment.get('assignment_id', ''),
                        type="assignment",
                        data=json.dumps(assignment)
                    ))
                    
            elif request.type == "submissions":
                if user.user_type == USER_TYPE_STUDENT:
                    submissions = self.database.get_student_submissions(user.user_id)
                else:  # Instructor
                    assignment_id = filter_data.get('assignment_id')
                    submissions = self.database.get_submissions(assignment_id)
                
                for submission in submissions:
                    items.append(lms_pb2.DataItem(
                        id=submission.get('submission_id', ''),
                        type="submission",
                        data=json.dumps(submission)
                    ))
                    
            elif request.type == "queries":
                if user.user_type == USER_TYPE_STUDENT:
                    queries = self.database.get_student_queries(user.user_id)
                else:  # Instructor
                    queries = self.database.get_all_queries()
                
                for query in queries:
                    items.append(lms_pb2.DataItem(
                        id=query.get('query_id', ''),
                        type="query",
                        data=json.dumps(query)
                    ))
                    
            elif request.type == "course_materials":
                materials = self.database.get_course_materials()
                for material in materials:
                    items.append(lms_pb2.DataItem(
                        id=material.get('material_id', ''),
                        type="course_material",
                        data=json.dumps(material)
                    ))
                    
            elif request.type == "students" and user.user_type == USER_TYPE_INSTRUCTOR:
                students = self.database.get_all_students()
                for student in students:
                    items.append(lms_pb2.DataItem(
                        id=student.get('user_id', ''),
                        type="student",
                        data=json.dumps(student)
                    ))
            
            return lms_pb2.GetResponse(
                success=True,
                error="",
                items=items
            )
            
        except Exception as e:
            logger.error(f"Get request error: {e}")
            return lms_pb2.GetResponse(
                success=False,
                error=str(e),
                items=[]
            )
    
    # @require_instructor(auth_manager)
    @require_instructor(lambda self: self.auth_manager)
    def SubmitGrade(self, request, context):
        """Handle grade submission"""
        try:
            user = context.user
            
            # Create Raft command
            command = create_raft_command(RAFT_CMD_GRADE_ASSIGNMENT, {
                'submission_id': request.submission_id,
                'grade': request.grade,
                'feedback': request.feedback,
                'graded_by': user.user_id
            })
            
            # Submit through Raft with leader forwarding
            success, result, error = self._forward_to_leader(
                command, user.user_id, generate_id()
            )
            
            if success:
                return lms_pb2.GradeResponse(
                    success=True,
                    error=""
                )
            else:
                return lms_pb2.GradeResponse(
                    success=False,
                    error=error or "Failed to submit grade"
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
        """Handle PostQuery request"""
        try:
            user = context.user
            logger.info(f"PostQuery: Received from user {user.user_id}")
            
            # Only students can post queries
            if user.user_type != USER_TYPE_STUDENT:
                context.abort(grpc.StatusCode.PERMISSION_DENIED, "Only students can post queries")
            
            # Create query data
            query_data = {
                'query_id': generate_id(),
                'query': request.query,
                'use_llm': request.use_llm,
                'student_id': user.user_id
            }
            
            # Create Raft command
            command = create_raft_command("POST", "query", query_data, user.user_id)
            
            # Submit through Raft with leader forwarding
            success, result, error = self._forward_to_leader(command, user.user_id, generate_id())
            
            if success:
                result_data = json.loads(result) if result else {}
                query_id = result_data.get('id', '')
                
                # If using LLM, request answer asynchronously
                if request.use_llm:
                    threading.Thread(
                        target=self._request_llm_answer,
                        args=(query_id, request.query, user.user_id),
                        daemon=True
                    ).start()
                
                return lms_pb2.QueryResponse(
                    success=True,
                    error="",
                    query_id=query_id
                )
            else:
                return lms_pb2.QueryResponse(
                    success=False,
                    error=error or "Unknown error",
                    query_id=""
                )
                
        except Exception as e:
            logger.error(f"PostQuery error: {e}")
            return lms_pb2.QueryResponse(
                success=False,
                error=str(e),
                query_id=""
            )
    def _request_llm_answer(self, query_id: str, query: str, student_id: str):
        """Request answer from LLM tutoring server with detailed logging"""
        logger.info(f"🎯 LLM Request: Starting for query {query_id}")
        
        if not self.tutoring_client:
            logger.error("❌ LLM Request: Tutoring client not available")
            return
        
        try:
            # Get student context
            student_level = self._get_student_level(student_id)
            
            # Create LLM request
            context = tutoring_pb2.StudentContext(
                student_level=student_level,
                course_materials=[],
                average_grade=85.0
            )
            
            request = tutoring_pb2.LLMRequest(
                query_id=query_id,
                query=query,
                student_id=student_id,
                context=context
            )
            
            logger.info(f"📤 LLM Request: Sending to tutoring server...")
            start_time = time.time()
            
            # Send to Tutoring Server
            response = self.tutoring_client.GetLLMAnswer(request, timeout=300)
            
            llm_time = time.time() - start_time
            logger.info(f"⚡ LLM Request: Tutoring server responded in {llm_time:.2f}s")
            
            if response.success:
                logger.info(f"✅ LLM Request: Got answer for {query_id}")
                
                # Update query with LLM answer through Raft
                answer_command = create_raft_command(
                    "PUT", 
                    "query_answer",
                    {
                        'query_id': query_id,
                        'answer': response.answer,
                        'answered_by': 'AI_TUTOR',
                        'answered_at': time.time()
                    },
                    'system'
                )
                
                logger.info(f"🔗 LLM Request: Updating answer via Raft...")
                # ✅ Remove timeout parameter
                success, result, error = self.raft_node.handle_client_request(
                    answer_command, 'system', generate_id()
                )
                
                if success:
                    logger.info(f"✅ LLM Request: Answer successfully stored for {query_id}")
                else:
                    logger.error(f"❌ LLM Request: Failed to store answer: {error}")
            else:
                logger.error(f"❌ LLM Request: Tutoring server error: {response.error}")
            
        except Exception as e:
            logger.error(f"💥 LLM Request: Exception: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _get_student_level(self, student_id: str) -> str:
        """Get student level from state machine"""
        try:
            if self.raft_node and hasattr(self.raft_node, 'state_machine'):
                return self.raft_node.state_machine.get_student_level(student_id)
            return 'intermediate'  # Default
        except Exception as e:
            logger.error(f"Error getting student level: {e}")
            return 'intermediate'

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