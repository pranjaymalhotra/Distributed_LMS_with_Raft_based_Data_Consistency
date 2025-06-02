"""
Tutoring server implementation with Ollama integration.
Provides adaptive LLM-based tutoring for students.
"""

import grpc
import json
import os
import threading
from concurrent import futures
from typing import Dict, List, Optional

# import tutoring_pb2
# import tutoring_pb2_grpc
# import lms_pb2
# import lms_pb2_grpc

from lms import lms_pb2,lms_pb2_grpc
# from lms import lms_pb2_grpc
from tutoring import tutoring_pb2,tutoring_pb2_grpc
#from tutoring import tutoring_pb2_grpc

from tutoring.llm_client import OllamaClient
from tutoring.context_builder import ContextBuilder
from tutoring.adaptive_tutor import AdaptiveTutor
from common.logger import get_logger
from common.constants import *
from common.utils import read_pdf_content, read_text_file

logger = get_logger(__name__)


class TutoringServer(tutoring_pb2_grpc.TutoringServiceServicer):
    """
    gRPC server for the tutoring service.
    Handles LLM-based tutoring with adaptive difficulty.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize tutoring server.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.tutoring_config = config['cluster']['tutoring_server']
        
        # Initialize Ollama client
        self.llm_client = OllamaClient(
            model=self.tutoring_config['model'],
            context_window=self.tutoring_config.get('context_window', 2048)
        )
        
        # Initialize context builder and adaptive tutor
        self.context_builder = ContextBuilder()
        self.adaptive_tutor = AdaptiveTutor()
        
        # Storage for processed course materials
        self.processed_materials = {}
        self.materials_lock = threading.RLock()
        
        # LMS clients for callback
        self.lms_clients = {}
        self._init_lms_clients()
        
        # Server instance
        self.server = None
        
        logger.info("Tutoring server initialized")
    
    def _init_lms_clients(self):
        """Initialize connections to LMS servers"""
        try:
            for node_id, node_config in self.config['cluster']['nodes'].items():
                if node_config['role'] == 'LMS_WITH_RAFT':
                    address = f"{node_config['host']}:{node_config['lms_port']}"
                    channel = grpc.insecure_channel(address)
                    stub = lms_pb2_grpc.LMSServiceStub(channel)
                    self.lms_clients[node_id] = stub
                    logger.info(f"Connected to LMS server {node_id} at {address}")
        except Exception as e:
            logger.error(f"Failed to connect to LMS servers: {e}")
    
    def GetLLMAnswer(self, request, context):
        """Generate answer using LLM"""
        try:
            logger.info(f"Received LLM request for query {request.query_id}")
            
            # Build context for the query
            full_context = self._build_full_context(
                request.query,
                request.student_id,
                request.context
            )
            
            # Generate prompt based on student level
            prompt = self.adaptive_tutor.generate_prompt(
                query=request.query,
                student_level=request.context.student_level,
                context=full_context
            )
            
            # Get response from LLM
            llm_response = self.llm_client.generate(prompt)
            
            # Adapt response based on student level
            adapted_response = self.adaptive_tutor.adapt_response(
                response=llm_response,
                student_level=request.context.student_level
            )
            
            # Extract references from response
            references = self._extract_references(adapted_response)
            
            # Send answer back to LMS
            self._send_answer_to_lms(
                request.query_id,
                adapted_response,
                request.student_id
            )
            
            return tutoring_pb2.LLMResponse(
                query_id=request.query_id,
                answer=adapted_response,
                difficulty_level=request.context.student_level,
                references=references,
                success=True,
                error=""
            )
            
        except Exception as e:
            logger.error(f"Error generating LLM answer: {e}")
            return tutoring_pb2.LLMResponse(
                query_id=request.query_id,
                answer="",
                difficulty_level="",
                references=[],
                success=False,
                error=str(e)
            )
    
    def ProcessCourseMaterial(self, request, context):
        """Process course material for context building"""
        try:
            logger.info(f"Processing course material: {request.filename}")
            
            # Extract text content based on type
            if request.content_type == FILE_TYPE_PDF:
                # Save temporarily and extract
                temp_path = f"/tmp/{request.material_id}.pdf"
                with open(temp_path, 'wb') as f:
                    f.write(request.content)
                content = read_pdf_content(temp_path)
                os.remove(temp_path)
            else:
                # Text file
                content = request.content.decode('utf-8')
            
            # Process content
            processed = self.context_builder.process_material(
                material_id=request.material_id,
                filename=request.filename,
                content=content
            )
            
            # Store processed material
            with self.materials_lock:
                self.processed_materials[request.material_id] = processed
            
            return tutoring_pb2.ProcessResponse(
                success=True,
                error="",
                summary=processed.get('summary', ''),
                topics=processed.get('topics', [])
            )
            
        except Exception as e:
            logger.error(f"Error processing course material: {e}")
            return tutoring_pb2.ProcessResponse(
                success=False,
                error=str(e),
                summary="",
                topics=[]
            )
    
    def HealthCheck(self, request, context):
        """Check health of tutoring server"""
        try:
            # Check if LLM is responsive
            llm_ready = self.llm_client.is_ready()
            
            return tutoring_pb2.HealthResponse(
                healthy=True,
                llm_ready=llm_ready,
                model_name=self.tutoring_config['model']
            )
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return tutoring_pb2.HealthResponse(
                healthy=False,
                llm_ready=False,
                model_name=""
            )
    
    def _build_full_context(self, query: str, student_id: str, 
                           student_context: tutoring_pb2.StudentContext) -> str:
        """Build complete context for LLM"""
        try:
            context_parts = []
            
            # Add student information
            context_parts.append(f"Student Level: {student_context.student_level}")
            
            # Parse course context
            if student_context.course_context:
                course_info = json.loads(student_context.course_context)
                context_parts.append(
                    f"Student Performance: Average Grade {course_info.get('average_grade', 0):.1f}%, "
                    f"Completed {course_info.get('total_assignments', 0)} assignments"
                )
            
            # Add recent grades context
            if student_context.recent_grades:
                grades_text = "Recent Grades: "
                grades = [f"{g.grade:.1f}%" for g in student_context.recent_grades]
                grades_text += ", ".join(grades)
                context_parts.append(grades_text)
            
            # Add relevant course materials
            relevant_content = []
            with self.materials_lock:
                for material_id in student_context.relevant_materials:
                    if material_id in self.processed_materials:
                        material = self.processed_materials[material_id]
                        # Find relevant sections based on query
                        relevant_sections = self.context_builder.find_relevant_sections(
                            query, material
                        )
                        relevant_content.extend(relevant_sections)
            
            if relevant_content:
                context_parts.append("Relevant Course Content:")
                context_parts.extend(relevant_content[:3])  # Limit to top 3 sections
            
            return "\n\n".join(context_parts)
            
        except Exception as e:
            logger.error(f"Error building context: {e}")
            return ""
    
    def _extract_references(self, response: str) -> List[str]:
        """Extract material references from response"""
        references = []
        
        # Simple extraction - look for material IDs mentioned
        with self.materials_lock:
            for material_id, material in self.processed_materials.items():
                if material.get('filename', '') in response:
                    references.append(material_id)
        
        return references
    
    def _send_answer_to_lms(self, query_id: str, answer: str, student_id: str):
        """Send answer back to LMS server"""
        try:
            # Try each LMS client until one succeeds
            for node_id, stub in self.lms_clients.items():
                try:
                    request = lms_pb2.LLMQueryRequest(
                        query_id=query_id,
                        query="",  # Not needed for response
                        context=json.dumps({
                            'answer': answer,
                            'answered_by': QUERY_SOURCE_LLM
                        })
                    )
                    
                    response = stub.GetLLMAnswer(request, timeout=5)
                    
                    if response.success:
                        logger.info(f"Successfully sent answer to LMS node {node_id}")
                        return
                        
                except Exception as e:
                    logger.debug(f"Failed to send to LMS node {node_id}: {e}")
                    continue
            
            logger.error("Failed to send answer to any LMS node")
            
        except Exception as e:
            logger.error(f"Error sending answer to LMS: {e}")
    
    def start(self, port: int):
        """Start the gRPC server"""
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[
                ('grpc.max_send_message_length', 50 * 1024 * 1024),
                ('grpc.max_receive_message_length', 50 * 1024 * 1024),
            ]
        )
        
        tutoring_pb2_grpc.add_TutoringServiceServicer_to_server(self, self.server)
        
        self.server.add_insecure_port(f'[::]:{port}')
        self.server.start()
        
        logger.info(f"Tutoring server started on port {port}")
    
    def stop(self):
        """Stop the gRPC server"""
        if self.server:
            self.server.stop(grace=5)
            logger.info("Tutoring server stopped")