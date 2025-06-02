"""
Common utilities for the distributed LMS system.
"""

import hashlib
import secrets
import time
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import PyPDF2
import uuid


def generate_token() -> str:
    """Generate a secure random token"""
    return secrets.token_urlsafe(32)


def generate_id() -> str:
    """Generate a unique ID"""
    return str(uuid.uuid4())


def hash_password(password: str) -> str:
    """Hash a password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against its hash"""
    return hash_password(password) == hashed


def timestamp() -> float:
    """Get current timestamp"""
    return time.time()


def datetime_now() -> datetime:
    """Get current datetime"""
    return datetime.now()


def format_datetime(dt: datetime) -> str:
    """Format datetime for display"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime from string"""
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def is_token_expired(token_created: float, expiry_hours: int) -> bool:
    """Check if a token has expired"""
    expiry_time = token_created + (expiry_hours * 3600)
    return time.time() > expiry_time


def read_pdf_content(file_path: str) -> str:
    """Extract text content from a PDF file"""
    try:
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            content = []
            
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                content.append(page.extract_text())
            
            return '\n'.join(content)
    except Exception as e:
        raise Exception(f"Failed to read PDF: {e}")


def read_text_file(file_path: str) -> str:
    """Read content from a text file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        raise Exception(f"Failed to read text file: {e}")


def save_file(content: bytes, file_path: str):
    """Save content to a file"""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'wb') as file:
            file.write(content)
    except Exception as e:
        raise Exception(f"Failed to save file: {e}")


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON data from a file"""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                return json.load(file)
        return {}
    except Exception as e:
        raise Exception(f"Failed to load JSON file: {e}")


def save_json_file(data: Dict[str, Any], file_path: str):
    """Save data to a JSON file"""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=2)
    except Exception as e:
        raise Exception(f"Failed to save JSON file: {e}")


def calculate_grade_level(average_grade: float) -> str:
    """Calculate student level based on average grade"""
    if average_grade >= 85:
        return "ADVANCED"
    elif average_grade >= 70:
        return "INTERMEDIATE"
    else:
        return "BEGINNER"


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def chunk_file(file_path: str, chunk_size: int = 1024 * 1024) -> list:
    """Split a file into chunks for streaming"""
    chunks = []
    with open(file_path, 'rb') as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
    return chunks


def merge_chunks(chunks: list, output_path: str):
    """Merge file chunks back into a single file"""
    with open(output_path, 'wb') as file:
        for chunk in chunks:
            file.write(chunk)


def validate_username(username: str) -> bool:
    """Validate username format"""
    return len(username) >= 3 and username.isalnum()


def validate_assignment_submission(submission: Dict[str, Any]) -> bool:
    """Validate assignment submission data"""
    required_fields = ['assignment_id', 'content']
    return all(field in submission for field in required_fields)


def create_raft_command(command_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a Raft command for state machine"""
    return {
        'type': command_type,
        'timestamp': timestamp(),
        **data
    }


def parse_grpc_address(address: str) -> tuple:
    """Parse gRPC address into host and port"""
    parts = address.split(':')
    if len(parts) != 2:
        raise ValueError(f"Invalid address format: {address}")
    return parts[0], int(parts[1])


def get_file_extension(filename: str) -> str:
    """Get file extension from filename"""
    return os.path.splitext(filename)[1].lower()


def is_pdf_file(filename: str) -> bool:
    """Check if file is a PDF"""
    return get_file_extension(filename) == '.pdf'


def is_text_file(filename: str) -> bool:
    """Check if file is a text file"""
    return get_file_extension(filename) in ['.txt', '.text']