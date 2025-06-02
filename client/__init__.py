# client/__init__.py
"""
Client applications for students and instructors.
"""

from .base_client import BaseLMSClient
from .student_client import StudentClient
from .instructor_client import InstructorClient

__all__ = [
    'BaseLMSClient',
    'StudentClient',
    'InstructorClient'
]
