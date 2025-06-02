# tutoring/__init__.py
"""
Tutoring server with LLM integration.
"""

from .server import TutoringServer
from .llm_client import OllamaClient
from .context_builder import ContextBuilder
from .adaptive_tutor import AdaptiveTutor

__all__ = [
    'TutoringServer',
    'OllamaClient',
    'ContextBuilder',
    'AdaptiveTutor'
]