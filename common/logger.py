"""
Logging configuration for the distributed LMS system.
Provides consistent logging across all components.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime


def setup_logging(config: dict = None):
    """
    Set up logging configuration for the entire application.
    
    Args:
        config: Configuration dictionary with logging settings
    """
    if config is None:
        config = {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file': 'logs/lms.log'
        }
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(config['file'])
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config['level'].upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(config['format'])
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        config['file'],
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Set specific log levels for noisy libraries
    logging.getLogger('grpc').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Name of the module (usually __name__)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class NodeLogger:
    """
    Specialized logger for Raft nodes that includes node ID in messages.
    """
    
    def __init__(self, node_id: str, name: str):
        self.node_id = node_id
        self.logger = logging.getLogger(f"{name}[{node_id}]")
    
    def debug(self, message: str, *args, **kwargs):
        self.logger.debug(f"[{self.node_id}] {message}", *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        self.logger.info(f"[{self.node_id}] {message}", *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        self.logger.warning(f"[{self.node_id}] {message}", *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        self.logger.error(f"[{self.node_id}] {message}", *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        self.logger.critical(f"[{self.node_id}] {message}", *args, **kwargs)