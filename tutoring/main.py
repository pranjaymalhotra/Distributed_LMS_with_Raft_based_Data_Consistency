"""
Main entry point for running the tutoring server with Ollama integration.
"""

import sys
import argparse
import json
import signal
import time

from tutoring.server import TutoringServer
from common.logger import setup_logging, get_logger


class TutoringNode:
    """Tutoring server node with Ollama integration"""
    
    def __init__(self, config_path: str):
        """
        Initialize tutoring node.
        
        Args:
            config_path: Path to configuration file
        """
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Setup logging
        setup_logging(self.config['logging'])
        self.logger = get_logger(__name__)
        
        self.tutoring_config = self.config['cluster']['tutoring_server']
        
        # Components
        self.tutoring_server = None
        
        # Signal handling
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def start(self):
        """Start the tutoring server"""
        self.logger.info("Starting Tutoring Server")
        
        try:
            # Check Ollama availability
            self._check_ollama()
            
            # Create and start tutoring server
            self.tutoring_server = TutoringServer(self.config)
            self.tutoring_server.start(self.tutoring_config['port'])
            
            # Print status
            self._print_status()
            
            # Keep running
            self.logger.info("Tutoring server is running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Error starting tutoring server: {e}")
            raise
        finally:
            self.stop()
    
    def stop(self):
        """Stop the tutoring server"""
        self.logger.info("Stopping Tutoring Server")
        
        if self.tutoring_server:
            try:
                self.tutoring_server.stop()
                self.logger.info("Tutoring server stopped")
            except Exception as e:
                self.logger.error(f"Error stopping tutoring server: {e}")
        
        self.logger.info("Tutoring server stopped successfully")
    
    def _check_ollama(self):
        """Check if Ollama is available and model is downloaded"""
        import requests
        
        try:
            # Check if Ollama is running
            response = requests.get("http://localhost:11434/api/tags", timeout=5)
            if response.status_code != 200:
                raise Exception("Ollama is not running")
            
            # Check if model is available
            models = response.json().get('models', [])
            model_names = [m['name'] for m in models]
            
            if self.tutoring_config['model'] not in model_names:
                self.logger.warning(f"Model {self.tutoring_config['model']} not found!")
                self.logger.warning(f"Available models: {model_names}")
                self.logger.warning(f"Please run: ollama pull {self.tutoring_config['model']}")
                
                print("\n" + "="*60)
                print("WARNING: Required model not found!")
                print(f"Please run: ollama pull {self.tutoring_config['model']}")
                print("="*60 + "\n")
                
                response = input("Continue anyway? (y/n): ")
                if response.lower() != 'y':
                    sys.exit(1)
            else:
                self.logger.info(f"Model {self.tutoring_config['model']} is available")
                
        except requests.exceptions.RequestException:
            self.logger.error("Cannot connect to Ollama!")
            print("\n" + "="*60)
            print("ERROR: Cannot connect to Ollama!")
            print("Please ensure Ollama is running:")
            print("  ollama serve")
            print("="*60 + "\n")
            sys.exit(1)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}")
        sys.exit(0)
    
    def _print_status(self):
        """Print server status"""
        print("\n" + "="*60)
        print("TUTORING SERVER STATUS")
        print("="*60)
        print(f"Host: {self.tutoring_config['host']}")
        print(f"Port: {self.tutoring_config['port']}")
        print(f"Model: {self.tutoring_config['model']}")
        print(f"Context Window: {self.tutoring_config['context_window']}")
        print("="*60)
        print("Services:")
        print(f"  ✓ Tutoring Server: Running on port {self.tutoring_config['port']}")
        print(f"  ✓ Ollama Integration: Connected")
        print("="*60)
        print("Features:")
        print("  - Adaptive tutoring based on student level")
        print("  - Context-aware responses using course materials")
        print("  - PDF and TXT file processing")
        print("  - Real-time LLM responses")
        print("="*60)
        print("Connected LMS Nodes:")
        for node_id in self.config['cluster']['nodes'].keys():
            print(f"  - {node_id}")
        print("="*60 + "\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Run the LMS tutoring server with Ollama LLM integration'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    
    parser.add_argument(
        '--model',
        help='Override the Ollama model to use'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        help='Override the server port'
    )
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    # Apply overrides
    if args.model:
        config['cluster']['tutoring_server']['model'] = args.model
    
    if args.port:
        config['cluster']['tutoring_server']['port'] = args.port
    
    # Save updated config back
    with open(args.config, 'w') as f:
        json.dump(config, f, indent=2)
    
    # Create and start server
    try:
        server = TutoringNode(args.config)
        server.start()
    except Exception as e:
        print(f"Failed to start tutoring server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()