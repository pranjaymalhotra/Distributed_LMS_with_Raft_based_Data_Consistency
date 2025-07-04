"""
Ollama client for LLM integration.
Handles communication with the Ollama API for the deepseek-r1:1.5b model.
"""

import requests
import json
from typing import Optional, Dict, List
from common.logger import get_logger
import time

logger = get_logger(__name__)


class OllamaClient:
    """
    Client for interacting with Ollama API.
    """
    
    def __init__(self, model: str = "deepseek-r1:1.5b", 
                 base_url: str = "http://localhost:11434",
                 context_window: int = 2048):
        """
        Initialize Ollama client.
        
        Args:
            model: Model name to use
            base_url: Base URL for Ollama API
            context_window: Maximum context window size
        """
        self.model = model
        self.base_url = base_url
        self.context_window = context_window
        
        # Check if model is available
        self._check_model_availability()
    
    def _check_model_availability(self):
        """Check if the specified model is available"""
        try:
            response = requests.get(f"{self.base_url}/api/tags")
            if response.status_code == 200:
                models = response.json().get('models', [])
                model_names = [m['name'] for m in models]
                
                if self.model not in model_names:
                    logger.warning(f"Model {self.model} not found. Available models: {model_names}")
                    logger.info(f"Please run: ollama pull {self.model}")
                else:
                    logger.info(f"Model {self.model} is available")
            else:
                logger.warning("Could not check model availability")
                
        except Exception as e:
            logger.error(f"Error checking model availability: {e}")
    
    def generate(self, prompt: str, temperature: float = 0.7, 
                max_tokens: Optional[int] = None) -> str:
        """Generate response from LLM with detailed logging"""
        start_time = time.time()
        
        try:
            logger.info(f"🤖 Ollama: Starting generation...")
            logger.info(f"   🎯 Model: {self.model}")
            logger.info(f"   📏 Prompt length: {len(prompt)} chars")
            logger.info(f"   🌡️ Temperature: {temperature}")
            
            # Prepare request
            data = {
                "model": self.model,
                "prompt": prompt,
                "temperature": temperature,
                "stream": False
            }
            
            if max_tokens:
                data["options"] = {"num_predict": max_tokens}
            
            logger.info(f"📤 Ollama: Sending request to {self.base_url}")
            
            # Call Ollama API with extended timeout
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=data,
                timeout=300  # 5 minutes
            )
            
            elapsed = time.time() - start_time
            logger.info(f"⚡ Ollama: API call completed in {elapsed:.2f}s")
            logger.info(f"   📊 Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                answer = result.get('response', '')
                logger.info(f"✅ Ollama: Successfully generated response")
                logger.info(f"   📏 Response length: {len(answer)} chars")
                logger.info(f"   📝 Preview: {answer[:100]}...")
                return answer
            else:
                logger.error(f"❌ Ollama: API error {response.status_code}")
                logger.error(f"   📝 Error text: {response.text}")
                return "I apologize, but I'm having trouble generating a response right now."
                
        except requests.exceptions.Timeout:
            elapsed = time.time() - start_time
            logger.error(f"⏰ Ollama: Request timed out after {elapsed:.2f}s")
            return "The response is taking too long. Please try a simpler question."
            
        except requests.exceptions.ConnectionError:
            logger.error(f"🔌 Ollama: Cannot connect to {self.base_url}")
            return "Cannot connect to the AI service. Please check if Ollama is running."
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"💥 Ollama: Error after {elapsed:.2f}s: {e}")
            return "I encountered an error while processing your request."
    
    def generate_with_context(self, prompt: str, context: str, 
                            temperature: float = 0.7) -> str:
        """
        Generate response with additional context.
        
        Args:
            prompt: User's question
            context: Additional context to include
            temperature: Sampling temperature
            
        Returns:
            Generated response
        """
        # Combine context and prompt
        full_prompt = f"""Context:
{context}

Question: {prompt}

Please provide a helpful and educational response based on the context above."""
        
        # Truncate if needed to fit context window
        if len(full_prompt) > self.context_window * 3:  # Rough estimate
            # Truncate context, keep prompt
            max_context_len = (self.context_window * 3) - len(prompt) - 200
            context = context[:max_context_len] + "..."
            full_prompt = f"""Context:
{context}

Question: {prompt}

Please provide a helpful and educational response based on the context above."""
        
        return self.generate(full_prompt, temperature)
    
    def chat(self, messages: List[Dict[str, str]], temperature: float = 0.7) -> str:
        """
        Chat completion with conversation history.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            temperature: Sampling temperature
            
        Returns:
            Generated response
        """
        try:
            data = {
                "model": self.model,
                "messages": messages,
                "temperature": temperature,
                "stream": False
            }
            
            response = requests.post(
                f"{self.base_url}/api/chat",
                json=data,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get('message', {}).get('content', '')
            else:
                logger.error(f"Ollama chat error: {response.status_code}")
                return "I apologize, but I'm having trouble generating a response."
                
        except Exception as e:
            logger.error(f"Error in chat: {e}")
            return "I encountered an error while processing your request."
    
    def is_ready(self) -> bool:
        """Check if Ollama service is ready"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_available_models(self) -> list:
        """Get list of available models"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            if response.status_code == 200:
                models = response.json().get('models', [])
                return [m['name'] for m in models]
            return []
        except:
            return []
        
    def pull_model(self, model_name: Optional[str] = None) -> bool:
        """
        Pull a model from Ollama registry.
        
        Args:
            model_name: Model to pull (defaults to configured model)
            
        Returns:
            Success status
        """
        model = model_name or self.model
        
        try:
            logger.info(f"Pulling model {model}...")
            
            data = {"name": model}
            response = requests.post(
                f"{self.base_url}/api/pull",
                json=data,
                stream=True
            )
            
            if response.status_code == 200:
                # Stream progress
                for line in response.iter_lines():
                    if line:
                        progress = json.loads(line)
                        if 'status' in progress:
                            logger.info(f"Pull status: {progress['status']}")
                
                logger.info(f"Successfully pulled model {model}")
                return True
            else:
                logger.error(f"Failed to pull model: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error pulling model: {e}")
            return False