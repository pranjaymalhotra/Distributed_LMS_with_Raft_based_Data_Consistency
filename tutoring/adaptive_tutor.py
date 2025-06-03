"""
Adaptive tutor that adjusts responses based on student level.
Provides personalized tutoring experiences.
"""

from typing import Dict, List, Optional
from common.constants import *
from common.logger import get_logger
import time


logger = get_logger(__name__)


class AdaptiveTutor:
    """
    Provides adaptive tutoring by adjusting complexity and style
    based on student performance level.
    """
    
    def __init__(self):
        """Initialize adaptive tutor"""
        # Response templates for different levels
        self.level_templates = {
            LEVEL_BEGINNER: {
                'intro': "Let me explain this in simple terms:\n\n",
                'style': 'simple',
                'examples': 'basic',
                'depth': 'shallow',
                'encouragement': [
                    "Great question! Let's break this down step by step.",
                    "Don't worry, this concept can be tricky at first.",
                    "You're doing great by asking questions!",
                    "Let's start with the basics and build from there."
                ]
            },
            LEVEL_INTERMEDIATE: {
                'intro': "Here's a detailed explanation:\n\n",
                'style': 'moderate',
                'examples': 'practical',
                'depth': 'medium',
                'encouragement': [
                    "Good question! Let's explore this further.",
                    "You're making good progress. Here's the next level:",
                    "Building on what you know, let's dive deeper.",
                    "This is a great opportunity to expand your understanding."
                ]
            },
            LEVEL_ADVANCED: {
                'intro': "Let's examine this concept in depth:\n\n",
                'style': 'technical',
                'examples': 'complex',
                'depth': 'deep',
                'encouragement': [
                    "Excellent question! This touches on advanced concepts.",
                    "Let's explore the nuances of this topic.",
                    "This is a sophisticated question that requires careful analysis.",
                    "You're ready for the advanced aspects of this topic."
                ]
            }
        }
        
        # Complexity adjustments
        self.complexity_rules = {
            LEVEL_BEGINNER: {
                'max_sentence_length': 20,
                'technical_terms': 'explain',
                'analogies': 'everyday',
                'math_level': 'basic'
            },
            LEVEL_INTERMEDIATE: {
                'max_sentence_length': 30,
                'technical_terms': 'use_with_brief_explanation',
                'analogies': 'technical',
                'math_level': 'intermediate'
            },
            LEVEL_ADVANCED: {
                'max_sentence_length': 50,
                'technical_terms': 'use_freely',
                'analogies': 'abstract',
                'math_level': 'advanced'
            }
        }
    
    def generate_prompt(self, query: str, student_level: str, context: str) -> str:
        """
        Generate an appropriate prompt for the LLM based on student level.
        
        Args:
            query: Student's question
            student_level: Student's current level
            context: Additional context
            
        Returns:
            Formatted prompt for LLM
        """
        template = self.level_templates.get(student_level, self.level_templates[LEVEL_INTERMEDIATE])
        rules = self.complexity_rules.get(student_level, self.complexity_rules[LEVEL_INTERMEDIATE])
        
        prompt = f"""You are an adaptive tutor helping a {student_level} level student.

Student's Question: {query}

Context Information:
{context}

Instructions for your response:
1. Complexity Level: {template['style']}
2. Use {template['examples']} examples
3. Provide {template['depth']} explanations
4. Technical Terms: {rules['technical_terms']}
5. Keep sentences under {rules['max_sentence_length']} words when possible
6. Use {rules['analogies']} analogies if helpful

Remember to:
- Be encouraging and supportive
- Check understanding with follow-up questions
- Provide step-by-step explanations for complex topics
- Adapt your language to the student's level

Please provide a helpful, educational response:"""
        
        return prompt
    
    def adapt_response(self, response: str, student_level: str) -> str:
        """
        Adapt LLM response to better match student level.
        
        Args:
            response: Original LLM response
            student_level: Student's level
            
        Returns:
            Adapted response
        """
        template = self.level_templates.get(student_level, self.level_templates[LEVEL_INTERMEDIATE])
        
        # Add appropriate introduction
        adapted = template['intro']
        
        # Add encouragement for beginners
        if student_level == LEVEL_BEGINNER:
            import random
            encouragement = random.choice(template['encouragement'])
            adapted = f"{encouragement}\n\n{adapted}"
        
        # Process the response
        adapted += self._process_response(response, student_level)
        
        # Add appropriate conclusion
        adapted += self._add_conclusion(student_level)
        
        return adapted
    
    def _process_response(self, response: str, student_level: str) -> str:
        """Process response based on student level"""
        if student_level == LEVEL_BEGINNER:
            # Simplify complex sentences
            response = self._simplify_language(response)
            
            # Add more breaks between concepts
            response = response.replace('. ', '.\n\n')
            
            # Highlight key points
            response = self._highlight_key_points(response)
            
        elif student_level == LEVEL_ADVANCED:
            # Add technical depth indicators
            response = self._add_depth_indicators(response)
        
        return response
    
    def _simplify_language(self, text: str) -> str:
        """Simplify complex language for beginners"""
        # Replace complex terms with simpler ones
        replacements = {
            'utilize': 'use',
            'implement': 'create',
            'demonstrate': 'show',
            'fundamental': 'basic',
            'consequently': 'so',
            'furthermore': 'also',
            'nevertheless': 'but',
            'comprehend': 'understand'
        }
        
        for complex_term, simple_term in replacements.items():
            text = text.replace(complex_term, simple_term)
            text = text.replace(complex_term.capitalize(), simple_term.capitalize())
        
        return text
    
    def _highlight_key_points(self, text: str) -> str:
        """Highlight key points for better understanding"""
        # Add emphasis to important concepts
        key_phrases = ['important:', 'remember:', 'key point:', 'note:']
        
        for phrase in key_phrases:
            text = text.replace(phrase, f"**{phrase.upper()}**")
            text = text.replace(phrase.capitalize(), f"**{phrase.upper()}**")
        
        return text
    
    def _add_depth_indicators(self, text: str) -> str:
        """Add depth indicators for advanced students"""
        # Add section markers for complex topics
        if "however" in text.lower():
            text = text.replace("However", "\n**Alternative Perspective:**\nHowever")
        
        if "for example" in text.lower():
            text = text.replace("For example", "\n**Example:**\nFor example")
        
        return text
    
    def _add_conclusion(self, student_level: str) -> str:
        """Add appropriate conclusion based on level"""
        conclusions = {
            LEVEL_BEGINNER: "\n\n**Next Steps:**\n- Try working through a simple example\n- "
                           "Ask if anything is unclear\n- Practice with basic problems first",
            
            LEVEL_INTERMEDIATE: "\n\n**To deepen your understanding:**\n- "
                               "Try implementing this concept\n- "
                               "Explore related topics\n- "
                               "Challenge yourself with harder problems",
            
            LEVEL_ADVANCED: "\n\n**For further exploration:**\n- "
                           "Consider edge cases and optimizations\n- "
                           "Research recent developments in this area\n- "
                           "Try to apply this to real-world scenarios"
        }
        
        return conclusions.get(student_level, conclusions[LEVEL_INTERMEDIATE])
    
    def assess_query_complexity(self, query: str) -> str:
        """
        Assess the complexity of a student's query.
        
        Args:
            query: Student's question
            
        Returns:
            Complexity level (BEGINNER, INTERMEDIATE, ADVANCED)
        """
        # Keywords indicating different complexity levels
        beginner_keywords = [
            'what is', 'how do i', 'can you explain', 'basics of',
            'simple', 'start', 'beginning', 'introduction'
        ]
        
        advanced_keywords = [
            'optimize', 'complexity', 'algorithm', 'implementation',
            'advanced', 'efficient', 'performance', 'architecture',
            'design pattern', 'best practice', 'trade-off'
        ]
        
        query_lower = query.lower()
        
        # Check for complexity indicators
        beginner_score = sum(1 for keyword in beginner_keywords if keyword in query_lower)
        advanced_score = sum(1 for keyword in advanced_keywords if keyword in query_lower)
        
        # Determine complexity
        if beginner_score > advanced_score:
            return LEVEL_BEGINNER
        elif advanced_score > beginner_score:
            return LEVEL_ADVANCED
        else:
            return LEVEL_INTERMEDIATE
    
    def generate_follow_up_questions(self, topic: str, student_level: str) -> List[str]:
        """
        Generate appropriate follow-up questions based on student level.
        
        Args:
            topic: Current topic
            student_level: Student's level
            
        Returns:
            List of follow-up questions
        """
        questions = {
            LEVEL_BEGINNER: [
                f"Does this explanation of {topic} make sense to you?",
                f"Would you like to see a simple example of {topic}?",
                f"What part of {topic} would you like me to explain more?",
                "Should we go through this step-by-step?"
            ],
            LEVEL_INTERMEDIATE: [
                f"How would you apply {topic} in a real project?",
                f"Can you think of other ways to use {topic}?",
                f"What challenges might you face when implementing {topic}?",
                f"Would you like to explore the performance aspects of {topic}?"
            ],
            LEVEL_ADVANCED: [
                f"What are the trade-offs when using {topic} at scale?",
                f"How would you optimize {topic} for your specific use case?",
                f"Can you identify potential edge cases in {topic}?",
                f"What alternative approaches to {topic} have you considered?"
            ]
        }
        
        return questions.get(student_level, questions[LEVEL_INTERMEDIATE])