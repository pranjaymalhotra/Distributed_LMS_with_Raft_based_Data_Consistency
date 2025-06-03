"""
Context builder for LLM tutoring.
Processes course materials and builds relevant context for queries.
"""

import re
from typing import Dict, List, Optional
from collections import defaultdict
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from common.logger import get_logger
import time

# Download required NLTK data
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except:
    pass

logger = get_logger(__name__)


class ContextBuilder:
    """
    Builds context for LLM queries from course materials.
    """
    
    def __init__(self):
        """Initialize context builder"""
        try:
            self.stop_words = set(stopwords.words('english'))
        except:
            # Fallback if NLTK data not available
            self.stop_words = set(['the', 'is', 'at', 'which', 'on', 'a', 'an'])
        
        # Keywords for different topics
        self.topic_keywords = {
            'algorithms': ['algorithm', 'complexity', 'sorting', 'search', 'graph', 'tree', 'dynamic'],
            'data_structures': ['array', 'list', 'stack', 'queue', 'heap', 'hash', 'tree', 'graph'],
            'operating_systems': ['process', 'thread', 'memory', 'scheduling', 'synchronization', 'deadlock'],
            'networking': ['tcp', 'ip', 'protocol', 'packet', 'routing', 'socket', 'http'],
            'databases': ['sql', 'query', 'table', 'index', 'transaction', 'normalization', 'join'],
            'programming': ['function', 'class', 'object', 'variable', 'loop', 'condition', 'method'],
            'distributed_systems': ['consensus', 'replication', 'partition', 'consistency', 'availability']
        }
    
    def process_material(self, material_id: str, filename: str, content: str) -> Dict:
        """
        Process course material and extract key information.
        
        Args:
            material_id: Unique ID for the material
            filename: Original filename
            content: Text content of the material
            
        Returns:
            Processed material dictionary
        """
        try:
            # Clean content
            content = self._clean_text(content)
            
            # Extract sections
            sections = self._extract_sections(content)
            
            # Extract key concepts
            concepts = self._extract_concepts(content)
            
            # Identify topics
            topics = self._identify_topics(content)
            
            # Generate summary
            summary = self._generate_summary(sections)
            
            processed = {
                'material_id': material_id,
                'filename': filename,
                'sections': sections,
                'concepts': concepts,
                'topics': topics,
                'summary': summary,
                'word_count': len(content.split())
            }
            
            logger.info(f"Processed material {filename}: {len(sections)} sections, "
                       f"{len(concepts)} concepts, topics: {topics}")
            
            return processed
            
        except Exception as e:
            logger.error(f"Error processing material: {e}")
            return {
                'material_id': material_id,
                'filename': filename,
                'sections': [{'title': 'Full Content', 'content': content[:1000]}],
                'concepts': [],
                'topics': [],
                'summary': content[:200] + '...',
                'word_count': len(content.split())
            }
    
    def find_relevant_sections(self, query: str, material: Dict, max_sections: int = 3) -> List[str]:
        """
        Find sections most relevant to a query.
        
        Args:
            query: User's question
            material: Processed material dictionary
            max_sections: Maximum number of sections to return
            
        Returns:
            List of relevant section contents
        """
        if not material.get('sections'):
            return []
        
        # Tokenize and clean query
        query_words = self._tokenize_text(query.lower())
        
        # Score each section
        section_scores = []
        for section in material['sections']:
            score = self._calculate_relevance_score(
                query_words,
                section.get('content', ''),
                section.get('title', '')
            )
            section_scores.append((score, section))
        
        # Sort by score and return top sections
        section_scores.sort(key=lambda x: x[0], reverse=True)
        
        relevant_sections = []
        for score, section in section_scores[:max_sections]:
            if score > 0:
                section_text = f"From {material['filename']} - {section.get('title', 'Section')}:\n"
                section_text += section['content'][:500]  # Limit length
                if len(section['content']) > 500:
                    section_text += '...'
                relevant_sections.append(section_text)
        
        return relevant_sections
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep sentence structure
        text = re.sub(r'[^\w\s\.\!\?\-\:\;]', ' ', text)
        
        return text.strip()
    
    def _extract_sections(self, content: str) -> List[Dict[str, str]]:
        """Extract sections from content"""
        sections = []
        
        # Try to find headers (lines that are likely titles)
        lines = content.split('\n')
        current_section = {'title': 'Introduction', 'content': ''}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if line is likely a header
            if (len(line) < 100 and 
                (line.isupper() or 
                 re.match(r'^\d+\.?\s+\w+', line) or  # Numbered sections
                 re.match(r'^Chapter|Section|Part', line, re.I))):
                
                # Save current section if it has content
                if current_section['content'].strip():
                    sections.append(current_section)
                
                # Start new section
                current_section = {'title': line, 'content': ''}
            else:
                current_section['content'] += line + ' '
        
        # Add last section
        if current_section['content'].strip():
            sections.append(current_section)
        
        # If no sections found, treat as single section
        if not sections:
            sections = [{'title': 'Full Content', 'content': content}]
        
        return sections
    
    def _extract_concepts(self, content: str) -> List[str]:
        """Extract key concepts from content"""
        concepts = []
        
        # Look for definitions and key terms
        definition_patterns = [
            r'(\w+)\s+is\s+defined\s+as',
            r'(\w+)\s+refers\s+to',
            r'The\s+(\w+)\s+is',
            r'A\s+(\w+)\s+is',
            r'(\w+):\s+[A-Z]'  # Term followed by colon and capital letter
        ]
        
        for pattern in definition_patterns:
            matches = re.findall(pattern, content, re.I)
            concepts.extend([m.lower() for m in matches if len(m) > 3])
        
        # Remove duplicates and common words
        concepts = list(set(concepts))
        concepts = [c for c in concepts if c not in self.stop_words]
        
        return concepts[:20]  # Limit to top 20 concepts
    
    def _identify_topics(self, content: str) -> List[str]:
        """Identify topics covered in the content"""
        content_lower = content.lower()
        identified_topics = []
        
        for topic, keywords in self.topic_keywords.items():
            # Count keyword occurrences
            keyword_count = sum(1 for keyword in keywords 
                              if keyword in content_lower)
            
            # If multiple keywords found, likely covers this topic
            if keyword_count >= 2:
                identified_topics.append(topic)
        
        return identified_topics
    
    def _generate_summary(self, sections: List[Dict[str, str]], max_sentences: int = 3) -> str:
        """Generate a summary from sections"""
        if not sections:
            return ""
        
        # Take first few sentences from first section
        first_content = sections[0].get('content', '')
        
        try:
            sentences = sent_tokenize(first_content)
            summary = ' '.join(sentences[:max_sentences])
        except:
            # Fallback if sentence tokenization fails
            words = first_content.split()
            summary = ' '.join(words[:50]) + '...'
        
        return summary
    
    def _tokenize_text(self, text: str) -> List[str]:
        """Tokenize text and remove stop words"""
        try:
            tokens = word_tokenize(text.lower())
        except:
            # Fallback tokenization
            tokens = text.lower().split()
        
        # Remove stop words and short tokens
        tokens = [t for t in tokens if t not in self.stop_words and len(t) > 2]
        
        return tokens
    
    def _calculate_relevance_score(self, query_words: List[str], 
                                 content: str, title: str) -> float:
        """Calculate relevance score between query and content"""
        content_lower = content.lower()
        title_lower = title.lower()
        
        score = 0.0
        
        # Check for exact phrase match
        query_phrase = ' '.join(query_words)
        if query_phrase in content_lower:
            score += 10.0
        
        # Score individual word matches
        for word in query_words:
            # Title matches are worth more
            if word in title_lower:
                score += 3.0
            
            # Count occurrences in content
            occurrences = content_lower.count(word)
            score += min(occurrences * 0.5, 5.0)  # Cap per-word score
        
        # Normalize by content length
        content_words = len(content.split())
        if content_words > 0:
            score = score / (content_words / 100)  # Normalize to per-100-words
        
        return score