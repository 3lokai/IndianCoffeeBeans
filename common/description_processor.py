# common/description_processor.py
import logging
import re
import json
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)

class DescriptionProcessor:
    """Centralized processor for coffee roaster descriptions"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize description text"""
        if not text:
            return ""
            
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', text).strip()
        # Filter out common boilerplate
        for phrase in ["cookie policy", "privacy policy", "subscribe", 
                       "add to cart", "free shipping", "login", "sign up"]:
            cleaned = re.sub(rf"(?i){phrase}.*?(\.|$)", "", cleaned)
        # Remove URLs and email addresses
        cleaned = re.sub(r'https?://\S+|www\.\S+|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', cleaned)
        return cleaned
    
    @staticmethod
    def truncate(text: str, max_length: int = 500) -> str:
        """Truncate text to max length with ellipsis"""
        if not text:
            return ""
        return text[:max_length-3] + "..." if len(text) > max_length else text
    
    @staticmethod
    def score_description(text: str) -> int:
        """Score description quality based on content and keywords"""
        if not text:
            return 0
            
        # Base score on length (but penalize if too long)
        length_score = min(len(text) / 20, 25)  # Max 25 points for length
        
        # Score coffee-related terms
        coffee_terms = ['coffee', 'roast', 'bean', 'brew', 'espresso', 'cafe', 
                      'arabica', 'robusta', 'origin', 'flavor', 'notes', 'profile',
                      'ethical', 'sustainable', 'farm', 'direct trade']
                      
        term_score = sum(3 for term in coffee_terms if term in text.lower())
        
        # Penalize generic content
        generic_phrases = ['welcome to', 'we are a', 'click here', 'check out']
        generic_penalty = sum(5 for phrase in generic_phrases if phrase in text.lower())
        
        return int(length_score + term_score - generic_penalty)
    
    @classmethod
    def get_best_description(cls, candidates: List[str], min_score: int = 10) -> Optional[str]:
        """Select best description from candidates based on scoring"""
        if not candidates:
            return None
            
        # Clean and score all candidates
        processed = []
        for text in candidates:
            if not text:
                continue
                
            cleaned = cls.clean_text(text)
            if len(cleaned) < 50:  # Skip very short descriptions
                continue
                
            score = cls.score_description(cleaned)
            processed.append((cleaned, score))
        
        # Sort by score (highest first)
        processed.sort(key=lambda x: x[1], reverse=True)
        
        # Log scores for debugging
        for i, (text, score) in enumerate(processed[:3]):
            preview = text[:50] + "..." if len(text) > 50 else text
            logger.debug(f"Description candidate {i}: Score={score}, Text={preview}")
        
        # Return highest scoring if it meets minimum threshold
        if processed and processed[0][1] >= min_score:
            return cls.truncate(processed[0][0])
            
        # If we have candidates but none meet threshold, return best one anyway
        if processed:
            return cls.truncate(processed[0][0])
            
        return None
    
    @staticmethod
    def extract_from_schema(html: str) -> Optional[str]:
        """Extract description from schema.org JSON-LD if available"""
        schema_pattern = r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        schema_matches = re.findall(schema_pattern, html, re.DOTALL)
        
        for match in schema_matches:
            try:
                data = json.loads(match)
                # Handle different schema.org formats
                if isinstance(data, dict):
                    if data.get('description'):
                        return data['description']
                    elif data.get('@graph'):
                        for item in data['@graph']:
                            if item.get('description'):
                                return item['description']
            except:
                continue
                
        return None
        
    @classmethod
    async def compile_description(cls, 
                                 sources: Dict[str, Any], 
                                 roaster_name: str,
                                 deepseek_client=None) -> str:
        """
        Compile the best description from multiple sources
        
        Args:
            sources: Dict with 'homepage', 'about_pages', etc.
            roaster_name: Name of the coffee roaster
            deepseek_client: Optional client for LLM enhancement
            
        Returns:
            str: Best available description
        """
        all_candidates = []
        
        # Add homepage description if available
        if sources.get('homepage'):
            all_candidates.append(sources['homepage'])
            
        # Add about page descriptions if available
        if sources.get('about_pages'):
            all_candidates.extend(sources['about_pages'])
            
        # Get best non-AI description first
        best_description = cls.get_best_description(all_candidates)
        
        # If we have a good description, return it
        if best_description and len(best_description) >= 100:
            return best_description
            
        # If description is missing or too short and we have DeepSeek available, use it
        if deepseek_client and sources.get('markdown'):
            try:
                # Use all available text as context
                context = "\n\n".join([c for c in all_candidates if c])
                context += "\n\n" + sources.get('markdown', '')
                
                prompt = f"""
                Summarize this coffee roaster's story in 2–3 sentences.
                Focus on their origin, coffee types, values, and uniqueness.

                Roaster Name: {roaster_name}
                Source Text: {context[:4000]}
                """

                response = deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": "You are a coffee domain expert."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7,
                    max_tokens=150,
                    stream=False
                )

                enhanced = response.choices[0].message.content.strip()
                
                # If we already had a description, combine them
                if best_description:
                    return best_description + " " + enhanced
                    
                return enhanced
                
            except Exception as e:
                logger.error(f"DeepSeek enhancement failed: {str(e)}")
                # Fall back to best available description
                return best_description if best_description else "A specialty coffee roaster."
                
        # Return what we have, or a generic fallback
        return best_description if best_description else "A specialty coffee roaster."