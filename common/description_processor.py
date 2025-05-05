# common/description_processor.py
import logging
import re
import json
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)

class DescriptionProcessor:
    """Centralized processor for coffee roaster descriptions"""
    
    # Pre-compiled regex patterns
    WHITESPACE_PATTERN = re.compile(r'\s+')
    URL_EMAIL_PATTERN = re.compile(r'https?://\S+|www\.\S+|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    SENTENCE_PATTERN = re.compile(r'(?<=[.!?])\s+')
    SCHEMA_PATTERN = re.compile(r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>')

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize description text"""
        if not text:
            return ""
            
        # Remove excessive whitespace
        cleaned = DescriptionProcessor.WHITESPACE_PATTERN.sub(' ', text).strip()
        # Filter out common boilerplate
        for phrase in ["cookie policy", "privacy policy", "subscribe", 
                       "add to cart", "free shipping", "login", "sign up"]:
            cleaned = re.sub(rf"(?i){phrase}.*?(\.|$)", "", cleaned)
        # Remove URLs and email addresses
        cleaned = DescriptionProcessor.URL_EMAIL_PATTERN.sub('', cleaned)
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
        schema_matches = DescriptionProcessor.SCHEMA_PATTERN.findall(html)
        
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
        Compile a multi-line description from multiple sources, aiming for 3–5 lines (sentences), each 50–100 words.
        """
        all_candidates = []
        # Add homepage description if available
        if sources.get('homepage'):
            all_candidates.append(sources['homepage'])
        # Add about page descriptions if available
        if sources.get('about_pages'):
            all_candidates.extend(sources['about_pages'])
        # Add markdown if available
        if sources.get('markdown'):
            all_candidates.append(sources['markdown'])
        # Extract sentences from all candidates
        all_text = "\n".join([c for c in all_candidates if c])
        # Split into sentences (simple split, can be improved)
        raw_sentences = DescriptionProcessor.SENTENCE_PATTERN.split(all_text)
        # Clean and filter
        cleaned_sentences = []
        for sent in raw_sentences:
            clean = cls.clean_text(sent)
            word_count = len(clean.split())
            if 5 <= word_count <= 100 and clean:
                cleaned_sentences.append(clean)
        # If too few, allow shorter/longer sentences
        if len(cleaned_sentences) < 3:
            for sent in raw_sentences:
                clean = cls.clean_text(sent)
                word_count = len(clean.split())
                if 2 <= word_count <= 120 and clean and clean not in cleaned_sentences:
                    cleaned_sentences.append(clean)
                if len(cleaned_sentences) >= 5:
                    break
        # Score and sort
        scored = [(s, cls.score_description(s)) for s in cleaned_sentences]
        scored.sort(key=lambda x: x[1], reverse=True)
        # Select top 3–5
        selected = [s for s, _ in scored[:5]]
        if len(selected) < 3 and deepseek_client and sources.get('markdown'):
            # Use LLM to enhance if available
            try:
                context = all_text[:5000]
                # Enhanced prompt that guides DeepSeek to focus on key aspects of coffee roasters
                # while maintaining appropriate tone and sentence structure
                prompt = f"""
                        You're creating a description for a coffee roaster website. Based on the available information, create 3-5 clear, informative sentences about this coffee roaster. Focus on:
                        - Their origin story and philosophy
                        - Types of coffee they offer
                        - What makes them unique
                        - Sustainable/ethical practices (if mentioned)

                        Write in an engaging, professional tone. Each sentence should be 10-20 words.

                        Coffee Roaster: {roaster_name}
                        Source Text: {context}
                        """
                response = deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": "You are a coffee domain expert."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7,
                    max_tokens=350,
                    stream=False
                )
                enhanced = response.choices[0].message.content.strip()
                # Split LLM output into lines
                enhanced_lines = [cls.clean_text(l) for l in enhanced.split('\n') if l.strip()]
                selected.extend([l for l in enhanced_lines if l and l not in selected])
            except Exception as e:
                logger.error(f"DeepSeek enhancement failed: {str(e)}")
                # Fall back to the original sentences we already extracted
        final_lines = [l for l in selected if 5 <= len(l.split()) <= 100][:5]
        if not final_lines:
            return f"{roaster_name} is a specialty coffee roaster focusing on quality beans and expert roasting techniques."
        return "\n".join(final_lines)