import google.generativeai as genai
from typing import Dict, Any, List, Optional
import json
import time
import logging
from config.settings import settings

class GeminiService:
    """Enhanced service with flexible JSON generation for user-specified data types"""
    
    def __init__(self):
        if not settings.GEMINI_API_KEYS:
            raise ValueError("No GEMINI_API_KEYS found in environment variables. Please provide at least one.")
        
        self.api_keys = settings.GEMINI_API_KEYS
        self.current_key_index = 0
        self._configure_gemini_with_current_key()

        self.model = genai.GenerativeModel('gemini-2.5-flash')
        self.logger = logging.getLogger(__name__)
        
        self.generation_config = genai.types.GenerationConfig(
            temperature=settings.GEMINI_TEMPERATURE,
            max_output_tokens=settings.GEMINI_MAX_TOKENS,
        )

    def _get_current_api_key(self) -> str:
        return self.api_keys[self.current_key_index]

    def _rotate_api_key(self):
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        self._configure_gemini_with_current_key()
        self.logger.warning(f"Rotated to API key index: {self.current_key_index}")

    def _configure_gemini_with_current_key(self):
        genai.configure(api_key=self._get_current_api_key())

    def generate_text(self, prompt: str, 
                     system_instruction: Optional[str] = None,
                     max_retries: int = 3) -> str:
        """Generate text using Gemini with retry logic and API key rotation"""
        
        full_prompt = prompt
        if system_instruction:
            full_prompt = f"System: {system_instruction}\n\nUser: {prompt}"
        
        for attempt in range(max_retries * len(self.api_keys)):
            try:
                response = self.model.generate_content(
                    full_prompt,
                    generation_config=self.generation_config
                )
                
                return response.text
                
            except Exception as e:
                self.logger.error(f"Attempt {attempt+1}: Gemini API call failed with error: {e}")
                
                if "quota exceeded" in str(e).lower() or "rate limit" in str(e).lower():
                    self.logger.warning("Rate limit hit or quota exceeded. Rotating API key...")
                    self._rotate_api_key()
                
                if attempt == (max_retries * len(self.api_keys)) - 1:
                    raise Exception(f"Failed to generate text after {max_retries * len(self.api_keys)} attempts across all API keys: {e}")
                
                # Exponential backoff (after key rotation or other errors)
                time.sleep(2 ** (attempt // len(self.api_keys)))
        
        return ""
    
    def parse_query(self, user_query: str) -> Dict[str, Any]:
        """Parse user query where user must specify language, domain, data type, and sample count"""
        
        system_instruction = """
        You are a query parser. The user will provide a request that should contain:
        1. domain_type: The field/domain they want (e.g., medical, finance, cybersecurity, etc.)
        2. data_type: The type of data they want (e.g., QA, summarization, STS, classification, etc.)  
        3. sample_count: How many data points they need
        4. language: The language they want the data in (they must specify this)
    5. description: (Optional) A description or example of the desired data format (e.g., "triplets: query, positive, negative") or content details.

    Extract these 5 pieces of information from the user's input.
    If the user doesn't specify language, return "en" as default.
    If any other information is missing, make a reasonable guess.
    
    Return ONLY a JSON object with these exact keys: domain_type, data_type, sample_count, language, description
    No explanations or additional text.
    """
        
        prompt = f"""
        Parse this user request: "{user_query}"
        
        Examples:
        "I want 1000 medical QA data points in English"
        → {{"domain_type": "medical", "data_type": "QA", "sample_count": 1000, "language": "en", "description": null}}
        
        "Generate 500 finance classification examples in Spanish with columns: text, category"
        → {{"domain_type": "finance", "data_type": "classification", "sample_count": 500, "language": "es", "description": "columns: text, category"}}
        
        "Need 200 cybersecurity summarization pairs in French about network attacks"
        → {{"domain_type": "cybersecurity", "data_type": "summarization", "sample_count": 200, "language": "fr", "description": "about network attacks"}}
        
        "Create 300 legal QA data in Arabic, where question and answer are short"
        → {{"domain_type": "legal", "data_type": "QA", "sample_count": 300, "language": "ar", "description": "question and answer are short"}}
        
        "I want 150 medical triplets in German language with fields: query, positive_example, negative_example"
        → {{"domain_type": "medical", "data_type": "triplets", "sample_count": 150, "language": "de", "description": "fields: query, positive_example, negative_example"}}
        """
        
        response = self.generate_text(prompt, system_instruction)
        
        try:
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            json_str = response[start_idx:end_idx]
            parsed_data = json.loads(json_str)
            
            # Validate and set defaults
            result = {
                "domain_type": parsed_data.get("domain_type", "general knowledge"),
                "data_type": parsed_data.get("data_type", "general_text"),
                "sample_count": parsed_data.get("sample_count", 100), 
                "language": parsed_data.get("language", "en"),
                "description": parsed_data.get("description", None) # New: Add description
            }
            
            # Ensure sample_count is integer
            try:
                result["sample_count"] = int(result["sample_count"])
            except (ValueError, TypeError):
                result["sample_count"] = 100
                
            return result
            
        except Exception as e:
            self.logger.warning(f"Failed to parse query response: {e}")
            return {
                "domain_type": "general knowledge",
                "data_type": "general_text", 
                "sample_count": 100,
                "language": "en"
            }
            
    def refine_queries(self, domain_type: str, language: str, count: int = 20) -> List[str]:
        """Generate domain-specific search queries in specified language"""
        
        system_instruction = f"""
        Generate {count} diverse search queries for the "{domain_type}" domain in {language} language.
        Focus on different subtopics to maximize topic diversity for synthetic data generation.
        Each query should target different aspects within this domain.
        Return only the queries, one per line, without numbering or bullet points.
        All queries must be written in {language}.
        """
        
        prompt = f"""
        Generate {count} search queries in {language} for "{domain_type}" domain:
        
        Examples for medical domain (but generate in {language}):
        diabetes treatment methods
        cancer prevention strategies
        surgical procedures cardiology
        pharmaceutical drug interactions
        medical diagnosis techniques
        emergency medicine protocols
        mental health therapies
        pediatric care guidelines
        antibiotic resistance mechanisms
        medical imaging technologies
        
        Examples for cybersecurity domain (but generate in {language}):
        network security threats
        malware detection techniques
        data encryption methods
        firewall configuration best practices
        incident response protocols
        
        Examples for finance domain (but generate in {language}):
        investment portfolio management
        risk assessment methodologies
        financial market analysis
        banking regulations compliance
        cryptocurrency trading strategies
        
        Generate {count} similar diverse queries for "{domain_type}" in {language}:
        """
        
        response = self.generate_text(prompt, system_instruction)
        
        # Clean and extract queries
        queries = []
        for line in response.split('\n'):
            clean_line = line.strip('- •').strip('0123456789. ').strip()
            if clean_line and len(clean_line) > 3:
                queries.append(clean_line)
        
        # Ensure we have the right number of queries
        if len(queries) < count:
            fallback_query = f"{domain_type} information"
            queries.extend([fallback_query] * (count - len(queries)))
        
        return queries[:count]
    
    def extract_topics(self, content: str, language: str, domain_type: str) -> List[str]:
        """Extract subtopic names in specified language related to the given domain"""
        
        system_instruction = f"""
        Extract specific, focused subtopics from the provided content in {language} language.
        Each subtopic should be expressed in {language} and be specific enough to generate 5 high-quality synthetic data points.
        Focus on concrete concepts, procedures, methods, or entities mentioned in the content, ensuring they are relevant to the {domain_type} domain.
        Return only subtopic names as a JSON array of strings in {language}.
        """
        
        prompt = f"""
        Extract focused subtopics from this content and express them in {language}, ensuring relevance to the {domain_type} domain:
        {content[:3000]}
        
        Examples of good subtopics (but you should generate in {language} and related to {domain_type}):
        "Diabetes medication side effects"
        "Heart surgery recovery protocols"
        "Cancer screening guidelines"
        "Antibiotic resistance mechanisms"
        "Network intrusion detection"
        "Financial risk assessment methods"
        
        Return JSON array with subtopics in {language}: ["subtopic1", "subtopic2", ...]
        """
        
        response = self.generate_text(prompt, system_instruction)
        
        try:
            start_idx = response.find('[')
            end_idx = response.rfind(']') + 1
            json_str = response[start_idx:end_idx]
            topics_list = json.loads(json_str)
            return [str(topic) for topic in topics_list if isinstance(topic, str)]
        except Exception as e:
            self.logger.warning(f"Failed to extract topics: {e}")
            return []
    
    def generate_synthetic_data(self, topic: str, data_type: str, language: str, description: Optional[str] = None) -> List[Dict[str, Any]]:
        """Generate synthetic data based on a topic, data type, language, and optional description"""
        
        self.logger.debug(f"Generating synthetic data for topic: '{topic}' in {language}")
        
        # Build the prompt with the optional description
        description_prompt = ""
        if description:
            description_prompt = f"""
        The user has provided this description for the desired output:
        ---
        {description}
        ---
        The output format should be inspired by this description.
        """
                
        system_instruction = f"""
        You are a synthetic data generation expert. Your task is to generate a list of JSON objects based on a given topic, data type, and language.
        The output must be a valid JSON list (array of objects).
        Each object in the list should be a unique data point related to the topic.
        Generate exactly {settings.ROWS_PER_SUBTOPIC} data points.
        All text content must be in {language}.
        Do not include any explanations or introductory text outside of the JSON list.
        """
                
        prompt = f"""
        Generate {settings.ROWS_PER_SUBTOPIC} synthetic data points for the topic: "{topic}".
        The data should be of type: "{data_type}".
        The language for all text must be: {language}.
        {description_prompt}
        Return a valid JSON list of objects.
        """
        
        response = self.generate_text(prompt, system_instruction)
        
        # Clean and parse the JSON response
        try:
            # Find the start of the JSON list
            json_start = response.find('[')
            if json_start == -1:
                self.logger.error("No JSON list found in Gemini response")
                return []
            
            # Find the end of the JSON list
            json_end = response.rfind(']')
            if json_end == -1:
                self.logger.error("Incomplete JSON list in Gemini response")
                return []
            
            json_str = response[json_start:json_end+1]
            
            # Parse the JSON string
            data = json.loads(json_str)
            
            if isinstance(data, list):
                self.logger.info(f"Successfully generated {len(data)} data points for topic '{topic}'")
                return data
            else:
                self.logger.warning(f"Generated data is not a list for topic '{topic}'")
                return []
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON for topic '{topic}': {e}")
            self.logger.debug(f"Problematic response: {response}")
            return []
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during data generation for topic '{topic}': {e}")
            return []

# Initialize service
gemini_service = GeminiService()
