import google.generativeai as genai
from typing import Dict, Any, List, Optional
import asyncio
import json
import time
import logging
from ..config.settings import settings

class GeminiQuotaExhaustedError(Exception):
    """Custom exception for when all Gemini API keys have their quotas exhausted."""
    pass

class GeminiService:
    """Enhanced service with flexible JSON generation for user-specified data types"""
    
    def __init__(self, model_name: str = 'gemini-2.5-flash'):
        if not settings.GEMINI_API_KEY:
            raise ValueError("No GEMINI_API_KEYS found in environment variables. Please provide at least one.")
        
        self.api_key = settings.GEMINI_API_KEY 
        self._configure_gemini_with_current_key()

        self.model = genai.GenerativeModel(model_name)
        self.logger = logging.getLogger(__name__)
        
        self.generation_config = genai.types.GenerationConfig(
            temperature=settings.GEMINI_TEMPERATURE,
            max_output_tokens=settings.GEMINI_MAX_TOKENS,
        )

    def _configure_gemini_with_current_key(self):
        genai.configure(api_key=self.api_key)

    def generate_text(self, prompt: str, 
                     system_instruction: Optional[str] = None,
                     max_retries: int = 3) -> str:
        """Generate text using Gemini with enhanced retry logic, API key rotation, and cooldown."""
        
        full_prompt = prompt
        if system_instruction:
            full_prompt = f"System: {system_instruction}\n\nUser: {prompt}"
        
        try:
            self.logger.info(f"Attempting Gemini API call with key index: {0}")
            response = self.model.generate_content(
                full_prompt,
                generation_config=self.generation_config
            )
            return response.text
        except Exception as e:
            error_message = str(e).lower()
            self.logger.error(f"Gemini API call with key index {0} failed: {e}")
            
            if "rate limit" in error_message or "quota" in error_message or "resource has been exhausted" in error_message:
                raise Exception(f"Gemini API quota exhausted for key index {0}. Stopping.") from e
            else:
                # For other errors, re-raise to indicate failure
                raise Exception(f"Gemini API call failed unexpectedly for key index {0}. Error: {e}") from e

    def check_and_parse_query(self, user_query: str) -> Dict[str, Any]:
        """
        Checks the user's query intent and parses it in a single request.

        Returns a dictionary indicating the query type and, if applicable, the parsed details.
        """
        system_instruction = """


            You are a specialized query classifier and parameter extractor for a data generation pipeline. Your role is to parse user requests with precision and return structured JSON responses that enable downstream processing.

            ## Core Function
            Analyze incoming user queries and classify them into one of three categories, extracting relevant parameters when applicable. Return only a single, valid JSON object with no additional text.

            ## Analysis Framework

            ### Step 1: Intent Classification
            Determine the user's primary intent by evaluating:

            **Data Generation Requests** - Look for:
            - Explicit requests for data creation/generation
            - Mentions of datasets, samples, examples, or data points
            - Specifications of data types (e.g., "generate classification data", "create QA pairs")
            - Keywords: generate, create, produce, build, make + data/dataset/samples/examples

            **Non-Data Generation Requests** - Include:
            - Greetings, casual conversation, or meta-questions
            - Requests for explanations, help, or general information
            - Questions about the system itself
            - Any request not related to data generation

            ### Step 2: Completeness Assessment
            For data generation requests, verify ALL required parameters are present:

            **Required Parameters:**
            - **Sample Count**: Explicit numeric value (e.g., "100 samples", "50 examples", "1000 rows")
            - **Data Description**: Clear indication of what type of data is needed
            - **Data Type**: Format or structure specification
            - **Language**: Target language for generation

            **Optional Parameters:**
            - **Domain**: Specific field or industry context
            - **Categories**: User-defined categories within the domain (e.g., "cardiovascular, neurology, oncology" for medical domain)

            ## Response Format Specifications

            ### Case 1: Non-Data Generation
            {"query_type": "not_data_generation"}

            ### Case 2: Incomplete Data Generation Request
            {"query_type": "incomplete"} 

            ### Case 3: Complete Data Generation Request
            {
                "query_type": "data_generation",
                "domain_type": "string - specific domain/industry context or 'general' if unspecified",
                "data_type": "string",
                "sample_count": integer - exact number requested (never estimate or default),
                "language": "string - full language name (e.g., English, Egyptian Arabic, German, French, Spanish, Arabic)",
                "iso_language": "string - ISO 639-1 code (e.g., en, ar). For dialects, use the base language code (e.g., 'ar' for Egyptian Arabic)",
                "description": "string - comprehensive summary of requirements, constraints, and formatting details",
                "categories": "array of strings - user-defined categories within the domain, or null if not specified"
            }

            ## Critical Guidelines

            1. **Strict JSON Output**: Return ONLY valid JSON. No explanations, comments, or additional text.

            2. **No Assumptions**: If sample_count is missing or ambiguous, classify as "incomplete". Never guess quantities.

            3. **Parameter Extraction Rules**:
            - domain_type`: Extract from context clues (medical, finance, education, etc.) or use "general"
            - data_type`: Identify structure (QA, classification, text-label pairs, etc.) or use "unspecified"
            - sample_count`: Must be explicitly stated number only
            - language: "full language name (e.g., English, Arabic, Egyptian Arabic)"
            - iso_language: "ISO 639-1 code (e.g., en, ar). For dialects, use the base language code (e.g., 'ar' for Egyptian Arabic)"
            - description`: Capture ALL specific requirements, formatting needs, and constraints
            - categories`: Extract user-defined subcategories within the domain (e.g., ["cardiovascular", "neurology"] for medical) or null if not specified

            4. **Edge Case Handling**:
            - Ambiguous quantities (e.g., "some data", "a few examples") → incomplete
            - Multiple conflicting requirements → capture all in description
            - Vague data descriptions → may still be complete if sample count is clear

            ## Quality Assurance
            Before responding, verify:
            - ✓ JSON is syntactically valid
            - ✓ All required fields are present for complete requests
            - ✓ No extraneous text or explanations included
            - ✓ Classification aligns with intent analysis
            - ✓ Parameter extraction is accurate and conservative """

        prompt = f"""
        Analyze this user query: "{user_query}"

        ---
        Examples:
        User Query: "I want 1000 medical QA data points in English"
        JSON Output: {{"query_type": "data_generation", "domain_type": "medical", "data_type": "QA", "sample_count": 1000, "language": "English", "iso_language": "en", "description":data columns are (question, answer) and mix between shor and long exampls}}

        User Query: "Generate 500 classification examples"
        JSON Output: {{"query_type": "data_generation", "data_type": "classification", "sample_count": 500, "language": "English", "iso_language": "en", "description": "the data contains two columns(text, label)"}}

        User Query: "I need data about cybersecurity"
        JSON Output: {{"query_type": "incomplete"}}

        User Query: "Create some data for me"
        JSON Output: {{"query_type": "incomplete"}}
        
        User Query: "Generate medical QA data"
        JSON Output: {{"query_type": "incomplete"}}

        User Query: "Generate 500 medical QA pairs in English covering cardiovascular and neurology topics"
        JSON Output: {{"query_type": "data_generation", "domain_type": "medical", "data_type": "QA", "sample_count": 500, "language": "English", "iso_language": "en", "description": "QA pairs with question and answer columns", "categories": ["cardiovascular", "neurology"]}}

        User Query: "Create 200 finance classification examples in Arabic"
        JSON Output: {{"query_type": "data_generation", "domain_type": "finance", "data_type": "classification", "sample_count": 200, "language": "Arabic", "iso_language": "ar", "description": "text-label classification pairs", "categories": null}}

        User Query: "How are you?"
        JSON Output: {{"query_type": "not_data_generation"}}

        User Query: "Who created you?"
        JSON Output: {{"query_type": "not_data_generation"}}
        ---

        Now, analyze the user query provided at the start of this prompt and return the appropriate JSON object.
        """

        response = self.generate_text(prompt, system_instruction)
        
        try:
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            json_str = response[start_idx:end_idx]
            parsed_data = json.loads(json_str)
            

            return parsed_data

        except (json.JSONDecodeError, IndexError) as e:
            self.logger.error(f"Failed to decode JSON from query analysis: {e}. Response: '{response}'")
            # Fallback for safety
            return {"query_type": "not_data_generation"}
            
    def refine_queries(self, domain_type: str, language: str, count: int = 20, categories: Optional[List[str]] = None) -> List[str]:
        """Generate domain-specific search queries in specified language"""
        
        # Determine category instruction based on whether categories are provided
        if categories:
            category_instruction = f"""
        ## Category Focus
            The user has specified the following categories within the {domain_type} domain: {', '.join(categories)}
            - Prioritize generating queries that cover these specific categories
            - Ensure queries are distributed across all provided categories
            - Focus on sub-topics, methods, and concepts within these categories
            - Use category-specific terminology and concepts
            """
        else:
            category_instruction = f"""
        ## Domain Coverage
            Cover the entire {domain_type} domain comprehensively since no specific categories were provided.
            - Ensure broad coverage across all major aspects of the domain
            - Include foundational and advanced topics
            """

        system_instruction = f"""
        You are an expert query generator specializing in creating diverse, high-quality search queries for synthetic data generation pipelines.

        ## Task Overview
            Generate {count} strategically diverse search queries for the "{domain_type}" domain in {language} language. 
            These queries will be used to create training data, so maximize topical coverage and query variety.
        
        {category_instruction}
        
        ### Topic Breadth
            - Include both foundational concepts and advanced/specialized areas
            - Balance theoretical knowledge with practical applications
            - Span different complexity levels (beginner to expert)
        ### Specificity
            - Each query should target a distinct aspect of the domain
            - Avoid overly generic or vague questions
            - Include specific terminology, methods, tools, or concepts when relevant
            - Ensure queries are detailed enough to generate meaningful responses
        ## Output Format
            - Return exactly {count} queries
            - One query per line
            - No numbering, bullet points, or additional formatting
            - All queries must be grammatically correct in {language}
            - Ensure each query is self-contained and clear
        Generate diverse, high-quality search queries that will enable comprehensive synthetic data creation for the {domain_type} domain.
        """
        
        # Create category-specific prompt
        if categories:
            category_prompt = f"""
        Categories to focus on: {', '.join(categories)}
        - Ensure queries are distributed across these categories
        - Use terminology specific to each category
        """
        else:
            category_prompt = """
        - Cover the entire domain comprehensively
        - Include all major subtopics and specializations
        """

        prompt = f"""
        Generate {count} diverse and professional search queries in {language} for the "{domain_type}" domain.

        {category_prompt}

        Requirements:
        - Each query should be 2-10 words long and use domain-specific terminology
        - Queries must cover different aspects, subtopics, and specializations within the domain
        - Use professional vocabulary that experts in this field would search for
        - Ensure variety in query types: techniques, methods, tools, concepts, procedures, guidelines, and technologies
        - All queries must be in {language}
        - Focus on practical, actionable, and research-oriented terms
        - Avoid generic or overly broad queries
        - Each query should be distinct and non-redundant

        Output format:
        - List each query on a separate line
        - Use lowercase unless proper nouns are involved
        - No numbering or bullet points needed

        Generate {count} search queries for "{domain_type}" domain in {language}:
        """
        
        # Generate {count} similar diverse queries for "{domain_type}" in {language}:
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
    

    async def extract_topics_async(self, content: str, language: str, domain_type: str) -> List[str]:
        """Asynchronously extract subtopic names in specified language related to the given domain"""
        
        system_instruction = f"""
        You are an expert content analyst specializing in subtopic extraction for synthetic data generation. 
        Your task is to identify optimal subtopics from provided content that will enable high-quality, focused data point creation.
        ## What to look for:
            - Main concepts, methods, or procedures mentioned
            - Specific topics that have enough depth for questions/examples
            - Concrete subjects rather than vague themes

        ## Guidelines:
            - Each subtopic should be specific enough to create multiple related examples
            - Focus only on topics clearly present in the content
            - Use {language} for all subtopic names
            - Keep subtopics relevant to {domain_type}

        ## Examples:
            Instead of: "General principles" 
            Use: "Risk assessment procedures"

            Instead of: "Technology" 
            Use: "Database indexing strategies"

        ## Output:
            Return a JSON array of subtopic strings in {language}:
            ["subtopic 1", "subtopic 2", "subtopic 3"]

            Extract 5-10 focused subtopics from the content.
        """
        
        prompt = f"""
        Extract focused subtopics from this content and express them in {language}, ensuring relevance to the {domain_type} domain:
        {content}
        
        Examples of good subtopics:
        "Diabetes medication side effects"
        "Heart surgery recovery protocols"
        "Cancer screening guidelines"
        "Antibiotic resistance mechanisms"
        "Network intrusion detection"
        "Financial risk assessment methods"
    
        Make sure the subtopics are falling within the {domain_type}, this point is very important.
        Return JSON array with subtopics in {language}: ["subtopic1", "subtopic2", ...]
        """
        
        response = await self.generate_text_async(prompt, system_instruction)
        time.sleep(3)
        try:
            start_idx = response.find('[')
            end_idx = response.rfind(']') + 1
            json_str = response[start_idx:end_idx]
            topics_list = json.loads(json_str)
            return [str(topic) for topic in topics_list if isinstance(topic, str)]
        except Exception as e:
            self.logger.warning(f"Failed to extract topics asynchronously: {e}")
            return []

    async def generate_synthetic_data(self, topic: str, data_type: str, language: str, description: Optional[str] = None) -> List[Dict[str, Any]]:
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
        Generate {settings.ROWS_PER_SUBTOPIC} unique data points for the given topic in {language}.
        ## Requirements:
        - Return only a valid JSON array
        - Each object should be different but related to the topic
        - All text must be in {language}
        - No explanations or extra text

        """
                
        prompt = f"""

        Generate {settings.ROWS_PER_SUBTOPIC} high-quality, diverse data points about "{topic}" as {data_type} in {language}.

        ## Requirements:
            - Ensure each data point is clear, self-contained, and immediately understandable without additional context
            - Make every data point completely independent—avoid cross-references, pronouns referring to other entries, or sequential dependencies
            - Vary sentence structure, complexity, and vocabulary to create natural diversity across all data points
            - Use authentic, natural {language} appropriate for the context and domain
            - Ensure factual accuracy and cultural appropriateness for {language} speakers
            - Return ONLY a valid JSON array with no additional text, explanations, or markdown formatting

        Follow this specific description and constraints:
            {description_prompt}
        """
        
        response = await self.generate_text_async(prompt, system_instruction)
        
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

    async def generate_text_async(self, prompt: str, 
                                system_instruction: Optional[str] = None,
                                max_retries: int = 3) -> str:
        """Asynchronously generate text using Gemini with enhanced retry logic and cooldown."""
        
        full_prompt = prompt
        if system_instruction:
            full_prompt = f"System: {system_instruction}\n\nUser: {prompt}"
        
        try:
            self.logger.info(f"Attempting async Gemini API call with key index: {0}")
            response = await self.model.generate_content_async(
                full_prompt,
                generation_config=self.generation_config
            )
            return response.text
        except Exception as e:
            error_message = str(e).lower()
            self.logger.error(f"Async Gemini API call with key index {0} failed: {e}")
            
            if "rate limit" in error_message or "quota" in error_message or "resource has been exhausted" in error_message:
                raise GeminiQuotaExhaustedError(f"Gemini API quota exhausted for key index {0}. Stopping.") from e
            else:
                # For other errors, re-raise to indicate failure
                raise Exception(f"Async Gemini API call failed unexpectedly for key index {0}. Error: {e}") from e
