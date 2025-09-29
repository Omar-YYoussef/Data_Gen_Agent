from typing import Dict, Any, Optional
from models.data_schemas import ParsedQuery
from agents.base_agent import BaseAgent
from services.gemini_service import gemini_service
from config.settings import settings  # Import settings
import math
class QueryParserAgent(BaseAgent):
    """Agent to parse user queries and extract domain, data type, and sample count"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("query_parser", config)
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate that input is a non-empty string"""
        return isinstance(input_data, str) and len(input_data.strip()) > 0
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate that output is a ParsedQuery object"""
        return isinstance(output_data, ParsedQuery)
    
    def execute(self, input_data: str, context: Optional[Dict[str, Any]] = None) -> ParsedQuery:
        """Analyzes and parses the user query in a single step."""
        
        self.logger.info(f"Analyzing and parsing query: {input_data}")
        
        try:
            # Use the combined service method
            parsed_data = gemini_service.check_and_parse_query(input_data)
            query_type = parsed_data.get("query_type")

            if query_type == "not_data_generation":
                self.logger.warning("Query is not related to data generation.")
                raise ValueError("I am a data generation pipeline. Please provide a prompt for generating data.")
            
            if query_type == "incomplete":
                self.logger.warning("Query is an incomplete data generation request.")
                raise ValueError("Your request is incomplete. Please specify the number of rows, a description of the data, and the data type (e.g., '1000 medical QA pairs').")

            if query_type == "data_generation":
                self.logger.info("Query identified as a valid data generation request.")
                
                # Create ParsedQuery object from the already-parsed data
                parsed_query = ParsedQuery(
                    original_query=input_data,
                    domain_type=parsed_data.get("domain_type", "general knowledge"),
                    data_type=parsed_data.get("data_type"),
                    sample_count=parsed_data.get("sample_count"),
                    language=parsed_data.get("language", "en"),
                    iso_language=parsed_data.get("iso_language"), # New: ISO 639-1 language code
                    description=parsed_data.get("description")
                )
                
                # Calculate required subtopics based on settings
                required_subtopics = math.ceil(parsed_query.sample_count / settings.ROWS_PER_SUBTOPIC)
                
                self.logger.info("Parsed query successfully:")
                self.logger.info(f"  - Domain: {parsed_query.domain_type}")
                self.logger.info(f"  - Data Type: {parsed_query.data_type}")
                self.logger.info(f"  - Sample Count: {parsed_query.sample_count}")
                self.logger.info(f"  - Required Subtopics: {required_subtopics}")
                
                return parsed_query
            
            # Fallback for any unexpected query_type
            self.logger.error(f"Unexpected query type returned: {query_type}")
            raise ValueError("Failed to understand the query type.")

        except Exception as e:
            self.logger.error(f"Failed during query parsing and analysis: {e}")
            raise
