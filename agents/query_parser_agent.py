from typing import Dict, Any, Optional
from models.data_schemas import ParsedQuery
from agents.base_agent import BaseAgent
from services.gemini_service import gemini_service
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
        """Parse user query into structured format"""
        
        self.logger.info(f"Parsing query: {input_data}")
        
        try:
            # Use Gemini to parse the query
            parsed_data = gemini_service.parse_query(input_data)
            
            # Create ParsedQuery object
            parsed_query = ParsedQuery(
                original_query=input_data,
                domain_type=parsed_data.get("domain_type", "general knowledge"),
                data_type=parsed_data.get("data_type", "general_text"),
                sample_count=parsed_data.get("sample_count", 100),
                language=parsed_data.get("language", "en"),
                description=parsed_data.get("description", None) # New: Pass description
            )
            
            # Calculate required subtopics
            required_subtopics = math.ceil(parsed_query.sample_count / 5)
            
            self.logger.info(f"Parsed query successfully:")
            self.logger.info(f"  - Domain: {parsed_query.domain_type}")
            self.logger.info(f"  - Data Type: {parsed_query.data_type}")
            self.logger.info(f"  - Sample Count: {parsed_query.sample_count}")
            self.logger.info(f"  - Required Subtopics: {required_subtopics}")
            
            return parsed_query
            
        except Exception as e:
            self.logger.error(f"Failed to parse query: {e}")
            raise
