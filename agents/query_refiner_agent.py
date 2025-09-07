from typing import Dict, Any, Optional, List
from models.data_schemas import ParsedQuery, SearchQuery
from agents.base_agent import BaseAgent
from services.gemini_service import gemini_service
from config.settings import settings

class QueryRefinerAgent(BaseAgent):
    """Agent to generate domain-specific search queries"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("query_refiner", config)
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate that input is a ParsedQuery"""
        return isinstance(input_data, ParsedQuery)
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate that output is a list of SearchQuery objects"""
        return (isinstance(output_data, list) and 
                len(output_data) > 0 and
                all(isinstance(item, SearchQuery) for item in output_data))
    
    def execute(self, input_data: ParsedQuery, context: Optional[Dict[str, Any]] = None) -> List[SearchQuery]:
        """Generate refined search queries for the domain"""
        
        self.logger.info(f"Refining queries for domain: {input_data.domain_type}")
        self.logger.info(f"Target: {settings.REFINED_QUERIES_COUNT} queries")
        
        try:
            # Generate refined queries using Gemini
            refined_queries = gemini_service.refine_queries(
                input_data.domain_type, 
                language=input_data.language, 
                count=settings.REFINED_QUERIES_COUNT
            )
            
            # Create SearchQuery objects
            search_queries = []
            for i, query in enumerate(refined_queries):
                search_query = SearchQuery(
                    query=query,
                    domain=input_data.domain_type,
                    priority=i + 1
                )
                search_queries.append(search_query)
            
            self.logger.info(f"Generated {len(search_queries)} refined queries")
            
            # Save queries for debugging
            queries_data = [
                {
                    "query": sq.query, 
                    "domain": sq.domain, 
                    "priority": sq.priority
                } 
                for sq in search_queries
            ]
            
            self.save_data(
                queries_data,
                f"refined_queries_{input_data.domain_type.replace(' ', '_')}.json",
                settings.DEBUG_PATH
            )
            
            return search_queries
            
        except Exception as e:
            self.logger.error(f"Failed to refine queries: {e}")
            raise
