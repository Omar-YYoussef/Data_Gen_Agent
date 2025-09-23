from typing import Dict, Any, Optional, List
from models.data_schemas import SearchQuery, SearchResult
from agents.base_agent import BaseAgent
from services.tavily_service import tavily_service, TavilyQuotaExhaustedError
from config.settings import settings
import time

class WebSearchAgent(BaseAgent):
    """Agent to perform web searches using Tavily with 20x5 strategy"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("web_search", config)
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate that input is a list of SearchQuery objects"""
        return (isinstance(input_data, list) and 
                len(input_data) > 0 and
                all(isinstance(item, SearchQuery) for item in input_data))
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate that output is a list of SearchResult objects"""
        return (isinstance(output_data, list) and 
                all(isinstance(item, SearchResult) for item in output_data))
    
    def execute(self, input_data: List[SearchQuery], context: Optional[Dict[str, Any]] = None) -> List[SearchResult]:
        """
        Perform web searches for all queries, stopping if the Tavily quota is exhausted.
        If the quota is exhausted before any results are gathered, it halts the workflow.
        """
        
        self.logger.info(f"Performing web search for {len(input_data)} queries")
        self.logger.info(f"Strategy: {settings.SEARCH_RESULTS_PER_QUERY} results per query")
        
        all_results = []
        
        try:
            for i, search_query in enumerate(input_data):
                try:
                    self.logger.info(f"Searching ({i+1}/{len(input_data)}): {search_query.query}")
                    
                    # Perform search using Tavily
                    search_results = tavily_service.search(
                        search_query.query, 
                        max_results=settings.SEARCH_RESULTS_PER_QUERY
                    )
                    
                    # Convert to SearchResult objects
                    for result in search_results:
                        search_result = SearchResult(
                            url=result["url"],
                            title=result["title"],
                            snippet=result["content"][:500],  # Limit snippet length
                            relevance_score=result.get("score"),
                            source_query=search_query.query
                        )
                        all_results.append(search_result)
                    
                    # Rate limiting between searches
                    if i < len(input_data) - 1:  # Don't sleep after the last query
                        time.sleep(3)

                except TavilyQuotaExhaustedError:
                    self.logger.error("Tavily API quota is exhausted. Halting all further web searches.")
                    if not all_results:
                        # If we have no results at all, stop the entire workflow.
                        raise Exception("Web search failed completely due to Tavily quota exhaustion. Halting workflow.")
                    else:
                        # If we have some results, just stop searching and proceed.
                        self.logger.warning("Proceeding with partially collected search results.")
                        break 
            
            self.logger.info(f"Total search results collected: {len(all_results)}")
            
            # If the loop completes and we still have no results (e.g., all queries failed for other reasons)
            if not all_results:
                self.logger.error("No search results could be collected from any query.")
                raise Exception("Web search returned no results for any query. Halting workflow.")

            return all_results
            
        except Exception as e:
            # Catch the exception raised from the loop or any other unexpected error
            self.logger.error(f"An error occurred in the web search agent that will halt the workflow: {e}")
            raise
