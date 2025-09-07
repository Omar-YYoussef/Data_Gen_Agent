from typing import List, Dict, Any, Optional
import requests
import logging
import time
from config.settings import settings

class TavilyService:
    """Service for Tavily web search integration with enhanced error handling"""
    
    def __init__(self):
        if not settings.TAVILY_API_KEY:
            raise ValueError("TAVILY_API_KEY not found in environment variables")
        
        self.api_key = settings.TAVILY_API_KEY
        self.base_url = "https://api.tavily.com"
        self.logger = logging.getLogger(__name__)
    
    def search(self, query: str, max_results: int = 5, max_retries: int = 3) -> List[Dict[str, Any]]:
        """Search using Tavily API with retry logic"""
        
        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}/search"
                
                payload = {
                    "api_key": self.api_key,
                    "query": query,
                    "search_depth": "advanced",
                    "include_answer": False,
                    "include_raw_content": False,
                    "max_results": max_results,
                    "include_domains": [],
                    "exclude_domains": []
                }
                
                response = requests.post(url, json=payload, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                results = data.get("results", [])
                
                # Format results consistently
                formatted_results = []
                for result in results:
                    formatted_result = {
                        "url": result.get("url", ""),
                        "title": result.get("title", ""),
                        "content": result.get("content", ""),
                        "score": result.get("score", 0.0)
                    }
                    formatted_results.append(formatted_result)
                
                self.logger.info(f"Search '{query}' returned {len(formatted_results)} results")
                return formatted_results
                
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Tavily search failed for query '{query}' after {max_retries} attempts: {e}")
                    return []
                else:
                    self.logger.warning(f"Tavily search attempt {attempt + 1} failed for '{query}': {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        return []

# Initialize service
tavily_service = TavilyService()
