from typing import List, Dict, Any, Optional
import requests
import logging
import time
from config.settings import settings

class TavilyQuotaExhaustedError(Exception):
    """Custom exception for when the Tavily API quota is exhausted."""
    pass

class TavilyService:
    """Service for Tavily web search integration with enhanced error handling"""
    
    def __init__(self):
        if not settings.TAVILY_API_KEY:
            raise ValueError("TAVILY_API_KEY not found in environment variables")
        
        self.api_key = settings.TAVILY_API_KEY
        self.base_url = "https://api.tavily.com"
        self.logger = logging.getLogger(__name__)
    
    def search(self, query: str, max_results: int = 5, max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        Search using Tavily API with retry logic.
        Raises TavilyQuotaExhaustedError if the API quota is depleted.
        """
        
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
                
            except requests.exceptions.HTTPError as http_err:
                # Specifically check for quota-related errors (402: Payment Required, 432: Unassigned)
                if http_err.response.status_code in [402, 432]:
                    self.logger.error("Tavily API quota exhausted. Stopping search.")
                    raise TavilyQuotaExhaustedError("Tavily API quota exhausted.") from http_err
                
                error_text = http_err.response.text.lower()
                if "quota" in error_text or "credit" in error_text:
                    self.logger.error("Tavily API quota exhausted based on error message. Stopping search.")
                    raise TavilyQuotaExhaustedError("Tavily API quota exhausted.") from http_err

                # For other HTTP errors, proceed with retry logic
                if attempt == max_retries - 1:
                    self.logger.error(f"Tavily search failed for query '{query}' after {max_retries} attempts: {http_err}")
                    return []
                else:
                    self.logger.warning(f"Tavily search attempt {attempt + 1} failed for '{query}': {http_err}")
                    time.sleep(2 ** attempt)  # Exponential backoff

            except Exception as e:
                # For non-HTTP errors (e.g., network issues)
                if attempt == max_retries - 1:
                    self.logger.error(f"Tavily search failed for query '{query}' after {max_retries} attempts with a non-HTTP error: {e}")
                    return []
                else:
                    self.logger.warning(f"Tavily search attempt {attempt + 1} failed for '{query}': {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        return []

# Initialize service
tavily_service = TavilyService()
