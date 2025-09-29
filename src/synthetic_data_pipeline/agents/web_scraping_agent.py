from typing import Dict, Any, Optional, List
from ..models.data_schemas import SearchResult, ScrapedContent
from ..agents.base_agent import BaseAgent
from ..services.scraping_service import scraping_service
from ..config.settings import settings
from datetime import datetime
import time
import asyncio
import concurrent.futures
import threading

class WebScrapingAgent(BaseAgent):
    """Agent to scrape web content from filtered URLs"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("web_scraping", config)
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate that input is a list of SearchResult objects"""
        return (isinstance(input_data, list) and 
                all(isinstance(item, SearchResult) for item in input_data))
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate that output is a list of ScrapedContent objects"""
        return (isinstance(output_data, list) and 
                all(isinstance(item, ScrapedContent) for item in output_data))
    
    def execute(self, input_data: List[SearchResult], context: Optional[Dict[str, Any]] = None) -> List[ScrapedContent]:
        """Scrape content from all provided URLs"""
        
        self.logger.info(f"Starting to scrape {len(input_data)} URLs concurrently")
        
        # Since the main loop is now async, we can directly await the async method
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # If a loop is running, we can schedule the task
                scraped_results = asyncio.ensure_future(self.ascrape_all(input_data))
            else:
                # This case is less likely if called from an async main
                scraped_results = loop.run_until_complete(self.ascrape_all(input_data))
        except RuntimeError:
            # No event loop, create a new one
            scraped_results = asyncio.run(self.ascrape_all(input_data))

        # This part needs to be adapted if you want to wait for the results here.
        # For now, assuming ascrape_all is awaited in an async context.
        # The logic below is better placed in an async execute method.
        
        # This synchronous `execute` method is now primarily a wrapper.
        # The actual processing and logging of results should happen where it's awaited.
        # Let's assume the caller will handle the async nature.
        # For direct calls to `run`, we'll run it to completion.
        
        if not asyncio.iscoroutine(scraped_results):
             # If it's not a coroutine, it means it was run to completion
             successful_scrapes = sum(1 for r in scraped_results if r.success)
             failed_scrapes = len(scraped_results) - successful_scrapes
             self.logger.info(f"Scraping complete: {successful_scrapes} successful, {failed_scrapes} failed")
             return scraped_results
        else:
            # If it is a coroutine, it needs to be awaited by the caller
            # This path is taken when called from an async context that awaits the result of `run`
            # which in turn calls this `execute` method.
            # To make this work synchronously, we must run the loop.
            loop = asyncio.get_event_loop()
            final_results = loop.run_until_complete(scraped_results)
            successful_scrapes = sum(1 for r in final_results if r.success)
            failed_scrapes = len(final_results) - successful_scrapes
            self.logger.info(f"Scraping complete: {successful_scrapes} successful, {failed_scrapes} failed")
            return final_results
    
    async def execute_async(self, input_data: List[SearchResult], context: Optional[Dict[str, Any]] = None) -> List[ScrapedContent]:
        """Async version of execute - use this when calling from async context"""
        self.logger.info(f"Starting to scrape {len(input_data)} URLs concurrently (async)")
        
        scraped_results = await self.ascrape_all(input_data)
        
        successful_scrapes = sum(1 for r in scraped_results if r.success)
        failed_scrapes = len(scraped_results) - successful_scrapes
        
        self.logger.info(f"Scraping complete: {successful_scrapes} successful, {failed_scrapes} failed")
        
        return scraped_results

    async def ascrape_all(self, input_data: List[SearchResult]) -> List[ScrapedContent]:
        """Asynchronously scrape all URLs"""
        
        urls_to_scrape = [item.url for item in input_data]
        
        # Use the service's concurrent scraping method
        scraped_data_list = await scraping_service.scrape_multiple_urls(urls_to_scrape)
        
        # Create a dictionary for quick lookup of original search results
        input_map = {item.url: item for item in input_data}
        
        final_results = []
        for scrape_data in scraped_data_list:
            url = scrape_data["url"]
            search_result = input_map.get(url)
            
            if not search_result:
                self.logger.warning(f"Could not find original search result for scraped URL: {url}")
                continue

            if scrape_data and scrape_data.get("success", False):
                scraped_content = ScrapedContent(
                    url=url,
                    title=scrape_data.get("title", search_result.title),
                    content=scrape_data.get("content", ""),
                    metadata={
                        "original_title": search_result.title,
                        "original_snippet": search_result.snippet,
                        "source_query": search_result.source_query,
                        "relevance_score": search_result.relevance_score,
                        "word_count": scrape_data.get("word_count", 0),
                        "links": scrape_data.get("links", []),
                        "images": scrape_data.get("images", [])
                    },
                    scraping_timestamp=datetime.now(),
                    content_length=len(scrape_data.get("content", "")),
                    success=True
                )
            else:
                error_msg = scrape_data.get("error", "Unknown error") if scrape_data else "No response"
                scraped_content = ScrapedContent(
                    url=url,
                    title=search_result.title,
                    content="",
                    metadata={
                        "original_snippet": search_result.snippet,
                        "source_query": search_result.source_query,
                        "error": error_msg
                    },
                    scraping_timestamp=datetime.now(),
                    content_length=0,
                    success=False
                )
            
            final_results.append(scraped_content)
            
        return final_results
