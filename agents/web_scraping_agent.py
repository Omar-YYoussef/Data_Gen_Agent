from typing import Dict, Any, Optional, List
from models.data_schemas import SearchResult, ScrapedContent
from agents.base_agent import BaseAgent
from services.crawl4ai_service import crawl4ai_service
from config.settings import settings
from datetime import datetime
import time

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
        
        self.logger.info(f"Starting to scrape {len(input_data)} URLs")
        
        scraped_results = []
        successful_scrapes = 0
        failed_scrapes = 0
        
        try:
            for i, search_result in enumerate(input_data):
                self.logger.info(f"Scraping ({i+1}/{len(input_data)}): {search_result.url}")
                
                # Scrape the URL
                scrape_data = crawl4ai_service.scrape_url(search_result.url)
                
                if scrape_data and scrape_data.get("success", False):
                    # Create ScrapedContent object
                    scraped_content = ScrapedContent(
                        url=search_result.url,
                        title=scrape_data.get("title", search_result.title),
                        content=scrape_data.get("content", ""),
                        metadata={
                            "original_title": search_result.title,
                            "original_snippet": search_result.snippet,
                            "source_query": search_result.source_query,
                            "relevance_score": search_result.relevance_score,
                            "word_count": scrape_data.get("word_count", 0)
                        },
                        scraping_timestamp=datetime.now(),
                        content_length=len(scrape_data.get("content", "")),
                        success=True
                    )
                    
                    scraped_results.append(scraped_content)
                    successful_scrapes += 1
                    
                    # Save individual scraped content
                    filename = f"scraped_{i:03d}_{hash(search_result.url) % 10000:04d}.json"
                    self.save_data(
                        {
                            "url": scraped_content.url,
                            "title": scraped_content.title,
                            "content": scraped_content.content,
                            "metadata": scraped_content.metadata,
                            "timestamp": scraped_content.scraping_timestamp.isoformat(),
                            "content_length": scraped_content.content_length
                        },
                        filename,
                        settings.RAW_CONTENT_PATH / "scraped_files"
                    )
                
                else:
                    # Failed scrape
                    error_msg = scrape_data.get("error", "Unknown error") if scrape_data else "No response"
                    
                    scraped_content = ScrapedContent(
                        url=search_result.url,
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
                    
                    scraped_results.append(scraped_content)
                    failed_scrapes += 1
                
                # Rate limiting between requests
                if i < len(input_data) - 1:
                    time.sleep(2)  # 2 second delay between scrapes
            
            # Log summary statistics
            self.logger.info(f"Scraping complete: {successful_scrapes} successful, {failed_scrapes} failed")
            
            # Save summary data
            summary_data = {
                "total_urls": len(input_data),
                "successful_scrapes": successful_scrapes,
                "failed_scrapes": failed_scrapes,
                "success_rate": f"{(successful_scrapes/len(input_data))*100:.1f}%" if input_data else "0%",
                "scraped_files": [f"scraped_{i:03d}_{hash(result.url) % 10000:04d}.json" 
                                for i, result in enumerate(input_data) if any(s.url == result.url and s.success for s in scraped_results)]
            }
            
            self.save_data(
                summary_data,
                "scraping_summary.json",
                settings.DEBUG_PATH
            )
            
            return scraped_results
            
        except Exception as e:
            self.logger.error(f"Web scraping failed: {e}")
            raise
