from typing import Dict, Any, Optional, List, Set
from models.data_schemas import SearchResult
from agents.base_agent import BaseAgent
from config.settings import settings
from urllib.parse import urlparse
import hashlib

class FiltrationAgent(BaseAgent):
    """Agent to filter and deduplicate search results"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("filtration", config)
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate that input is a list of SearchResult objects"""
        return (isinstance(input_data, list) and 
                all(isinstance(item, SearchResult) for item in input_data))
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate that output is a list of SearchResult objects"""
        return (isinstance(output_data, list) and 
                all(isinstance(item, SearchResult) for item in output_data))
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for comparison"""
        try:
            parsed = urlparse(url.lower().strip())
            # Remove common URL variations
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if normalized.endswith('/'):
                normalized = normalized[:-1]
            return normalized
        except Exception as e:
            self.logger.debug(f"Failed to normalize URL {url}: {e}")
            return url.lower().strip()
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid for scraping"""
        try:
            parsed = urlparse(url)
            
            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Filter out unwanted file extensions
            excluded_extensions = {
                '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
                '.zip', '.rar', '.tar', '.gz', '.mp4', '.avi', '.mp3',
                '.jpg', '.jpeg', '.png', '.gif', '.bmp'
            }
            if any(url.lower().endswith(ext) for ext in excluded_extensions):
                return False
            
            # Filter out problematic domains
            excluded_domains = {
                'twitter.com', 'facebook.com', 'instagram.com', 'linkedin.com',
                'youtube.com', 'tiktok.com', 'reddit.com'
            }
            if any(domain in parsed.netloc.lower() for domain in excluded_domains):
                return False
            
            # Filter out URLs that are too short or suspicious
            if len(url) < 10:
                return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"URL validation failed for {url}: {e}")
            return False
    
    def _calculate_content_hash(self, title: str, snippet: str) -> str:
        """Calculate hash for content deduplication"""
        content = f"{title.lower().strip()} {snippet.lower().strip()}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def execute(self, input_data: List[SearchResult], context: Optional[Dict[str, Any]] = None) -> List[SearchResult]:
        """Filter and deduplicate search results"""
        
        self.logger.info(f"Filtering {len(input_data)} search results")
        
        try:
            seen_urls: Set[str] = set()
            seen_content: Set[str] = set()
            filtered_results = []
            
            stats = {
                "invalid_urls": 0,
                "duplicate_urls": 0,
                "duplicate_content": 0,
                "valid_results": 0
            }
            
            for result in input_data:
                # Check URL validity
                if not self._is_valid_url(result.url):
                    stats["invalid_urls"] += 1
                    self.logger.debug(f"Filtered invalid URL: {result.url}")
                    continue
                
                # Normalize URL for deduplication
                normalized_url = self._normalize_url(result.url)
                
                # Check for URL duplicates
                if normalized_url in seen_urls:
                    stats["duplicate_urls"] += 1
                    self.logger.debug(f"Filtered duplicate URL: {result.url}")
                    continue
                
                # Check for content duplicates
                content_hash = self._calculate_content_hash(result.title, result.snippet)
                if content_hash in seen_content:
                    stats["duplicate_content"] += 1
                    self.logger.debug(f"Filtered duplicate content from: {result.url}")
                    continue
                
                # Add to filtered results
                seen_urls.add(normalized_url)
                seen_content.add(content_hash)
                filtered_results.append(result)
                stats["valid_results"] += 1
            
            # Log filtering statistics
            self.logger.info(f"Filtering complete: {len(input_data)} → {len(filtered_results)}")
            self.logger.info(f"  - Invalid URLs: {stats['invalid_urls']}")
            self.logger.info(f"  - Duplicate URLs: {stats['duplicate_urls']}")
            self.logger.info(f"  - Duplicate content: {stats['duplicate_content']}")
            self.logger.info(f"  - Valid results: {stats['valid_results']}")
            
            # Save filtered results and statistics
            filtered_data = {
                "statistics": stats,
                "results": []
            }
            
            for result in filtered_results:
                filtered_data["results"].append({
                    "url": result.url,
                    "title": result.title,
                    "snippet": result.snippet,
                    "relevance_score": result.relevance_score,
                    "source_query": result.source_query
                })
            
            self.save_data(
                filtered_data,
                "filtered_search_results.json",
                settings.DEBUG_PATH
            )
            
            return filtered_results
            
        except Exception as e:
            self.logger.error(f"Filtration failed: {e}")
            raise
