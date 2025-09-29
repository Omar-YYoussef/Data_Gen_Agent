# import logging
# from typing import Dict, Any, Optional
# import asyncio
# import requests
# from bs4 import BeautifulSoup
# import time

# class WebScrapingService:
#     """Service for web scraping integration"""
    
#     def __init__(self):
#         self.logger = logging.getLogger(__name__)
#         self.logger.info("Using traditional requests/BeautifulSoup scraper.")
        
#     async def ascrape_url(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
#         """Scrape a single URL and return structured content (async)"""
#         return await self._fallback_scrape(url, max_retries)

#     async def _fallback_scrape(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
#         """Scraper using requests and basic parsing"""
        
#         self.logger.debug(f"Scraping {url}")
#         for attempt in range(max_retries):
#             try:
#                 headers = {
#                     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
#                 }
                
#                 # Run requests in executor to avoid blocking
#                 loop = asyncio.get_event_loop()
#                 response = await loop.run_in_executor(
#                     None, 
#                     lambda: requests.get(url, headers=headers, timeout=30)
#                 )
#                 response.raise_for_status()
                
#                 soup = BeautifulSoup(response.content, 'html.parser')
                
#                 # Remove script and style elements
#                 for script in soup(["script", "style", "nav", "footer", "aside"]):
#                     script.decompose()
                
#                 # Extract text content
#                 text = soup.get_text(separator=' ', strip=True)
                
#                 # Clean up whitespace
#                 lines = (line.strip() for line in text.splitlines())
#                 chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
#                 text = ' '.join(chunk for chunk in chunks if chunk)
                
#                 # Extract title
#                 title = soup.find('title')
#                 title_text = title.get_text().strip() if title else ""
                
#                 return {
#                     "url": url,
#                     "title": title_text,
#                     "content": text,
#                     "word_count": len(text.split()),
#                     "success": True,
#                     "error": None,
#                 }
                
#             except Exception as e:
#                 if attempt == max_retries - 1:
#                     self.logger.error(f"Scraping failed for {url} after {max_retries} attempts: {e}")
#                     return {
#                         "url": url,
#                         "title": "",
#                         "content": "",
#                         "word_count": 0,
#                         "success": False,
#                         "error": str(e),
#                     }
#                 else:
#                     self.logger.warning(f"Scraping attempt {attempt + 1} failed for {url}: {e}")
#                     await asyncio.sleep(2 ** attempt)
        
#         return None

#     async def scrape_multiple_urls(self, urls: list, max_concurrent: int = 5) -> list:
#         """Scrape multiple URLs concurrently"""
#         semaphore = asyncio.Semaphore(max_concurrent)
        
#         async def scrape_with_semaphore(url):
#             async with semaphore:
#                 return await self.ascrape_url(url)
        
#         tasks = [scrape_with_semaphore(url) for url in urls]
#         results = await asyncio.gather(*tasks, return_exceptions=True)
        
#         # Filter out exceptions and return successful results
#         return [result for result in results if not isinstance(result, Exception) and result is not None]

# # Initialize service
# scraping_service = WebScrapingService()

import logging
from typing import Dict, Any, Optional
import asyncio
import os
from scraperapi_sdk import ScraperAPIClient
import time
import json
from ..config.settings import settings

class WebScrapingService:
    """Service for web scraping integration using ScraperAPI"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_key =settings.SCRAPERAPI_API_KEY
        if not self.api_key:
            raise ValueError("SCRAPERAPI_API_KEY environment variable is required")
        
        self.client = ScraperAPIClient(self.api_key)
        self.logger.info("Using ScraperAPI with autoparse feature.")
        
    async def ascrape_url(self, url: str, max_retries: int = 3, use_autoparse: bool = False) -> Optional[Dict[str, Any]]:
        """Scrape a single URL and return structured content (async)"""
        return await self._scraperapi_scrape(url, max_retries, use_autoparse)

    async def _scraperapi_scrape(self, url: str, max_retries: int = 3, use_autoparse: bool = False) -> Optional[Dict[str, Any]]:
        """Scraper using ScraperAPI with optional autoparse"""
        
        self.logger.debug(f"Scraping {url} with ScraperAPI")
        
        for attempt in range(max_retries):
            try:
                # Prepare parameters for ScraperAPI
                params = {}
                if use_autoparse:
                    params['autoparse'] = True
                    params['format'] = 'json'
                
                # Run ScraperAPI request in executor to avoid blocking
                loop = asyncio.get_event_loop()
                
                if use_autoparse:
                    # Use autoparse for structured data extraction
                    response = await loop.run_in_executor(
                        None,
                        lambda: self.client.get(url, params=params)
                    )
                    
                    # Parse JSON response from autoparse
                    try:
                        parsed_data = json.loads(response)
                        
                        # Extract relevant fields for consistent return format
                        title = parsed_data.get('name', '') or parsed_data.get('title', '')
                        content = self._extract_content_from_autoparse(parsed_data)
                        
                        return {
                            "url": url,
                            "title": title,
                            "content": content,
                            "word_count": len(content.split()) if content else 0,
                            "success": True,
                            "error": None,
                            "structured_data": parsed_data  # Include full structured data
                        }
                    except json.JSONDecodeError:
                        # If autoparse fails, fall back to regular scraping
                        self.logger.warning(f"Autoparse failed for {url}, falling back to regular scraping")
                        use_autoparse = False
                
                if not use_autoparse:
                    # Regular HTML scraping
                    html_content = await loop.run_in_executor(
                        None,
                        lambda: self.client.get(url)
                    )
                    
                    # Basic text extraction from HTML
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Remove script and style elements
                    for script in soup(["script", "style", "nav", "footer", "aside"]):
                        script.decompose()
                    
                    # Extract text content
                    text = soup.get_text(separator=' ', strip=True)
                    
                    # Clean up whitespace
                    lines = (line.strip() for line in text.splitlines())
                    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                    text = ' '.join(chunk for chunk in chunks if chunk)
                    
                    # Extract title
                    title = soup.find('title')
                    title_text = title.get_text().strip() if title else ""
                    
                    return {
                        "url": url,
                        "title": title_text,
                        "content": text,
                        "word_count": len(text.split()),
                        "success": True,
                        "error": None,
                    }
                
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"ScraperAPI scraping failed for {url} after {max_retries} attempts: {e}")
                    return {
                        "url": url,
                        "title": "",
                        "content": "",
                        "word_count": 0,
                        "success": False,
                        "error": str(e),
                    }
                else:
                    self.logger.warning(f"ScraperAPI scraping attempt {attempt + 1} failed for {url}: {e}")
                    await asyncio.sleep(2 ** attempt)
        
        return None

    def _extract_content_from_autoparse(self, parsed_data: Dict[str, Any]) -> str:
        """Extract meaningful text content from autoparse structured data"""
        content_parts = []
        
        # Common fields that contain textual content
        text_fields = [
            'name', 'title', 'description', 'full_description', 
            'small_description', 'content', 'text', 'summary'
        ]
        
        for field in text_fields:
            if field in parsed_data and parsed_data[field]:
                content_parts.append(str(parsed_data[field]))
        
        # Handle feature bullets or similar list fields
        if 'feature_bullets' in parsed_data and isinstance(parsed_data['feature_bullets'], list):
            content_parts.extend(parsed_data['feature_bullets'])
        
        # Handle product information or similar nested fields
        if 'product_information' in parsed_data and isinstance(parsed_data['product_information'], dict):
            for key, value in parsed_data['product_information'].items():
                if isinstance(value, str):
                    content_parts.append(f"{key}: {value}")
        
        return ' '.join(content_parts)

    async def scrape_multiple_urls(self, urls: list, max_concurrent: int = 5, use_autoparse: bool = False) -> list:
        """Scrape multiple URLs concurrently using ScraperAPI"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def scrape_with_semaphore(url):
            async with semaphore:
                return await self.ascrape_url(url, use_autoparse=use_autoparse)
        
        tasks = [scrape_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and return successful results
        return [result for result in results if not isinstance(result, Exception) and result is not None]

    async def scrape_with_premium(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Scrape using ScraperAPI premium features for difficult sites"""
        
        self.logger.debug(f"Scraping {url} with ScraperAPI premium features")
        
        for attempt in range(max_retries):
            try:
                loop = asyncio.get_event_loop()
                
                # Use premium features for difficult sites
                premium_content = await loop.run_in_executor(
                    None,
                    lambda: self.client.get(url, params={'premium': True, 'render': True})
                )
                
                # Basic text extraction from HTML
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(premium_content, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style", "nav", "footer", "aside"]):
                    script.decompose()
                
                # Extract text content
                text = soup.get_text(separator=' ', strip=True)
                
                # Clean up whitespace
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                
                # Extract title
                title = soup.find('title')
                title_text = title.get_text().strip() if title else ""
                
                return {
                    "url": url,
                    "title": title_text,
                    "content": text,
                    "word_count": len(text.split()),
                    "success": True,
                    "error": None,
                    "premium": True
                }
                
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Premium scraping failed for {url} after {max_retries} attempts: {e}")
                    return {
                        "url": url,
                        "title": "",
                        "content": "",
                        "word_count": 0,
                        "success": False,
                        "error": str(e),
                    }
                else:
                    self.logger.warning(f"Premium scraping attempt {attempt + 1} failed for {url}: {e}")
                    await asyncio.sleep(2 ** attempt)
        
        return None


# Initialize service
scraping_service = WebScrapingService()
