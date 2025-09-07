import requests
import time
import logging
from typing import Dict, Any, Optional, List
from config.settings import settings

class Crawl4AIService:
    """Service for Crawl4AI web scraping integration"""
    
    def __init__(self):
        if not hasattr(settings, 'CRAWL4AI_API_KEY') or not settings.CRAWL4AI_API_KEY:
            # Fallback to a simple requests-based scraper if no Crawl4AI key
            self.use_fallback = True
            self.logger = logging.getLogger(__name__)
            self.logger.warning("No Crawl4AI API key found, using fallback scraper")
        else:
            self.use_fallback = False
            self.api_key = settings.CRAWL4AI_API_KEY
            self.base_url = "https://api.crawl4ai.com/v1"
            self.session = requests.Session()
            self.session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            })
            self.logger = logging.getLogger(__name__)
    
    def scrape_url(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Scrape a single URL and return structured content"""
        
        if self.use_fallback:
            return self._fallback_scrape(url, max_retries)
        
        for attempt in range(max_retries):
            try:
                payload = {
                    "url": url,
                    "extract_text": True,
                    "extract_links": False,
                    "extract_images": False,
                    "wait_for": 3  # Wait 3 seconds for JS to load
                }
                
                response = self.session.post(
                    f"{self.base_url}/scrape",
                    json=payload,
                    timeout=60
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Format the response consistently
                return {
                    "url": url,
                    "title": data.get("title", ""),
                    "content": data.get("text", ""),
                    "word_count": len(data.get("text", "").split()),
                    "success": True,
                    "error": None
                }
                
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Failed to scrape {url} after {max_retries} attempts: {e}")
                    return {
                        "url": url,
                        "title": "",
                        "content": "",
                        "word_count": 0,
                        "success": False,
                        "error": str(e)
                    }
                else:
                    self.logger.warning(f"Scraping attempt {attempt + 1} failed for {url}: {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def _fallback_scrape(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fallback scraper using requests and basic parsing"""
        
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                # Basic text extraction (you might want to use BeautifulSoup here)
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Get text content
                text = soup.get_text()
                
                # Clean up text
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                
                title = soup.find('title')
                title_text = title.get_text().strip() if title else ""
                
                return {
                    "url": url,
                    "title": title_text,
                    "content": text,
                    "word_count": len(text.split()),
                    "success": True,
                    "error": None
                }
                
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Fallback scraping failed for {url}: {e}")
                    return {
                        "url": url,
                        "title": "",
                        "content": "",
                        "word_count": 0,
                        "success": False,
                        "error": str(e)
                    }
                else:
                    time.sleep(2 ** attempt)
        
        return None

# Initialize service
crawl4ai_service = Crawl4AIService()
