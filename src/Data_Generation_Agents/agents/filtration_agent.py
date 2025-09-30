# from typing import Dict, Any, Optional, List, Set
# from models.data_schemas import SearchResult
# from agents.base_agent import BaseAgent
# from config.settings import settings
# from urllib.parse import urlparse
# import hashlib

# class FiltrationAgent(BaseAgent):
#     """Agent to filter and deduplicate search results"""
    
#     def __init__(self, config: Optional[Dict[str, Any]] = None):
#         super().__init__("filtration", config)
    
#     def validate_input(self, input_data: Any) -> bool:
#         """Validate that input is a list of SearchResult objects"""
#         return (isinstance(input_data, list) and 
#                 all(isinstance(item, SearchResult) for item in input_data))
    
#     def validate_output(self, output_data: Any) -> bool:
#         """Validate that output is a list of SearchResult objects"""
#         return (isinstance(output_data, list) and 
#                 all(isinstance(item, SearchResult) for item in output_data))
    
#     def _normalize_url(self, url: str) -> str:
#         """Normalize URL for comparison"""
#         try:
#             parsed = urlparse(url.lower().strip())
#             # Remove common URL variations
#             normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
#             if normalized.endswith('/'):
#                 normalized = normalized[:-1]
#             return normalized
#         except Exception as e:
#             self.logger.debug(f"Failed to normalize URL {url}: {e}")
#             return url.lower().strip()
    
#     def _is_valid_url(self, url: str) -> bool:
#         """Check if URL is valid for scraping"""
#         try:
#             parsed = urlparse(url)
            
#             # Must have scheme and netloc
#             if not parsed.scheme or not parsed.netloc:
#                 return False
            
#             # Filter out unwanted file extensions
#             excluded_extensions = {
#                 '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
#                 '.zip', '.rar', '.tar', '.gz', '.mp4', '.avi', '.mp3',
#                 '.jpg', '.jpeg', '.png', '.gif', '.bmp'
#             }
#             if any(url.lower().endswith(ext) for ext in excluded_extensions):
#                 return False
            
#             # Filter out problematic domains
#             excluded_domains = {
#                 'twitter.com', 'facebook.com', 'instagram.com', 'linkedin.com',
#                 'youtube.com', 'tiktok.com', 'reddit.com'
#             }
#             if any(domain in parsed.netloc.lower() for domain in excluded_domains):
#                 return False
            
#             # Filter out URLs that are too short or suspicious
#             if len(url) < 10:
#                 return False
            
#             return True
            
#         except Exception as e:
#             self.logger.debug(f"URL validation failed for {url}: {e}")
#             return False
    
#     def _calculate_content_hash(self, title: str, snippet: str) -> str:
#         """Calculate hash for content deduplication"""
#         content = f"{title.lower().strip()} {snippet.lower().strip()}"
#         return hashlib.md5(content.encode('utf-8')).hexdigest()
    
#     def execute(self, input_data: List[SearchResult], context: Optional[Dict[str, Any]] = None) -> List[SearchResult]:
#         """Filter and deduplicate search results"""
        
#         self.logger.info(f"Filtering {len(input_data)} search results")
        
#         try:
#             seen_urls: Set[str] = set()
#             seen_content: Set[str] = set()
#             filtered_results = []
            
#             stats = {
#                 "invalid_urls": 0,
#                 "duplicate_urls": 0,
#                 "duplicate_content": 0,
#                 "valid_results": 0
#             }
            
#             for result in input_data:
#                 # Check URL validity
#                 if not self._is_valid_url(result.url):
#                     stats["invalid_urls"] += 1
#                     self.logger.debug(f"Filtered invalid URL: {result.url}")
#                     continue
                
#                 # Normalize URL for deduplication
#                 normalized_url = self._normalize_url(result.url)
                
#                 # Check for URL duplicates
#                 if normalized_url in seen_urls:
#                     stats["duplicate_urls"] += 1
#                     self.logger.debug(f"Filtered duplicate URL: {result.url}")
#                     continue
                
#                 # Check for content duplicates
#                 content_hash = self._calculate_content_hash(result.title, result.snippet)
#                 if content_hash in seen_content:
#                     stats["duplicate_content"] += 1
#                     self.logger.debug(f"Filtered duplicate content from: {result.url}")
#                     continue
                
#                 # Add to filtered results
#                 seen_urls.add(normalized_url)
#                 seen_content.add(content_hash)
#                 filtered_results.append(result)
#                 stats["valid_results"] += 1
            
#             # Log filtering statistics
#             self.logger.info(f"Filtering complete: {len(input_data)} → {len(filtered_results)}")
#             self.logger.info(f"  - Invalid URLs: {stats['invalid_urls']}")
#             self.logger.info(f"  - Duplicate URLs: {stats['duplicate_urls']}")
#             self.logger.info(f"  - Duplicate content: {stats['duplicate_content']}")
#             self.logger.info(f"  - Valid results: {stats['valid_results']}")
            
#             return filtered_results
            
#         except Exception as e:
#             self.logger.error(f"Filtration failed: {e}")
#             raise

from typing import Dict, Any, Optional, List, Set
from ..models.data_schemas import SearchResult
from ..agents.base_agent import BaseAgent
from ..config.settings import settings
from urllib.parse import urlparse
import hashlib
import requests
import asyncio


class FiltrationAgent(BaseAgent):
    """Agent to filter and deduplicate search results with ScraperAPI optimization"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("filtration", config)
        
        # ScraperAPI autoparse supported domains
        self.autoparse_domains = {
            'amazon.com', 'amazon.co.uk', 'amazon.ca', 'amazon.de', 'amazon.fr', 'amazon.es',
            'amazon.it', 'amazon.co.jp', 'amazon.in', 'amazon.com.br', 'amazon.com.mx',
            'walmart.com', 'ebay.com', 'redfin.com', 'google.com'
        }
        
        # Domains that work well with ScraperAPI premium features
        self.premium_domains = {
            'indeed.com', 'glassdoor.com', 'zillow.com', 'booking.com', 'airbnb.com',
            'tripadvisor.com', 'expedia.com', 'hotels.com', 'craigslist.org'
        }
        
        # Binary file magic numbers (file signatures)
        self.binary_signatures = {
            # PDF files
            b'%PDF-': 'application/pdf',
            # Microsoft Office documents
            b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1': 'application/msword',  # DOC, XLS, PPT
            b'PK\x03\x04': 'application/zip',  # ZIP, DOCX, XLSX, PPTX
            # Images
            b'\xFF\xD8\xFF': 'image/jpeg',
            b'\x89PNG\r\n\x1A\n': 'image/png',
            b'GIF87a': 'image/gif',
            b'GIF89a': 'image/gif',
            b'RIFF': 'image/webp',  # Also WAV, but we check for WEBP
            b'BM': 'image/bmp',
            # Video/Audio
            b'\x00\x00\x00\x20ftypmp41': 'video/mp4',
            b'\x00\x00\x00\x1CftypM4A': 'audio/mp4',
            b'ID3': 'audio/mpeg',  # MP3
            b'\xFF\xFB': 'audio/mpeg',  # MP3
            # Archives
            b'Rar!\x1A\x07\x00': 'application/x-rar-compressed',
            b'\x1F\x8B\x08': 'application/gzip',
            b'7z\xBC\xAF\x27\x1C': 'application/x-7z-compressed',
            # Executables
            b'MZ': 'application/x-msdownload',  # EXE
            b'\x7FELF': 'application/x-executable',  # Linux executable
        }
    
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
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if normalized.endswith('/'):
                normalized = normalized[:-1]
            return normalized
        except Exception as e:
            self.logger.debug(f"Failed to normalize URL {url}: {e}")
            return url.lower().strip()
    
    async def _check_content_type_async(self, url: str) -> Optional[str]:
        """Asynchronously check the actual content type of a URL"""
        try:
            # Use a HEAD request first to check headers without downloading content
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.head(url, timeout=10, allow_redirects=True)
            )
            
            content_type = response.headers.get('content-type', '').lower()
            
            # If we got a clear content type from headers, use it
            if any(binary_type in content_type for binary_type in [
                'application/pdf', 'application/msword', 'application/vnd.ms-',
                'application/vnd.openxmlformats', 'image/', 'video/', 'audio/',
                'application/zip', 'application/x-rar', 'application/octet-stream'
            ]):
                self.logger.debug(f"Binary content detected via headers: {url} -> {content_type}")
                return content_type
            
            # If headers are inconclusive, check the first few bytes
            if content_type.startswith('text/html') or not content_type:
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(url, stream=True, timeout=10, headers={
                        'Range': 'bytes=0-1023'  # Only get first 1KB
                    })
                )
                
                if response.status_code in [200, 206]:  # 206 = Partial Content
                    first_bytes = response.content[:50]  # Check first 50 bytes
                    
                    # Check for binary file signatures
                    for signature, mime_type in self.binary_signatures.items():
                        if first_bytes.startswith(signature):
                            self.logger.debug(f"Binary content detected via signature: {url} -> {mime_type}")
                            return mime_type
                    
                    # Check for null bytes (common in binary files)
                    if b'\x00' in first_bytes:
                        self.logger.debug(f"Binary content detected via null bytes: {url}")
                        return 'application/octet-stream'
                    
                    # Check for high ratio of non-printable characters
                    printable_chars = sum(1 for byte in first_bytes if 32 <= byte <= 126)
                    if len(first_bytes) > 0 and printable_chars / len(first_bytes) < 0.7:
                        self.logger.debug(f"Binary content detected via character analysis: {url}")
                        return 'application/octet-stream'
            
            return content_type
            
        except Exception as e:
            self.logger.debug(f"Content type check failed for {url}: {e}")
            return None
    
    def _is_binary_by_extension(self, url: str) -> bool:
        """Check if URL appears to be binary based on extension"""
        excluded_extensions = {
            '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
            '.zip', '.rar', '.tar', '.gz', '.7z', '.exe', '.dmg',
            '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv',
            '.mp3', '.wav', '.flac', '.aac', '.ogg',
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
            '.css', '.js', '.json', '.xml', '.rss'
        }
        return any(url.lower().endswith(ext) for ext in excluded_extensions)
    
    def _is_suspicious_binary_url(self, url: str) -> bool:
        """Check if URL patterns suggest it might be binary"""
        suspicious_patterns = [
            '/download/', '/files/', '/documents/', '/media/', '/assets/',
            '/uploads/', '/attachments/', '/content/', '/pdf/', '/doc/',
            '.cgi?', '.php?', '.asp?', '.jsp?', 'download=', 'file=',
            'document=', 'attachment=', 'export=', 'report='
        ]
        return any(pattern in url.lower() for pattern in suspicious_patterns)
    
    async def _is_scraperapi_compatible(self, url: str) -> bool:
        """Check if URL is compatible with ScraperAPI (async version)"""
        try:
            parsed = urlparse(url)
            
            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Check for login-required pages
            login_indicators = [
                '/login', '/signin', '/signup', '/register', '/auth',
                'login.', 'signin.', 'auth.', 'account.'
            ]
            if any(indicator in url.lower() for indicator in login_indicators):
                self.logger.debug(f"Filtered login page: {url}")
                return False
            
            # Quick extension-based check
            if self._is_binary_by_extension(url):
                self.logger.debug(f"Filtered binary file by extension: {url}")
                return False
            
            # Check suspicious binary URL patterns
            if self._is_suspicious_binary_url(url):
                # For suspicious URLs, check actual content type
                content_type = await self._check_content_type_async(url)
                if content_type and any(binary_type in content_type for binary_type in [
                    'application/pdf', 'application/msword', 'application/vnd.ms-',
                    'application/vnd.openxmlformats', 'image/', 'video/', 'audio/',
                    'application/zip', 'application/x-rar', 'application/octet-stream'
                ]):
                    self.logger.debug(f"Filtered binary file by content inspection: {url}")
                    return False
            
            # Social media restrictions
            restricted_domains = {
                'twitter.com', 'x.com', 'facebook.com', 'instagram.com', 
                'tiktok.com', 'snapchat.com', 'pinterest.com'
            }
            domain = parsed.netloc.lower().replace('www.', '')
            if any(restricted in domain for restricted in restricted_domains):
                self.logger.debug(f"Filtered restricted social media: {url}")
                return False
            
            # URL length and localhost checks
            if len(url) < 10:
                return False
            
            if any(local in parsed.netloc.lower() for local in ['localhost', '127.0.0.1', '0.0.0.0']):
                return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"URL validation failed for {url}: {e}")
            return False
    
    def _categorize_url_for_scraping(self, url: str) -> str:
        """Categorize URL for optimal ScraperAPI usage"""
        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc.replace('www.', '')
            
            # Check for autoparse supported domains
            for autoparse_domain in self.autoparse_domains:
                if autoparse_domain in domain:
                    return "autoparse"
            
            # Check for premium feature domains
            for premium_domain in self.premium_domains:
                if premium_domain in domain:
                    return "premium"
            
            return "regular"
            
        except Exception:
            return "regular"
    
    def _calculate_content_hash(self, title: str, snippet: str) -> str:
        """Calculate hash for content deduplication"""
        content = f"{title.lower().strip()} {snippet.lower().strip()}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _priority_score(self, result: SearchResult) -> int:
        """Calculate priority score for ranking results"""
        score = 0
        url_category = self._categorize_url_for_scraping(result.url)
        
        if url_category == "autoparse":
            score += 100
        
        reliable_domains = {
            'wikipedia.org', 'stackoverflow.com', 'github.com', 'medium.com',
            'reuters.com', 'bbc.com', 'cnn.com', 'techcrunch.com'
        }
        parsed = urlparse(result.url.lower())
        domain = parsed.netloc.replace('www.', '')
        if any(reliable in domain for reliable in reliable_domains):
            score += 50
        
        if result.url.startswith('https://'):
            score += 10
        
        if result.title and len(result.title) > 10:
            score += 5
        if result.snippet and len(result.snippet) > 50:
            score += 5
        
        return score
    
    async def execute(self, input_data: List[SearchResult], context: Optional[Dict[str, Any]] = None) -> List[SearchResult]:
        """Filter and deduplicate search results optimized for ScraperAPI (async)"""
        
        self.logger.info(f"Filtering {len(input_data)} search results for ScraperAPI compatibility")
        
        try:
            seen_urls: Set[str] = set()
            seen_content: Set[str] = set()
            filtered_results = []
            
            stats = {
                "incompatible_urls": 0,
                "binary_files": 0,
                "duplicate_urls": 0,
                "duplicate_content": 0,
                "autoparse_urls": 0,
                "premium_urls": 0,
                "regular_urls": 0,
                "valid_results": 0
            }
            
            # Sort by priority score
            sorted_results = sorted(input_data, key=self._priority_score, reverse=True)
            
            # Create semaphore to limit concurrent content checks
            semaphore = asyncio.Semaphore(10)  # Max 10 concurrent checks
            
            async def check_url_compatibility(result):
                async with semaphore:
                    return await self._is_scraperapi_compatible(result.url), result
            
            # Check compatibility for all URLs concurrently
            tasks = [check_url_compatibility(result) for result in sorted_results]
            compatibility_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for compat_result in compatibility_results:
                if isinstance(compat_result, Exception):
                    continue
                    
                is_compatible, result = compat_result
                
                if not is_compatible:
                    stats["incompatible_urls"] += 1
                    continue
                
                # Normalize URL for deduplication
                normalized_url = self._normalize_url(result.url)
                
                # Check for URL duplicates
                if normalized_url in seen_urls:
                    stats["duplicate_urls"] += 1
                    continue
                
                # Check for content duplicates
                content_hash = self._calculate_content_hash(result.title, result.snippet)
                if content_hash in seen_content:
                    stats["duplicate_content"] += 1
                    continue
                
                # Categorize URL for scraping strategy
                url_category = self._categorize_url_for_scraping(result.url)
                stats[f"{url_category}_urls"] += 1
                
                # Add category metadata
                if hasattr(result, 'metadata'):
                    result.metadata = result.metadata or {}
                    result.metadata['scraping_category'] = url_category
                
                # Add to filtered results
                seen_urls.add(normalized_url)
                seen_content.add(content_hash)
                filtered_results.append(result)
                stats["valid_results"] += 1
            
            # Log filtering statistics
            self.logger.info(f"Filtering complete: {len(input_data)} → {len(filtered_results)}")
            self.logger.info(f"  - Incompatible URLs: {stats['incompatible_urls']}")
            self.logger.info(f"  - Binary files detected: {stats['binary_files']}")
            self.logger.info(f"  - Duplicate URLs: {stats['duplicate_urls']}")
            self.logger.info(f"  - Duplicate content: {stats['duplicate_content']}")
            self.logger.info(f"  - Autoparse URLs: {stats['autoparse_urls']}")
            self.logger.info(f"  - Premium URLs: {stats['premium_urls']}")
            self.logger.info(f"  - Regular URLs: {stats['regular_urls']}")
            self.logger.info(f"  - Valid results: {stats['valid_results']}")
            
            return filtered_results
            
        except Exception as e:
            self.logger.error(f"Filtration failed: {e}")
            raise

