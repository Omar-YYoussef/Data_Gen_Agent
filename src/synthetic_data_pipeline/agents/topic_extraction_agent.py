from typing import Dict, Any, Optional, List
from ..models.data_schemas import ContentChunk, ExtractedTopic
from ..agents.base_agent import BaseAgent
from ..services.gemini_service import GeminiService
from ..config.settings import settings
import asyncio
import time

class TopicExtractionAgent(BaseAgent):
    """Agent to extract topics from content chunks"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("topic_extraction", config)
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate that input is a dictionary containing 'chunks' (list of ContentChunk), 'language' (str), 'domain_type' (str), and 'required_topics_count' (int)"""
        if not isinstance(input_data, dict):
            return False
        if "chunks" not in input_data or not isinstance(input_data["chunks"], list):
            return False
        if not all(isinstance(item, ContentChunk) for item in input_data["chunks"]):
            return False
        if "language" not in input_data or not isinstance(input_data["language"], str):
            return False
        if "domain_type" not in input_data or not isinstance(input_data["domain_type"], str):
            return False
        if "required_topics_count" not in input_data or not isinstance(input_data["required_topics_count"], int):
            return False
        return True
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate that output is a list of topic strings"""
        return (isinstance(output_data, list) and 
                all(isinstance(item, str) for item in output_data))
    
    async def execute(self, input_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> List[str]:
        """Extract topics using user's specified language"""
        
        chunks = input_data["chunks"]
        language = input_data["language"]
        domain_type = input_data["domain_type"]
        required_topics_count = input_data["required_topics_count"]

        gemini_model_name = context.get("gemini_model_name")
        gemini_service = GeminiService(model_name=gemini_model_name if gemini_model_name else settings.GEMINI_DEFAULT_MODEL)
        
        self.logger.info(f"Processing {len(chunks)} chunks in {language} for domain {domain_type}")
        
        all_topics = []
        processed_chunks = 0
        
        # Use a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(settings.TOPIC_EXTRACTION_CONCURRENCY)
        
        async def process_chunk(chunk):
            nonlocal all_topics
            nonlocal required_topics_count
            async with semaphore:
                if len(set(all_topics)) >= required_topics_count:
                    return

                self.logger.debug(f"Processing chunk {chunk.chunk_id} from {chunk.source_url}")
                
                try:
                    await asyncio.sleep(3) # Add a 1-second delay
                    chunk_topics = await gemini_service.extract_topics_async(chunk.content, language, domain_type)
                    if chunk_topics:
                        all_topics.extend(chunk_topics)
                        self.logger.debug(f"Extracted {len(chunk_topics)} topics from chunk {chunk.chunk_id}")
                except Exception as e:
                    self.logger.error(f"Error processing chunk {chunk.chunk_id}: {e}")

        tasks = [process_chunk(chunk) for chunk in chunks]
        await asyncio.gather(*tasks)
        
        processed_chunks = len(chunks)

        # Remove duplicates while preserving order
        unique_topics = []
        seen = set()
        for topic in all_topics:
            topic_lower = topic.lower().strip()
            if topic_lower not in seen:
                seen.add(topic_lower)
                unique_topics.append(topic)
        
        self.logger.info(f"Extracted {len(unique_topics)} unique topics from {processed_chunks} chunks")
        
        # Remove direct saving of extracted topics
        # topics_data = {
        #     "processed_chunks": processed_chunks,
        #     "total_topics_found": len(all_topics),
        #     "unique_topics_count": len(unique_topics),
        #     "topics": unique_topics
        # }
        
        # self.save_data(
        #     topics_data,
        #     "extracted_topics_summary.json",
        #     settings.EXTRACTED_TOPICS_PATH
        # )
        
        return unique_topics

