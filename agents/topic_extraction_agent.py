from typing import Dict, Any, Optional, List
from models.data_schemas import ContentChunk, ExtractedTopic
from agents.base_agent import BaseAgent
from services.gemini_service import gemini_service
from config.settings import settings
import asyncio
import time

class TopicExtractionAgent(BaseAgent):
    """Agent to extract topics from content chunks (runs 3 in parallel)"""
    
    def __init__(self, agent_index: int, config: Optional[Dict[str, Any]] = None):
        super().__init__(f"topic_extraction_{agent_index}", config)
        self.agent_index = agent_index
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate that input is a dictionary containing 'chunks' (list of ContentChunk), 'language' (str), 'domain_type' (str), and 'required_topics_count' (int)"""
        self.logger.debug(f"Validating input for TopicExtractionAgent: {input_data}")
        if not isinstance(input_data, dict):
            self.logger.error(f"Validation failed: input_data is not a dictionary. Type: {type(input_data)}")
            return False
        if "chunks" not in input_data or not isinstance(input_data["chunks"], list):
            self.logger.error(f"Validation failed: 'chunks' missing or not a list. Value: {input_data.get('chunks')}")
            return False
        if not all(isinstance(item, ContentChunk) for item in input_data["chunks"]):
            self.logger.error(f"Validation failed: Not all items in 'chunks' are ContentChunk objects. Sample: {input_data['chunks'][:5]}")
            return False
        if "language" not in input_data or not isinstance(input_data["language"], str):
            self.logger.error(f"Validation failed: 'language' missing or not a string. Value: {input_data.get('language')}")
            return False
        if "domain_type" not in input_data or not isinstance(input_data["domain_type"], str):
            self.logger.error(f"Validation failed: 'domain_type' missing or not a string. Value: {input_data.get('domain_type')}")
            return False
        if "required_topics_count" not in input_data or not isinstance(input_data["required_topics_count"], int):
            self.logger.error(f"Validation failed: 'required_topics_count' missing or not an int. Value: {input_data.get('required_topics_count')}")
            return False
        self.logger.debug("Input validation successful for TopicExtractionAgent.")
        return True
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate that output is a list of topic strings"""
        return (isinstance(output_data, list) and 
                all(isinstance(item, str) for item in output_data))
    
    def execute(self, input_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> List[str]:
        """Extract topics using user's specified language"""
        
        chunks = input_data["chunks"]
        language = input_data["language"]
        domain_type = input_data["domain_type"]  # Extract domain_type
        required_topics_count = input_data["required_topics_count"]
        
        self.logger.info(f"Agent {self.agent_index} processing {len(chunks)} chunks in {language} for domain {domain_type}")
        
        all_topics = []
        processed_chunks = 0
        
        try:
            for chunk in chunks:
                # Stop if enough unique topics have been collected
                if len(set(all_topics)) >= required_topics_count:
                    self.logger.info(f"Agent {self.agent_index} stopping early: Reached {len(set(all_topics))} unique topics (Target: {required_topics_count})")
                    break

                self.logger.debug(f"Processing chunk {chunk.chunk_id} from {chunk.source_url}")
                
                # Pass language and domain_type to Gemini service
                chunk_topics = gemini_service.extract_topics(chunk.content, language, domain_type)
                
                if chunk_topics:
                    all_topics.extend(chunk_topics)
                    self.logger.debug(f"Extracted {len(chunk_topics)} topics from chunk {chunk.chunk_id}")
                
                processed_chunks += 1
                
                # Small delay to respect API limits
                if processed_chunks % 10 == 0:
                    time.sleep(1)
            
            # Remove duplicates while preserving order
            unique_topics = []
            seen = set()
            for topic in all_topics:
                topic_lower = topic.lower().strip()
                if topic_lower not in seen:
                    seen.add(topic_lower)
                    unique_topics.append(topic)
            
            self.logger.info(f"Agent {self.agent_index} extracted {len(unique_topics)} unique topics from {processed_chunks} chunks")
            
            # Save extracted topics for this agent
            topics_data = {
                "agent_index": self.agent_index,
                "processed_chunks": processed_chunks,
                "total_topics_found": len(all_topics),
                "unique_topics_count": len(unique_topics),
                "topics": unique_topics
            }
            
            self.save_data(
                topics_data,
                f"topics_agent_{self.agent_index}.json",
                settings.EXTRACTED_TOPICS_PATH
            )
            
            return unique_topics
            
        except Exception as e:
            self.logger.error(f"Topic extraction failed for agent {self.agent_index}: {e}")
            raise

