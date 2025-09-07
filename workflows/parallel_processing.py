import asyncio
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from models.data_schemas import SearchResult, ContentChunk, ParsedQuery
from agents.web_scraping_agent import WebScrapingAgent
from agents.topic_extraction_agent import TopicExtractionAgent
from agents.synthetic_data_generator_agent import SyntheticDataGeneratorAgent
from agents.data_collection_agent import DataCollectionAgent
from services.chunking_service import chunking_service
from config.settings import settings

class ParallelProcessingOrchestrator:
    """Orchestrates parallel processing across pipeline stages"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.max_workers = settings.PARALLEL_AGENTS
    
    def run_web_scraping(self, filtered_urls: List[SearchResult]) -> List[Dict[str, Any]]:
        """Run web scraping (single agent for now, can be parallelized later)"""
        
        self.logger.info(f"Starting web scraping phase with {len(filtered_urls)} URLs")
        
        scraping_agent = WebScrapingAgent()
        scraped_data = scraping_agent.run(filtered_urls)
        
        # Convert to dict format for chunking service
        scraped_dict_data = []
        for scraped_content in scraped_data:
            if scraped_content.success:
                scraped_dict_data.append({
                    "url": scraped_content.url,
                    "title": scraped_content.title,
                    "content": scraped_content.content,
                    "success": True
                })
        
        return scraped_dict_data
    
    def run_parallel_topic_extraction(self, scraped_data: List[Dict[str, Any]], language: str, parsed_query: ParsedQuery) -> List[str]:
        """Run topic extraction with language support"""
        
        self.logger.info(f"Starting parallel topic extraction with {self.max_workers} agents")

        # Calculate required topics
        required_topics = parsed_query.calculate_required_subtopics()
        self.logger.info(f"Targeting {required_topics} unique topics for extraction.")
        
        # Chunk the content
        all_chunks = chunking_service.chunk_content(scraped_data)
        
        if not all_chunks:
            self.logger.warning("No chunks created from scraped data")
            return []
        
        all_topics = []
        processed_chunk_ids = set()
        
        # Process chunks incrementally until enough topics are gathered
        # Or all chunks are processed
        chunk_index = 0
        
        while len(all_topics) < required_topics and chunk_index < len(all_chunks):
            chunks_to_process_in_batch = []
            chunks_for_agents = [[] for _ in range(self.max_workers)]
            
            # Distribute a batch of chunks for parallel processing
            for i in range(self.max_workers):
                if chunk_index < len(all_chunks):
                    chunks_for_agents[i].append(all_chunks[chunk_index])
                    chunks_to_process_in_batch.append(all_chunks[chunk_index])
                    chunk_index += 1

            if not chunks_to_process_in_batch:
                break # No more chunks to process

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_agent = {}
                for i in range(self.max_workers):
                    if chunks_for_agents[i]:
                        agent = TopicExtractionAgent(i + 1)
                        input_data = {
                            "chunks": chunks_for_agents[i],
                            "language": language,
                            "domain_type": parsed_query.domain_type,
                            "required_topics_count": required_topics
                        }
                        future = executor.submit(agent.run, input_data)
                        future_to_agent[future] = i + 1
            
                for future in as_completed(future_to_agent):
                    agent_index = future_to_agent[future]
                    try:
                        topics = future.result()
                        # Add only new unique topics
                        for topic in topics:
                            topic_lower = topic.lower().strip()
                            if topic_lower not in processed_chunk_ids:
                                processed_chunk_ids.add(topic_lower)
                                all_topics.append(topic)
                        self.logger.info(f"Agent {agent_index} completed. Total unique topics: {len(all_topics)}")
                    except Exception as e:
                        self.logger.error(f"Agent {agent_index} failed: {e}")
            
            if len(all_topics) >= required_topics:
                self.logger.info(f"Enough topics ({len(all_topics)}) extracted. Stopping further extraction.")
                break

        unique_topics = all_topics # Already unique due to the processing above
        
        self.logger.info(f"Parallel topic extraction complete: {len(unique_topics)} unique topics")
        return unique_topics

    
    def run_parallel_synthetic_generation(self, topics: List[str], parsed_query: ParsedQuery) -> List[List[Any]]:
        """Run synthetic data generation with 3 parallel agents"""
        
        self.logger.info(f"Starting parallel synthetic generation for {len(topics)} topics")
        
        # Calculate required topics and validate coverage
        required_topics = parsed_query.calculate_required_subtopics()
        
        # ✅ FIX: Handle case when no topics are available
        if len(topics) == 0:
            self.logger.error("No topics available for synthetic data generation")
            return [[], [], []]  # Return empty lists for 3 agents
        
        if len(topics) < required_topics:
            self.logger.warning(f"Insufficient topics: have {len(topics)}, need {required_topics}")
            # ✅ FIX: Safe multiplication when topics exist
            multiplier = (required_topics // len(topics)) + 1
            topics = topics * multiplier
            topics = topics[:required_topics]
        
        # Distribute topics across agents
        topics_per_agent = len(topics) // self.max_workers
        remainder = len(topics) % self.max_workers
        
        topic_sets = []
        start_idx = 0
        
        for i in range(self.max_workers):
            # Give extra topics to first agents if there's a remainder
            agent_topic_count = topics_per_agent + (1 if i < remainder else 0)
            end_idx = start_idx + agent_topic_count
            topic_sets.append(topics[start_idx:end_idx])
            start_idx = end_idx
        
        # Run generation agents in parallel
        synthetic_data_lists = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit jobs
            future_to_agent = {}
            for i in range(self.max_workers):
                if topic_sets[i]:  # Only create agent if it has topics
                    agent = SyntheticDataGeneratorAgent(i + 1)
                    input_data = {
                        "topics": topic_sets[i],
                        "data_type": parsed_query.data_type,
                        "language": parsed_query.language,
                        "description": parsed_query.description  # Pass description
                    }
                    future = executor.submit(agent.run, input_data)
                    future_to_agent[future] = i + 1
            
            # Collect results
            for future in as_completed(future_to_agent):
                agent_index = future_to_agent[future]
                try:
                    synthetic_data = future.result()
                    synthetic_data_lists.append(synthetic_data)
                    self.logger.info(f"Agent {agent_index} generated {len(synthetic_data)} data points")
                except Exception as e:
                    self.logger.error(f"Generation agent {agent_index} failed: {e}")
                    synthetic_data_lists.append([])  # Add empty list for failed agent
        
        return synthetic_data_lists

    
    def run_data_collection(self, synthetic_data_lists: List[List[Any]], parsed_query: ParsedQuery) -> Dict[str, Any]:
        """Run final data collection and aggregation"""
        
        self.logger.info("Starting data collection phase")
        
        collection_agent = DataCollectionAgent()
        input_data = {
            "synthetic_data_lists": synthetic_data_lists,
            "parsed_query": parsed_query
        }
        
        result = collection_agent.run(input_data)
        return result

# Initialize orchestrator
parallel_orchestrator = ParallelProcessingOrchestrator()
