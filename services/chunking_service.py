import math
from typing import List, Dict, Any
from models.data_schemas import ContentChunk
from config.settings import settings
import logging

class ChunkingService:
    """Service to chunk content for parallel processing"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Gemini context window is ~8192 tokens, leave some room for prompts
        self.max_tokens_per_chunk = settings.GEMINI_MAX_TOKENS - 1000
        # Approximate tokens per character (rough estimate)
        self.chars_per_token = 4
        self.max_chars_per_chunk = self.max_tokens_per_chunk * self.chars_per_token
    
    def chunk_content(self, scraped_data: List[Dict[str, Any]]) -> List[ContentChunk]:
        """Split scraped content into manageable chunks"""
        
        chunks = []
        chunk_id_counter = 0
        
        for data in scraped_data:
            if not data.get("success", False) or not data.get("content"):
                continue
            
            content = data["content"]
            url = data["url"]
            
            if len(content) <= self.max_chars_per_chunk:
                # Content fits in one chunk
                chunk = ContentChunk(
                    chunk_id=f"chunk_{chunk_id_counter:04d}",
                    content=content,
                    source_url=url,
                    chunk_index=0,
                    total_chunks=1,
                    token_count=len(content) // self.chars_per_token
                )
                chunks.append(chunk)
                chunk_id_counter += 1
            else:
                # Split content into multiple chunks
                total_chunks = math.ceil(len(content) / self.max_chars_per_chunk)
                
                for i in range(total_chunks):
                    start_pos = i * self.max_chars_per_chunk
                    end_pos = min((i + 1) * self.max_chars_per_chunk, len(content))
                    chunk_content = content[start_pos:end_pos]
                    
                    chunk = ContentChunk(
                        chunk_id=f"chunk_{chunk_id_counter:04d}",
                        content=chunk_content,
                        source_url=url,
                        chunk_index=i,
                        total_chunks=total_chunks,
                        token_count=len(chunk_content) // self.chars_per_token
                    )
                    chunks.append(chunk)
                    chunk_id_counter += 1
        
        self.logger.info(f"Created {len(chunks)} chunks from {len(scraped_data)} scraped pages")
        return chunks
    
    def distribute_chunks(self, chunks: List[ContentChunk], num_agents: int = 3) -> List[List[ContentChunk]]:
        """Distribute chunks evenly across parallel agents"""
        
        chunk_sets = [[] for _ in range(num_agents)]
        
        # Round-robin distribution
        for i, chunk in enumerate(chunks):
            agent_index = i % num_agents
            chunk_sets[agent_index].append(chunk)
        
        self.logger.info(f"Distributed {len(chunks)} chunks across {num_agents} agents:")
        for i, chunk_set in enumerate(chunk_sets):
            self.logger.info(f"  Agent {i + 1}: {len(chunk_set)} chunks")
        
        return chunk_sets

# Initialize service
chunking_service = ChunkingService()
