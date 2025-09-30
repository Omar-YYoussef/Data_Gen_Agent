from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal
from datetime import datetime
import math

# Enhanced Query Parser Models with user-specified data_type
class ParsedQuery(BaseModel):
    original_query: str
    domain_type: str          # Flexible user-defined domain  
    data_type: str           # User-specified data type
    sample_count: int
    language: str            # User-specified language
    required_topics: int = Field(default=0) # Add this line
    iso_language: Optional[str] = None # New: ISO 639-1 language code
    description: Optional[str] = None # New: User-provided description or example data
    categories: Optional[List[str]] = None # New: User-defined categories within the domain
    additional_requirements: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    
    def calculate_required_subtopics(self, rows_per_subtopic: int = 5) -> int:
        return math.ceil(self.sample_count / rows_per_subtopic)

# Search Models
class SearchQuery(BaseModel):
    query: str
    domain: str
    priority: int = Field(ge=1, le=50)  # Updated for 50 queries

class SearchResult(BaseModel):
    url: str
    title: str
    snippet: str
    relevance_score: Optional[float] = None
    source_query: str

# Content Models
class ScrapedContent(BaseModel):
    url: str
    title: str
    content: str
    metadata: Dict[str, Any]
    scraping_timestamp: datetime
    content_length: int
    success: bool

class ContentChunk(BaseModel):
    chunk_id: str
    content: str
    source_url: str
    chunk_index: int
    total_chunks: int
    token_count: int

# Simplified Topic Model - Only topic names
class ExtractedTopic(BaseModel):
    topic_name: str

# Enhanced Synthetic Data Models with flexible structure
class SyntheticDataPoint(BaseModel):
    data_type: str           # The user-specified data type
    content: Dict[str, Any]  # Flexible structure decided by agent
    source_topics: List[str]
    generation_timestamp: datetime = Field(default_factory=datetime.now)

# Agent Communication Models
class AgentState(BaseModel):
    agent_name: str
    status: Literal["pending", "running", "completed", "failed"]
    data: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)

# Workflow State Model
class WorkflowState(BaseModel):
    workflow_id: str
    current_stage: str
    parsed_query: Optional[ParsedQuery] = None
    refined_queries: List[SearchQuery] = Field(default_factory=list)
    search_results: List[SearchResult] = Field(default_factory=list)
    scraped_content: List[ScrapedContent] = Field(default_factory=list)
    extracted_topics: List[ExtractedTopic] = Field(default_factory=list)
    synthetic_data: List[SyntheticDataPoint] = Field(default_factory=list)
    subtopic_requirements: Optional[int] = None
    agent_states: Dict[str, AgentState] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
