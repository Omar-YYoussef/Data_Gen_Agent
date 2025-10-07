from __future__ import annotations
from typing import TypedDict, Annotated, Optional
from operator import add

class PipelineState(TypedDict):
    """LangGraph state definition."""
    # Core data
    user_query: str
    parsed_query: Optional[dict]
    
    # Pipeline data
    refined_queries: list[dict]
    search_results: list[dict]
    filtered_results: list[dict]
    scraped_content: list[dict]
    all_chunks: list[dict]
    extracted_topics: Annotated[list[str], add]
    synthetic_data: Annotated[list[dict], add]
    
    # Metadata
    checkpoint_metadata: dict
    error_info: Optional[dict]
    config_params: dict
    status_manager_thread_id: str
