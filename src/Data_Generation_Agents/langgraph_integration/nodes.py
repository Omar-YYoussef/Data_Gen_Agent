from __future__ import annotations
from typing import Optional
from langgraph.types import RunnableConfig
import logging
from ..services.gemini_service import GeminiQuotaExhaustedError, PipelineStopRequested

from ..agents.query_parser_agent import QueryParserAgent
from ..agents.query_refiner_agent import QueryRefinerAgent
from ..agents.web_search_agent import WebSearchAgent
from ..agents.filtration_agent import FiltrationAgent
from ..agents.web_scraping_agent import WebScrapingAgent
from ..agents.topic_extraction_agent import TopicExtractionAgent
from ..agents.synthetic_data_generator_agent import SyntheticDataGeneratorAgent
from ..services.chunking_service import chunking_service
from ..models.data_schemas import ParsedQuery, SearchQuery, SearchResult, ScrapedContent, ContentChunk
from ..utils.data_saver import DataSaver
from ..utils.status_manager import StatusManager
from ..utils.node_helpers import (
    load_dependencies,
    load_jsonl_count,
    load_checkpoint_metadata
)
from .state import PipelineState

logger = logging.getLogger(__name__)


# ============================================================================
# STAGE 1: Query Parsing
# ============================================================================

def query_parser_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Parse user query to extract requirements."""
    logger.info("\n" + "="*60)
    logger.info("STAGE 1: PARSING QUERY")
    logger.info("="*60)
    
    thread_id = state["status_manager_thread_id"]
    
    try:
        query_parser = QueryParserAgent()
        gemini_model_name = config["configurable"].get("gemini_model_name")
        
        parsed_query = query_parser.run(
            state["user_query"],
            context={"gemini_model_name": gemini_model_name}
        )
        
        rows_per_subtopic = state["config_params"]["rows_per_subtopic"]
        parsed_query.required_topics = parsed_query.calculate_required_subtopics(rows_per_subtopic)
        
        checkpoint_metadata = {
            "required_topics": parsed_query.required_topics,
            "topics_found": 0,
            "last_processed_chunk_index": -1,
            "last_processed_topic_index": -1,
            "synthetic_data_generated_count": 0,
            "retries": 0
        }
        
        data_saver = DataSaver(thread_id)
        parsed_query_data = parsed_query.model_dump(mode='json')
        data_saver.save_agent_output("parsed_query", parsed_query_data)
        
        status_manager = StatusManager(thread_id)
        status_manager.mark_node_completed("query_parser", "query_parsed")
        status_manager.update_checkpoint_metadata(**checkpoint_metadata)
        
        logger.info(f"Required topics: {parsed_query.required_topics}")
        logger.info(f"Target samples: {parsed_query.sample_count}")
        
        return {"parsed_query": parsed_query_data}
        
    except GeminiQuotaExhaustedError as e:
        logger.critical("\n" + "="*60)
        logger.critical("❌ GEMINI API QUOTA EXHAUSTED")
        logger.critical("="*60)
        logger.critical(f"Stage: Query Parsing")
        logger.critical(f"Error: {e}")
        logger.critical(f"Pipeline cannot continue without API access.")
        logger.critical(f"Please wait for quota reset or add more API keys.")
        logger.critical("="*60)
        
        raise PipelineStopRequested(
            reason=f"Gemini API quota exhausted: {e}",
            stage="query_parser"
        )


# ============================================================================
# STAGE 2A: Query Refinement
# ============================================================================

def query_refiner_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Generate refined search queries."""
    logger.info("\n" + "="*60)
    logger.info("STAGE 2A: QUERY REFINEMENT")
    logger.info("="*60)
    
    thread_id = state["status_manager_thread_id"]
    deps = load_dependencies(state, thread_id, "parsed_query")
    
    try:
        parsed_query = ParsedQuery(**deps["parsed_query"])
        query_refiner = QueryRefinerAgent()
        
        refined_queries_count = state["config_params"]["refined_queries_count"]
        gemini_model_name = config["configurable"].get("gemini_model_name")
        
        refined_queries = query_refiner.run(
            parsed_query,
            context={
                "refined_queries_count": refined_queries_count,
                "gemini_model_name": gemini_model_name
            }
        )
        
        refined_queries_data = [q.model_dump() for q in refined_queries]
        
        data_saver = DataSaver(thread_id)
        data_saver.save_agent_output("refined_queries", refined_queries_data)
        
        status_manager = StatusManager(thread_id)
        status_manager.mark_node_completed("query_refiner", "query_refined")
        
        logger.info(f"Generated {len(refined_queries_data)} refined queries")
        
        return {"refined_queries": refined_queries_data}
        
    except GeminiQuotaExhaustedError as e:
        logger.critical("\n" + "="*60)
        logger.critical("❌ GEMINI API QUOTA EXHAUSTED")
        logger.critical("="*60)
        logger.critical(f"Stage: Query Refinement")
        logger.critical(f"Error: {e}")
        logger.critical(f"Pipeline stopped. Progress saved at: query_parser")
        logger.critical("="*60)

        raise PipelineStopRequested(
            reason=f"Gemini API quota exhausted: {e}",
            stage="query_refiner"
        )


# ============================================================================
# STAGE 2B: Web Search
# ============================================================================

def web_search_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Execute web searches."""
    logger.info("\n" + "="*60)
    logger.info("STAGE 2B: WEB SEARCH")
    logger.info("="*60)
    
    thread_id = state["status_manager_thread_id"]
    
    # ✨ USE HELPER
    deps = load_dependencies(state, thread_id, "refined_queries")
    
    refined_queries = [SearchQuery(**q) for q in deps["refined_queries"]]
    web_search = WebSearchAgent()
    
    search_results_per_query = state["config_params"]["search_results_per_query"]
    search_results = web_search.run(refined_queries, context={"max_results": search_results_per_query})
    
    search_results_data = [r.model_dump() for r in search_results]
    
    data_saver = DataSaver(thread_id)
    data_saver.save_agent_output("search_results", search_results_data)
    
    status_manager = StatusManager(thread_id)
    status_manager.mark_node_completed("web_search", "web_searched")
    
    logger.info(f"Found {len(search_results_data)} search results")
    
    return {"search_results": search_results_data}


# ============================================================================
# STAGE 2C: Filtration
# ============================================================================

async def filtration_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Filter search results."""
    logger.info("\n" + "="*60)
    logger.info("STAGE 2C: FILTRATION")
    logger.info("="*60)
    
    thread_id = state["status_manager_thread_id"]
    
    # ✨ USE HELPER - Load multiple at once!
    deps = load_dependencies(state, thread_id, "search_results", "parsed_query")
    
    search_results = [SearchResult(**r) for r in deps["search_results"]]
    parsed_query = ParsedQuery(**deps["parsed_query"])
    
    filtration = FiltrationAgent()
    filtered_results = await filtration.execute(search_results, context={"language": parsed_query.language})
    
    filtered_results_data = [r.model_dump() for r in filtered_results]
    
    data_saver = DataSaver(thread_id)
    data_saver.save_agent_output("filtered_results", filtered_results_data)
    
    status_manager = StatusManager(thread_id)
    status_manager.mark_node_completed("filtration", "web_scraped")
    
    logger.info(f"Filtered to {len(filtered_results_data)} results")
    
    return {"filtered_results": filtered_results_data}


# ============================================================================
# STAGE 2D: Web Scraping
# ============================================================================

async def web_scraping_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Scrape content from filtered URLs."""
    logger.info("\n" + "="*60)
    logger.info("STAGE 2D: WEB SCRAPING")
    logger.info("="*60)
    
    thread_id = state["status_manager_thread_id"]
    
    # ✨ USE HELPER
    deps = load_dependencies(state, thread_id, "filtered_results")
    
    filtered_results = [SearchResult(**r) for r in deps["filtered_results"]]
    scraping_agent = WebScrapingAgent()
    
    scraped_content = await scraping_agent.execute_async(filtered_results)
    scraped_content_data = [c.model_dump() for c in scraped_content]
    
    data_saver = DataSaver(thread_id)
    data_saver.save_agent_output("scraped_content", scraped_content_data)
    
    status_manager = StatusManager(thread_id)
    status_manager.mark_node_completed("web_scraping", "web_scraped")
    
    successful_scrapes = sum(1 for c in scraped_content if c.success)
    logger.info(f"Scraped {successful_scrapes}/{len(scraped_content)} pages successfully")
    
    return {"scraped_content": scraped_content_data}


# ============================================================================
# STAGE 2E: Chunking
# ============================================================================

def chunking_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Filter by language and chunk content."""
    logger.info("\n" + "="*60)
    logger.info("STAGE 2E: LANGUAGE FILTERING & CHUNKING")
    logger.info("="*60)
    
    thread_id = state["status_manager_thread_id"]
    
    # ✨ USE HELPER
    deps = load_dependencies(state, thread_id, "parsed_query", "scraped_content")
    
    parsed_query = ParsedQuery(**deps["parsed_query"])
    scraped_content = [ScrapedContent(**c) for c in deps["scraped_content"]]
    
    # Language filtering
    from langdetect import detect
    target_language = parsed_query.iso_language if parsed_query.iso_language else parsed_query.language.split('-')[0].lower()
    
    filtered_scraped_content = []
    for content_item in scraped_content:
        try:
            detected_lang = detect(content_item.content)
            if detected_lang == target_language:
                filtered_scraped_content.append(content_item)
        except:
            pass
    
    logger.info(f"Language filtering: {len(filtered_scraped_content)}/{len(scraped_content)} items retained")
    
    # Chunking
    scraped_data = [c.model_dump() for c in filtered_scraped_content if c.success]
    all_chunks = chunking_service.chunk_content(scraped_data)
    all_chunks_data = [c.model_dump() for c in all_chunks]
    
    data_saver = DataSaver(thread_id)
    data_saver.save_agent_output("all_chunks", all_chunks_data)
    
    status_manager = StatusManager(thread_id)
    status_manager.mark_node_completed("chunking", "content_gathered")
    
    logger.info(f"Created {len(all_chunks_data)} content chunks")
    
    return {"all_chunks": all_chunks_data}


# ============================================================================
# STAGE 3: Topic Extraction (Incremental)
# ============================================================================

async def topic_extraction_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Extract topics from next unprocessed chunk."""
    thread_id = state["status_manager_thread_id"]
    
    # ✨ USE HELPERS - Much cleaner!
    deps = load_dependencies(state, thread_id, "parsed_query", "all_chunks")
    checkpoint_metadata = load_checkpoint_metadata(state, thread_id)
    current_count = load_jsonl_count(state, thread_id, "extracted_topics")
    
    parsed_query = ParsedQuery(**deps["parsed_query"])
    all_chunks = [ContentChunk(**c) for c in deps["all_chunks"]]
    
    last_processed_chunk_index = checkpoint_metadata["last_processed_chunk_index"]
    next_chunk_index = last_processed_chunk_index + 1
    
    if next_chunk_index >= len(all_chunks):
        logger.info("✓ All chunks processed")
        status_manager = StatusManager(thread_id)
        status_manager.mark_node_completed("topic_extraction", "topics_extracted")
        return {}
    
    chunk = all_chunks[next_chunk_index]
    logger.info(f"\n📝 Processing chunk {next_chunk_index + 1}/{len(all_chunks)}")
    
    try:
        topic_extraction_agent = TopicExtractionAgent()
        gemini_model_name = config["configurable"].get("gemini_model_name")
        
        newly_extracted_topics = await topic_extraction_agent.execute({
            "chunks": [chunk],
            "language": parsed_query.language,
            "domain_type": parsed_query.domain_type,
            "required_topics_count": parsed_query.required_topics
        }, context={"gemini_model_name": gemini_model_name})
        
        if not newly_extracted_topics or len(newly_extracted_topics) == 0:
            logger.warning(f"⚠ No topics extracted from chunk {next_chunk_index + 1}")
            logger.warning(f"⚠ Chunk will be retried on next run (index NOT incremented)")
            return {"extracted_topics_count": current_count}
        
        # Success: increment and save
        checkpoint_metadata["last_processed_chunk_index"] = next_chunk_index
        
        data_saver = DataSaver(thread_id)
        data_saver.append_to_json_array("extracted_topics", newly_extracted_topics)
        
        new_count = current_count + len(newly_extracted_topics)
        
        # Update unique count
        all_topics = data_saver.load_agent_output("extracted_topics") or []
        unique_topics_count = len(set(all_topics))
        checkpoint_metadata["topics_found"] = unique_topics_count
        
        status_manager = StatusManager(thread_id)
        status_manager.update_checkpoint_metadata(**checkpoint_metadata)
        
        logger.info(f"✓ Appended {len(newly_extracted_topics)} new topics to file")
        logger.info(f"Progress - Topics: {unique_topics_count}/{parsed_query.required_topics} | Chunk: {next_chunk_index + 1}/{len(all_chunks)}")
        
        return {"extracted_topics_count": new_count}
        
            # Raise to stop pipeline immediately
    except GeminiQuotaExhaustedError as e:
        logger.critical("\n" + "="*60)
        logger.critical("❌ GEMINI API QUOTA EXHAUSTED")
        logger.critical("="*60)
        logger.critical(f"Stage: Topic Extraction")
        logger.critical(f"Chunk: {next_chunk_index + 1}/{len(all_chunks)}")
        logger.critical(f"Error: {e}")
        logger.critical(f"Progress saved. Will resume from chunk {next_chunk_index + 1}")
        logger.critical("="*60)
        
        # Raise to stop pipeline immediately
        raise PipelineStopRequested(
            reason=f"Gemini API quota exhausted: {e}",
            stage="topic_extraction"
        )
        


# ============================================================================
# STAGE 4: Synthetic Data Generation (Incremental)
# ============================================================================

async def synthetic_data_generator_node(state: PipelineState, config: RunnableConfig) -> dict:
    """Generate synthetic data from next unprocessed topic."""
    thread_id = state["status_manager_thread_id"]
    
    # ✨ USE HELPERS - So clean!
    deps = load_dependencies(state, thread_id, "parsed_query")
    checkpoint_metadata = load_checkpoint_metadata(state, thread_id)
    current_count = load_jsonl_count(state, thread_id, "synthetic_data")
    
    # Load topics from JSONL
    data_saver = DataSaver(thread_id)
    all_topics = data_saver.load_agent_output("extracted_topics") or [] 
    
    if not all_topics:
        logger.error("Cannot generate data: extracted_topics.jsonl not found or empty")
        return {}
    
    parsed_query = ParsedQuery(**deps["parsed_query"])
    
    last_processed_topic_index = checkpoint_metadata["last_processed_topic_index"]
    next_topic_index = last_processed_topic_index + 1
    
    if next_topic_index >= len(all_topics):
        logger.info("✓ All topics processed")
        status_manager = StatusManager(thread_id)
        status_manager.mark_node_completed("synthetic_data_generator", "data_generated")
        return {}
    
    topic = all_topics[next_topic_index]
    logger.info(f"\n🔄 Processing topic {next_topic_index + 1}/{len(all_topics)}: '{topic}'")
    
    try:
        agent = SyntheticDataGeneratorAgent(agent_index=1)
        gemini_model_name = config["configurable"].get("gemini_model_name")
        
        new_synthetic_data = await agent.execute({
            "topics": [topic],
            "data_type": parsed_query.data_type,
            "language": parsed_query.language,
            "description": parsed_query.description
        }, context={"gemini_model_name": gemini_model_name})
        
        if not new_synthetic_data or len(new_synthetic_data) == 0:
            logger.warning(f"⚠ No data generated for topic '{topic}'")
            logger.warning(f"⚠ Topic will be retried on next run (index NOT incremented)")
            return {"synthetic_data_count": current_count}
        
        # Success: increment and save
        checkpoint_metadata["last_processed_topic_index"] = next_topic_index
        
        new_data_dicts = [d.model_dump() for d in new_synthetic_data]
        data_saver.append_to_json_array("synthetic_data", new_data_dicts)
        
        new_count = current_count + len(new_data_dicts)
        checkpoint_metadata["synthetic_data_generated_count"] = new_count
        
        status_manager = StatusManager(thread_id)
        status_manager.update_checkpoint_metadata(**checkpoint_metadata)
        
        logger.info(f"✓ Appended {len(new_data_dicts)} new samples to file")
        logger.info(f"Progress - Samples: {new_count} | Topic: {next_topic_index + 1}/{len(all_topics)}")
        
        return {"synthetic_data_count": new_count}
        
    except GeminiQuotaExhaustedError as e:
        logger.critical("\n" + "="*60)
        logger.critical("❌ GEMINI API QUOTA EXHAUSTED")
        logger.critical("="*60)
        logger.critical(f"Stage: Synthetic Data Generation")
        logger.critical(f"Topic: {next_topic_index + 1}/{len(all_topics)} - '{topic}'")
        logger.critical(f"Error: {e}")
        logger.critical(f"Progress saved. Will resume from topic {next_topic_index + 1}")
        logger.critical("="*60)
        
        # Raise to stop pipeline immediately
        raise PipelineStopRequested(
            reason=f"Gemini API quota exhausted: {e}",
            stage="synthetic_data_generator"
        )


# ============================================================================
# CONDITIONAL ROUTING FUNCTIONS
# ============================================================================

def check_topics_sufficient(state: PipelineState) -> str:
    """Check if enough topics have been extracted."""
    thread_id = state["status_manager_thread_id"]
    data_saver = DataSaver(thread_id)
    status_manager = StatusManager(thread_id)
    
    # ✨ USE HELPER
    deps = load_dependencies(state, thread_id, "parsed_query")
    
    parsed_query = ParsedQuery(**deps["parsed_query"])
    all_topics = data_saver.load_agent_output("extracted_topics") or [] 
    all_chunks = data_saver.load_agent_output("all_chunks") or []
    
    checkpoint_metadata = status_manager.get_checkpoint_metadata()
    last_processed_chunk_index = checkpoint_metadata.get("last_processed_chunk_index", -1)
    
    unique_topics_count = len(set(all_topics))
    
    if unique_topics_count >= parsed_query.required_topics:
        logger.info(f"✓ Required topics met: {unique_topics_count}/{parsed_query.required_topics}")
        status_manager.mark_node_completed("topic_extraction", "topics_extracted")
        return "sufficient"
    
    if last_processed_chunk_index + 1 < len(all_chunks):
        logger.info(f"Processing more chunks... Topics: {unique_topics_count}/{parsed_query.required_topics}")
        return "continue_extraction"
    
    logger.warning(f"⚠ Insufficient topics: {unique_topics_count}/{parsed_query.required_topics}")
    status_manager.mark_node_completed("query_parser", "query_parsed")
    return "regather_content"


def check_sample_count_met(state: PipelineState) -> str:
    """Check if all topics have been processed."""
    thread_id = state["status_manager_thread_id"]
    data_saver = DataSaver(thread_id)
    status_manager = StatusManager(thread_id)
    
    all_topics = data_saver.load_agent_output("extracted_topics") or [] 
    checkpoint_metadata = status_manager.get_checkpoint_metadata()
    last_processed_topic_index = checkpoint_metadata.get("last_processed_topic_index", -1)
    
    if last_processed_topic_index + 1 < len(all_topics):
        return "continue_generation"
    
    logger.info(f"✓ All {len(all_topics)} topics processed")
    status_manager.update_status("completed")
    return "complete"
