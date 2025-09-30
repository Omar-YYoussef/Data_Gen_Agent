import asyncio
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import sys

from langdetect import detect, DetectorFactory

from .config.settings import settings
from .agents.query_parser_agent import QueryParserAgent
from .agents.query_refiner_agent import QueryRefinerAgent
from .agents.web_search_agent import WebSearchAgent
from .agents.filtration_agent import FiltrationAgent
from .agents.web_scraping_agent import WebScrapingAgent
from .agents.topic_extraction_agent import TopicExtractionAgent
from .agents.synthetic_data_generator_agent import SyntheticDataGeneratorAgent
from .services.chunking_service import chunking_service
from .utils.pipeline_state_manager import PipelineStateManager, STATUS_LEVELS
from .models.data_schemas import SearchQuery, SyntheticDataPoint, ParsedQuery, ScrapedContent, ContentChunk, SearchResult

# Setup enhanced logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

logging.getLogger('scraperapi_sdk._client').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

STATUS_LEVELS = {
    "initial": 0,
    "initialized": 1,
    "query_parsed": 2,
    "query_refined": 3,
    "web_searched": 4,
    "web_scraped": 5,
    "content_gathered": 6,
    "topics_extracted": 7,
    "data_generated": 8,
    "completed": 9,
}


def _detect_language(text: str) -> Optional[str]:
    try:
        return detect(text)
    except Exception:
        return None


async def run_pipeline(
    user_query: str,
    refined_queries_count: Optional[int] = None,
    search_results_per_query: Optional[int] = None,
    rows_per_subtopic: Optional[int] = None,
    gemini_model_name: Optional[str] = None
) -> None:
    """
    Run the pipeline and save results automatically.
    
    Args:
        user_query: The data generation request
        refined_queries_count: Number of refined queries (default from .env)
        search_results_per_query: Results per query (default from .env)
        rows_per_subtopic: Rows per subtopic (default from .env)
    """
    # Use provided values or fall back to settings
    _refined_queries_count = refined_queries_count or settings.REFINED_QUERIES_COUNT
    _search_results_per_query = search_results_per_query or settings.SEARCH_RESULTS_PER_QUERY
    _rows_per_subtopic = rows_per_subtopic or settings.ROWS_PER_SUBTOPIC
    
    logger.info("="*80)
    logger.info("Starting Synthetic Data Pipeline")
    logger.info(f"Query: {user_query}")
    logger.info(f"Config: Queries={_refined_queries_count}, Results/Query={_search_results_per_query}, Rows/Topic={_rows_per_subtopic}")
    logger.info("="*80)

    pipeline_start_time = datetime.now()
    state_manager = PipelineStateManager(user_query)
    state_manager.load_state()

    try:
        # STAGE 1: QUERY PARSING
        if state_manager.get_status_level() < STATUS_LEVELS["query_parsed"]:
            logger.info("\n" + "="*60)
            logger.info("STAGE 1: PARSING QUERY")
            logger.info("="*60)
            query_parser = QueryParserAgent()
            parsed_query = query_parser.run(user_query, context={"gemini_model_name": gemini_model_name})
            parsed_query.required_topics = parsed_query.calculate_required_subtopics(_rows_per_subtopic)
            state_manager.state["parsed_query"] = parsed_query.model_dump()
            state_manager.update_status("query_parsed")
        else:
            logger.info("\n" + "="*60)
            logger.info("STAGE 1: SKIPPED (loaded from cache)")
            logger.info("="*60)
            parsed_query = ParsedQuery(**state_manager.state["parsed_query"])
            parsed_query.required_topics = parsed_query.calculate_required_subtopics(_rows_per_subtopic)

        state_manager.update_checkpoint(required_topics=parsed_query.required_topics)

        while True:
            current_synthetic_data = state_manager.load_asset("synthetic_data") or []
            current_count = len(current_synthetic_data)
            
            logger.info("\n" + "="*80)
            logger.info(f"GENERATION LOOP: Current: {current_count} samples")
            logger.info(f"Goal: {parsed_query.sample_count} samples")
            logger.info("="*80)

            # STAGE 2: WEB SEARCH & CONTENT GATHERING
            if state_manager.get_status_level() < STATUS_LEVELS["content_gathered"]:
                logger.info("\n" + "="*60)
                logger.info(f"STAGE 2: WEB SEARCH & CONTENT GATHERING")
                logger.info("="*60)
                
                if state_manager.get_status_level() < STATUS_LEVELS["query_refined"]:
                    query_refiner = QueryRefinerAgent()
                    refined_queries = query_refiner.run(
                        parsed_query, 
                        context={"refined_queries_count": _refined_queries_count}
                    )
                    state_manager.save_asset("refined_queries", [q.model_dump() for q in refined_queries])
                    state_manager.update_status("query_refined")
                else:
                    logger.info("Query Refinement: SKIPPED (loaded from cache)")
                    refined_queries_dicts = state_manager.load_asset("refined_queries")
                    refined_queries = [SearchQuery(**d) for d in refined_queries_dicts]

                if state_manager.get_status_level() < STATUS_LEVELS["web_searched"]:
                    web_search = WebSearchAgent()
                    search_results = web_search.run(
                        refined_queries,
                        context={"max_results": _search_results_per_query}
                    )
                    state_manager.save_asset("search_results", [r.model_dump() for r in search_results])
                    state_manager.update_status("web_searched")
                else:
                    logger.info("Web Search: SKIPPED (loaded from cache)")
                    search_results_dicts = state_manager.load_asset("search_results")
                    search_results = [SearchResult(**d) for d in search_results_dicts]

                if state_manager.get_status_level() < STATUS_LEVELS["web_scraped"]:
                    filtration = FiltrationAgent()
                    filtered_results = await filtration.execute(search_results, context={"language": parsed_query.language})
                    state_manager.save_asset("filtered_results", [r.model_dump() for r in filtered_results])

                    scraping_agent = WebScrapingAgent()
                    scraped_content = await scraping_agent.execute_async(filtered_results)
                    state_manager.save_asset("scraped_content", [c.model_dump() for c in scraped_content])
                    state_manager.update_status("web_scraped")
                else:
                    logger.info("Web Scraping: SKIPPED (loaded from cache)")
                    scraped_content_dicts = state_manager.load_asset("scraped_content")
                    scraped_content = [ScrapedContent(**d) for d in scraped_content_dicts]
                
                # Language filtering
                target_language = parsed_query.iso_language if parsed_query.iso_language else parsed_query.language.split('-')[0].lower()
                initial_scraped_count = len(scraped_content)
                
                filtered_scraped_content = []
                for content_item in scraped_content:
                    detected_lang = _detect_language(content_item.content)
                    if detected_lang and detected_lang == target_language:
                        filtered_scraped_content.append(content_item)
                    else:
                        logger.warning(f"Filtered out content from {content_item.url} due to language mismatch.")
                
                scraped_content = filtered_scraped_content
                logger.info(f"Language filtering complete. {len(scraped_content)}/{initial_scraped_count} items retained.")

                scraped_data = [c.model_dump() for c in scraped_content if c.success]
                all_chunks = chunking_service.chunk_content(scraped_data)
                state_manager.save_asset("all_chunks", [c.model_dump() for c in all_chunks])
                state_manager.update_checkpoint(last_processed_chunk_index=-1)
                state_manager.update_status("content_gathered")
            else:
                logger.info("\n" + "="*60)
                logger.info("STAGE 2: SKIPPED (loaded from cache)")
                logger.info("="*60)

            # STAGE 3: TOPIC EXTRACTION
            if state_manager.get_status_level() < STATUS_LEVELS["topics_extracted"]:
                all_chunks_dicts = state_manager.load_asset("all_chunks") or []
                all_chunks = [ContentChunk(**c) for c in all_chunks_dicts]
                
                existing_topics = state_manager.load_asset("all_extracted_topics") or []
                if len(set(existing_topics)) >= parsed_query.required_topics:
                    logger.info(f"Already have sufficient topics. Skipping chunk processing.")
                    state_manager.update_status("topics_extracted")
                else:
                    last_processed_chunk_index = state_manager.get_checkpoint_value("last_processed_chunk_index", -1)
                    chunks_to_process = all_chunks[last_processed_chunk_index + 1:]

                    if chunks_to_process:
                        logger.info("\n" + "="*60)
                        logger.info(f"STAGE 3: EXTRACTING TOPICS from {len(chunks_to_process)} new chunks")
                        logger.info("="*60)
                        
                        topic_extraction_agent = TopicExtractionAgent()
                        
                        for idx, chunk in enumerate(chunks_to_process):
                            newly_extracted_topics = await topic_extraction_agent.execute({
                                "chunks": [chunk],
                                "language": parsed_query.language,
                                "domain_type": parsed_query.domain_type,
                                "required_topics_count": parsed_query.required_topics
                            })

                            all_topics = list(dict.fromkeys(existing_topics + newly_extracted_topics))
                            existing_topics = all_topics
                            
                            state_manager.save_asset("all_extracted_topics", all_topics)
                            state_manager.update_checkpoint(
                                topics_found=len(all_topics),
                                last_processed_chunk_index=last_processed_chunk_index + 1 + idx
                            )
                            state_manager.save_state()
                            
                            if len(set(all_topics)) >= parsed_query.required_topics:
                                logger.info(f"Required topics count reached. Stopping chunk processing early.")
                                break
                        
                        all_topics_after_extraction = state_manager.load_asset("all_extracted_topics") or []
                        if len(set(all_topics_after_extraction)) < parsed_query.required_topics:
                            logger.warning(f"Required topics not met. Re-gathering content.")
                            state_manager.update_status("query_parsed")
                            state_manager.clear_asset("refined_queries")
                            state_manager.clear_asset("search_results")
                            continue
                        else:
                            state_manager.update_status("topics_extracted")
                    else:
                        all_topics_after_extraction = state_manager.load_asset("all_extracted_topics") or []
                        if len(set(all_topics_after_extraction)) < parsed_query.required_topics:
                            logger.warning(f"No more chunks but required topics not met.")
                            state_manager.update_status("query_parsed")
                            state_manager.clear_asset("refined_queries")
                            state_manager.clear_asset("search_results")
                            continue
                        else:
                            state_manager.update_status("topics_extracted")
            else:
                logger.info("\n" + "="*60)
                logger.info("STAGE 3: SKIPPED (loaded from cache)")
                logger.info("="*60)

            # STAGE 4: SYNTHETIC DATA GENERATION
            if state_manager.get_status_level() < STATUS_LEVELS["data_generated"]:
                logger.info("\n" + "="*60)
                logger.info("STAGE 4: SYNTHETIC DATA GENERATION")
                logger.info("="*60)
                
                all_topics = state_manager.load_asset("all_extracted_topics") or []
                last_processed_topic_index = state_manager.get_checkpoint_value("last_processed_topic_index", -1)
                topics_to_process = all_topics[last_processed_topic_index + 1:]

                if topics_to_process:
                    existing_data_dicts = state_manager.load_asset("synthetic_data") or []
                    existing_data = [SyntheticDataPoint(**d) for d in existing_data_dicts]
                    
                    agent = SyntheticDataGeneratorAgent(agent_index=1)
                    current_synthetic_data = existing_data.copy()
                    current_topic_index = last_processed_topic_index
                    
                    for i, topic in enumerate(topics_to_process):
                        try:
                            actual_topic_index = last_processed_topic_index + 1 + i
                            logger.info(f"Processing topic {actual_topic_index + 1}/{len(all_topics)}")
                            
                            new_synthetic_data = await agent.execute({
                                "topics": [topic],
                                "data_type": parsed_query.data_type,
                                "language": parsed_query.language,
                                "description": parsed_query.description
                            })

                            if new_synthetic_data:
                                current_synthetic_data.extend(new_synthetic_data)

                            current_topic_index += 1
                            state_manager.save_asset("synthetic_data", [d.model_dump() for d in current_synthetic_data])
                            state_manager.update_checkpoint(
                                last_processed_topic_index=current_topic_index,
                                synthetic_data_generated_count=len(current_synthetic_data)
                            )
                            state_manager.save_state()

                            if len(current_synthetic_data) >= parsed_query.sample_count:
                                logger.info("Target sample count reached.")
                                break
                            
                        except Exception as e:
                            logger.error(f"Error processing topic {actual_topic_index + 1}: {e}")
                            current_topic_index += 1
                            state_manager.update_checkpoint(
                                last_processed_topic_index=current_topic_index,
                                synthetic_data_generated_count=len(current_synthetic_data)
                            )
                            state_manager.save_state()
                            break
                else:
                    state_manager.update_status("data_generated")
            else:
                logger.info("\n" + "="*60)
                logger.info("STAGE 4: SKIPPED (loaded from cache)")
                logger.info("="*60)

            final_synthetic_data = state_manager.load_asset("synthetic_data") or []

            if len(final_synthetic_data) >= parsed_query.sample_count:
                logger.info("Target sample count met. Exiting generation loop.")
                state_manager.update_status("completed")
                break

        state_manager.save_state()
        
        pipeline_end_time = datetime.now()
        execution_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        logger.info("="*80)
        logger.info("🎉 PIPELINE EXECUTION FINISHED!")
        logger.info(f"Total execution time: {execution_time:.2f} seconds")
        logger.info(f"Final samples generated: {len(final_synthetic_data)} / {parsed_query.sample_count}")
        logger.info("="*80)

    except Exception as e:
        logger.critical(f"Pipeline error: {e}", exc_info=True)
        state_manager.save_state()


def generate_synthetic_data(
    user_query: str,
    refined_queries_count: Optional[int] = None,
    search_results_per_query: Optional[int] = None,
    rows_per_subtopic: Optional[int] = None,
    gemini_model_name: Optional[str] = None
) -> None:
    """
    Generate synthetic data based on the query.
    
    Args:
        user_query: The data generation request
        refined_queries_count: Number of refined queries (default from .env: 30)
        search_results_per_query: Results per query (default from .env: 5)
        rows_per_subtopic: Rows per subtopic (default from .env: 5)
    """
    asyncio.run(run_pipeline(
        user_query,
        refined_queries_count,
        search_results_per_query,
        rows_per_subtopic
    ))


async def main():
    """CLI entry point."""
    if len(sys.argv) > 1:
        user_query = " ".join(sys.argv[1:])
    else:
        logger.error("No query provided. Usage: synthetic-data 'your query here'")
        sys.exit(1)

    await run_pipeline(user_query)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        pass