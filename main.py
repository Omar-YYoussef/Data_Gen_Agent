import asyncio
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import sys

from langdetect import detect, DetectorFactory
from config.settings import settings
from agents.query_parser_agent import QueryParserAgent
from agents.query_refiner_agent import QueryRefinerAgent
from agents.web_search_agent import WebSearchAgent
from agents.filtration_agent import FiltrationAgent
from agents.web_scraping_agent import WebScrapingAgent
from agents.topic_extraction_agent import TopicExtractionAgent
from agents.synthetic_data_generator_agent import SyntheticDataGeneratorAgent
# from agents.data_collection_agent import DataCollectionAgent
from services.chunking_service import chunking_service
# from utils.json_handler import JsonHandler
from utils.pipeline_state_manager import PipelineStateManager, STATUS_LEVELS
from models.data_schemas import SearchQuery
from models.data_schemas import SyntheticDataPoint
from models.data_schemas import ParsedQuery
# Setup enhanced logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    encoding='utf-8'  # Explicitly set encoding to UTF-8
)

# Set higher logging level for specific verbose libraries
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
    # "completed_partial": 10
}

# Ensure langdetect produces consistent results
#DetectorFactory.seed(0)

def _detect_language(text: str) -> Optional[str]:
    try:
        return detect(text)
    except Exception:  # Catch all exceptions from langdetect
        return None

async def main():
    """Complete pipeline execution with caching and resume functionality."""
    if len(sys.argv) > 1:
        user_query = " ".join(sys.argv[1:])
    else:
        logger.warning("No query provided via command-line. Using default query.")
        user_query = """I want to generate 15rows summarization synthitic datasets in Egyptian Arabic, The dataset consists of two columns
        (text - summarized_text), mix between short and long paragraphes."""

    logger.info("="*80)
    logger.info("Starting Complete Web Search & Synthetic Data Pipeline")
    logger.info(f"Query: {user_query}")
    logger.info("="*80)

    pipeline_start_time = datetime.now()
    state_manager = PipelineStateManager(user_query)
    state_manager.load_state()

    #parsed_query = None  # Initialize parsed_query to None

    # Check if the previous run for this query is complete.
    # if state_manager.get_status() in ["completed_partial"]:
    #     try:
    #         parsed_query_dict = state_manager.state.get("parsed_query")
    #         if parsed_query_dict:
    #             parsed_query = ParsedQuery(**parsed_query_dict)
                
    #             synthetic_data_count = state_manager.get_checkpoint_value("synthetic_data_generated_count", 0)

    #             if synthetic_data_count >= parsed_query.sample_count:
    #                 logger.warning("This query is already complete and meets the sample count.")
    #                 logger.warning("To run it again, all previous data will be cleared.")
    #                 state_manager.clear_all_state()
    #                 state_manager.initialize_new_state()
    #             else:
    #                 logger.info(f"Resuming partially completed pipeline. Goal: {parsed_query.sample_count}, Have: {synthetic_data_count}")
    #                 # Check what stage we should resume from based on available topics and processing status
    #                 all_topics = state_manager.load_asset("all_extracted_topics") or []
    #                 last_processed_topic_index = state_manager.get_checkpoint_value("last_processed_topic_index", -1)
                    
    #                 if len(all_topics) > 0 and last_processed_topic_index < len(all_topics) - 1:
    #                     # We have topics to process, continue with data generation
    #                     logger.info(f"Resuming from topic index {last_processed_topic_index + 1}/{len(all_topics)}")
    #                     state_manager.update_status("topics_extracted")
    #                 else:
    #                     # Need to gather more content since all topics have been processed
    #                     logger.info("All existing topics have been processed. Will gather more content.")
    #                     state_manager.update_status("query_parsed")
    #         else:
    #             # If there's no parsed query, something is wrong, so restart.
    #             state_manager.clear_all_state()
    #             state_manager.initialize_new_state()
    #     except Exception as e:
    #         logger.error(f"Error checking previous state, restarting pipeline. Error: {e}")
    #         state_manager.clear_all_state()
    #         state_manager.initialize_new_state()

    try:
        # =================================================================
        # STAGE 1: QUERY PARSING
        # =================================================================
        if state_manager.get_status_level() < STATUS_LEVELS["query_parsed"]:
            logger.info("\n" + "="*60)
            logger.info("STAGE 1: PARSING QUERY")
            logger.info("="*60)
            query_parser = QueryParserAgent()
            parsed_query = query_parser.run(user_query)
            # Ensure required_topics is calculated and stored with the parsed_query initially
            parsed_query.required_topics = parsed_query.calculate_required_subtopics(settings.ROWS_PER_SUBTOPIC)
            state_manager.state["parsed_query"] = parsed_query.model_dump()
            state_manager.update_status("query_parsed")
        else:
            logger.info("\n" + "="*60)
            logger.info("STAGE 1: SKIPPED (loaded from cache)")
            logger.info("="*60)
            parsed_query = ParsedQuery(**state_manager.state["parsed_query"])
            # Recalculate and set required_topics for parsed_query when loaded from cache
            parsed_query.required_topics = parsed_query.calculate_required_subtopics(settings.ROWS_PER_SUBTOPIC)

        # Load parsed query from state for subsequent steps
        # The required_topics is now part of parsed_query itself, and should be consistent
        # required_topics = parsed_query.calculate_required_subtopics() # This line is no longer needed here
        # parsed_query.required_topics = required_topics # This line is no longer needed here
        state_manager.update_checkpoint(required_topics=parsed_query.required_topics)

        # Main generation loop: continues until sample count is met or retries are exhausted
        # max_retries = 3
        # attempt = 0

        while True:
            # Get current status at the start of each attempt
            current_synthetic_data = state_manager.load_asset("synthetic_data") or []
            current_count = len(current_synthetic_data)
            
            logger.info("\n" + "="*80)
            logger.info(f"GENERATION LOOP: Current: {current_count} samples")
            logger.info(f"Goal: {parsed_query.sample_count} samples")
            logger.info("="*80)
            
            # Check if we've already met the target
            # if current_count >= parsed_query.sample_count:
            #     logger.info("Target sample count already met. Exiting generation loop.")
            #     break

            # =================================================================
            # STAGE 2: WEB SEARCH & CONTENT GATHERING
            # =================================================================
            if state_manager.get_status_level() < STATUS_LEVELS["content_gathered"]:
                logger.info("\n" + "="*60)
                logger.info(f"STAGE 2: WEB SEARCH & CONTENT GATHERING")
                logger.info("="*60)
                
                if state_manager.get_status_level() < STATUS_LEVELS["query_refined"]:
                    query_refiner = QueryRefinerAgent()
                    refined_queries = query_refiner.run(parsed_query)
                    state_manager.save_asset("refined_queries", [q.model_dump() for q in refined_queries])
                    state_manager.update_status("query_refined")
                else:
                    logger.info("Query Refinement: SKIPPED (loaded from cache)")
                    refined_queries_dicts = state_manager.load_asset("refined_queries")
                    refined_queries = [SearchQuery(**d) for d in refined_queries_dicts]

                if state_manager.get_status_level() < STATUS_LEVELS["web_searched"]:
                    web_search = WebSearchAgent()
                    search_results = web_search.run(refined_queries)
                    state_manager.save_asset("search_results", [r.model_dump() for r in search_results])
                    state_manager.update_status("web_searched")
                else:
                    logger.info("Web Search: SKIPPED (loaded from cache)")
                    from models.data_schemas import SearchResult
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
                    from models.data_schemas import ScrapedContent
                    scraped_content_dicts = state_manager.load_asset("scraped_content")
                    scraped_content = [ScrapedContent(**d) for d in scraped_content_dicts]
                
                # Language filtering for scraped content
                target_language = parsed_query.iso_language if parsed_query.iso_language else parsed_query.language.split('-')[0].lower() # Use iso_language if available
                initial_scraped_count = len(scraped_content)
                
                filtered_scraped_content = []
                for content_item in scraped_content:
                    detected_lang = _detect_language(content_item.content)
                    if detected_lang and detected_lang == target_language:
                        filtered_scraped_content.append(content_item)
                    else:
                        logger.warning(f"Filtered out content from {content_item.url} due to language mismatch. Expected: {target_language}, Detected: {detected_lang}")
                
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

            # =================================================================
            # STAGE 3: TOPIC EXTRACTION
            # =================================================================
            if state_manager.get_status_level() < STATUS_LEVELS["topics_extracted"]:
                all_chunks_dicts = state_manager.load_asset("all_chunks") or []
                from models.data_schemas import ContentChunk
                all_chunks = [ContentChunk(**c) for c in all_chunks_dicts]
                
                # Check if we already have enough topics before processing more chunks
                existing_topics = state_manager.load_asset("all_extracted_topics") or []
                if len(set(existing_topics)) >= parsed_query.required_topics:
                    logger.info(f"Already have sufficient topics ({len(set(existing_topics))}/{parsed_query.required_topics}). Skipping chunk processing.")
                    state_manager.update_status("topics_extracted")
                else:
                    last_processed_chunk_index = state_manager.get_checkpoint_value("last_processed_chunk_index", -1)
                    chunks_to_process = all_chunks[last_processed_chunk_index + 1:]

                    if chunks_to_process:
                        logger.info("\n" + "="*60)
                        logger.info(f"STAGE 3: EXTRACTING TOPICS from {len(chunks_to_process)} new chunks")
                        logger.info("="*60)
                        
                        topic_extraction_agent = TopicExtractionAgent()
                        
                        # Process chunks one by one or in small batches
                        for idx, chunk in enumerate(chunks_to_process):
                            newly_extracted_topics = await topic_extraction_agent.execute({
                                "chunks": [chunk],  # Process one chunk at a time
                                "language": parsed_query.language,
                                "domain_type": parsed_query.domain_type,
                                "required_topics_count": parsed_query.required_topics
                            })

                            all_topics = list(dict.fromkeys(existing_topics + newly_extracted_topics))
                            existing_topics = all_topics  # Update for next iteration
                            
                            # Save progress after each chunk
                            state_manager.save_asset("all_extracted_topics", all_topics)
                            state_manager.update_checkpoint(
                                topics_found=len(all_topics),
                                last_processed_chunk_index=last_processed_chunk_index + 1 + idx
                            )
                            state_manager.save_state()
                            
                            # Check if we've reached required topics count
                            if len(set(all_topics)) >= parsed_query.required_topics:
                                logger.info(f"Required topics count reached ({len(set(all_topics))}/{parsed_query.required_topics}). Stopping chunk processing early.")
                                break
                        
                        # After processing chunks, check if we have enough topics
                        all_topics_after_extraction = state_manager.load_asset("all_extracted_topics") or []
                        if len(set(all_topics_after_extraction)) < parsed_query.required_topics:
                            logger.warning(f"Required topics ({parsed_query.required_topics}) not met (found {len(set(all_topics_after_extraction))}). Re-gathering content.")
                            state_manager.update_status("query_parsed")
                            state_manager.clear_asset("refined_queries")
                            state_manager.clear_asset("search_results")
                            continue
                        else:
                            logger.info("Required topics met. Proceeding to synthetic data generation.")
                            state_manager.update_status("topics_extracted")
                    else:
                        # No more chunks to process
                        all_topics_after_extraction = state_manager.load_asset("all_extracted_topics") or []
                        if len(set(all_topics_after_extraction)) < parsed_query.required_topics:
                            logger.warning(f"No more chunks but required topics not met. Re-gathering content.")
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

            # =================================================================
            # STAGE 4: SYNTHETIC DATA GENERATION
            # =================================================================
            if state_manager.get_status_level() < STATUS_LEVELS["data_generated"]:
                logger.info("\n" + "="*60)
                logger.info("STAGE 4: SYNTHETIC DATA GENERATION")
                logger.info("="*60)
                
                all_topics = state_manager.load_asset("all_extracted_topics") or []
                last_processed_topic_index = state_manager.get_checkpoint_value("last_processed_topic_index", -1)
                topics_to_process = all_topics[last_processed_topic_index + 1:]

                logger.info(f"Total topics available: {len(all_topics)}")
                logger.info(f"Last processed topic index: {last_processed_topic_index}")
                logger.info(f"Topics remaining to process: {len(topics_to_process)}")

                if topics_to_process:
                    logger.info(f"Starting data generation for {len(topics_to_process)} remaining topics.")
                    
                    # Load existing data
                    existing_data_dicts = state_manager.load_asset("synthetic_data") or []
                    
                    existing_data = [SyntheticDataPoint(**d) for d in existing_data_dicts]
                    
                    agent = SyntheticDataGeneratorAgent(agent_index=1)
                    
                    # Process topics one by one
                    current_synthetic_data = existing_data.copy()
                    current_topic_index = last_processed_topic_index
                    
                    for i, topic in enumerate(topics_to_process):
                        try:
                            actual_topic_index = last_processed_topic_index + 1 + i
                            logger.info(f"Processing topic {actual_topic_index + 1}/{len(all_topics)}: {topic[:50]}...")
                            
                            new_synthetic_data = await agent.execute({
                                "topics": [topic],  # Process one topic at a time
                                "data_type": parsed_query.data_type,
                                "language": parsed_query.language,
                                "description": parsed_query.description
                            })

                            if new_synthetic_data:
                                current_synthetic_data.extend(new_synthetic_data)
                                logger.info(f"Topic {actual_topic_index + 1} processed successfully. Total samples: {len(current_synthetic_data)}")
                            else:
                                logger.warning(f"Topic {actual_topic_index + 1} generated no new synthetic data.")

                            # Always update the index and save progress after processing this topic
                            current_topic_index += 1
                            state_manager.save_asset("synthetic_data", [d.model_dump() for d in current_synthetic_data])
                            state_manager.update_checkpoint(
                                last_processed_topic_index=current_topic_index,
                                synthetic_data_generated_count=len(current_synthetic_data)
                            )
                            state_manager.save_state()  # Save state after each topic

                            # Check if we've reached our goal after processing each topic
                            if len(current_synthetic_data) >= parsed_query.sample_count:
                                logger.info("Target sample count reached during processing. Stopping topic processing.")
                                break
                            
                        except Exception as e:
                            logger.error(f"Error processing topic {actual_topic_index + 1}: {e}")
                            # Even if an error occurs, we increment the index to move past this topic
                            current_topic_index += 1
                            state_manager.update_checkpoint(
                                last_processed_topic_index=current_topic_index,
                                synthetic_data_generated_count=len(current_synthetic_data)
                            )
                            state_manager.save_state() # Save state on error
                            break # Exit inner loop to prevent getting stuck on this topic
                    
                else:
                    # # If there are no topics to process, check if the sample count has been met.
                    # # If not, revert to an earlier stage to gather more content/topics.
                    # if len(current_synthetic_data) < parsed_query.sample_count:
                    #     logger.info("No more topics to process in this iteration, and target sample count not met. Re-gathering content.")
                    #     state_manager.update_status("query_parsed")
                    #     state_manager.clear_asset("refined_queries")
                    #     state_manager.clear_asset("search_results")
                    #     state_manager.clear_asset("all_chunks")
                    #     continue # Continue to the next iteration of the main loop to re-gather content
                    # else:
                    logger.info("No topics to process in this iteration, and target sample count met. Marking data generation as complete.")
                    state_manager.update_status("data_generated")
            else:
                logger.info("\n" + "="*60)
                logger.info("STAGE 4: SKIPPED (loaded from cache)")
                logger.info("="*60)

            # Check current status after processing
            final_synthetic_data = state_manager.load_asset("synthetic_data") or []
            all_topics = state_manager.load_asset("all_extracted_topics") or []
            last_processed_topic_index = state_manager.get_checkpoint_value("last_processed_topic_index", -1)

            
            # if (len(final_synthetic_data) >= parsed_query.sample_count and
            #     len(all_topics) > 0 and 
            #     last_processed_topic_index >= len(all_topics) -1):
            #     logger.info("Target sample count met and all topics processed. Exiting generation loop.")
            #     state_manager.update_status("completed")
            #     break

            if len(final_synthetic_data) >= parsed_query.sample_count:
                logger.info("Target sample count met. Exiting generation loop.")
                state_manager.update_status("completed")
                break

        state_manager.save_state() # Final save
        
        pipeline_end_time = datetime.now()
        execution_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        # Determine the number of generated samples
        final_synthetic_data_dicts = state_manager.load_asset("synthetic_data") or []
        final_count = len(final_synthetic_data_dicts)

        logger.info("="*80)
        logger.info("🎉 PIPELINE EXECUTION FINISHED!")
        logger.info(f"Total execution time: {execution_time:.2f} seconds")
        logger.info(f"Final samples generated: {final_count} / {parsed_query.sample_count}")
        logger.info(f"Final synthetic data saved via PipelineStateManager.")
        logger.info("="*80)

    except Exception as e:
        logger.critical(f"An unrecoverable error occurred in the main pipeline: {e}", exc_info=True)
        state_manager.save_state() # Save state on failure
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        # The logger already captures the exception details.
        # This prevents a messy traceback at the very end.
        pass 
