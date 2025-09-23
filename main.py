"""
Main entry point for the complete web search and synthetic data pipeline
Enhanced version with Weeks 3 & 4 implementation
"""

import asyncio
import logging
from typing import Dict, Any
from pathlib import Path
from datetime import datetime
import sys

from config.settings import settings
from agents.query_parser_agent import QueryParserAgent
from agents.query_refiner_agent import QueryRefinerAgent
from agents.web_search_agent import WebSearchAgent
from agents.filtration_agent import FiltrationAgent
from agents.web_scraping_agent import WebScrapingAgent
from agents.topic_extraction_agent import TopicExtractionAgent
from agents.synthetic_data_generator_agent import SyntheticDataGeneratorAgent
from agents.data_collection_agent import DataCollectionAgent
from services.chunking_service import chunking_service
from utils.json_handler import JsonHandler

# Setup enhanced logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    encoding='utf-8'  # Explicitly set encoding to UTF-8
)

logger = logging.getLogger(__name__)

async def main():
    """Complete pipeline execution with Weeks 3 & 4"""
    # I want to generate 2000 QA synthitic Medical datasets in Egyptian Arabic, The dataset consists of 
    # the data set consisist of (question - answer) pairs, mix between short and long answers.
    # Get query from command-line arguments or use a default for direct execution
    if len(sys.argv) > 1:
        user_query = " ".join(sys.argv[1:])
    else:
        logger.warning("No query provided via command-line. Using default query from main.py.")
        user_query = """
        I want to generate 2000 QA synthitic LAW datasets in Egyptian Arabic, The dataset consists of 
        the data set consisist of (question - answer) pairs, mix between short and long answers.
        """
    
    logger.info("="*80)
    logger.info(f"Starting Complete Web Search & Synthetic Data Pipeline")
    logger.info(f"Query: {user_query}")
    logger.info("="*80)
    
    pipeline_start_time = datetime.now()
    
    try:
        # ===== WEEK 1-2: Query Processing & Web Search =====
        
        # Step 1: Parse Query
        logger.info("\n" + "="*60)
        logger.info("STEP 1: PARSING USER QUERY")
        logger.info("="*60)
        
        query_parser = QueryParserAgent()
        parsed_query = query_parser.run(user_query)
        
        # Step 2: Refine Queries  
        logger.info("\n" + "="*60)
        logger.info("STEP 2: GENERATING REFINED SEARCH QUERIES")
        logger.info("="*60)
        
        query_refiner = QueryRefinerAgent()
        refined_queries = query_refiner.run(parsed_query)
        
        # Step 3: Web Search
        logger.info("\n" + "="*60)
        logger.info("STEP 3: PERFORMING WEB SEARCH")
        logger.info("="*60)
        
        web_search = WebSearchAgent()
        search_results = web_search.run(refined_queries)
        
        # Step 4: Filter Results
        logger.info("\n" + "="*60)
        logger.info("STEP 4: FILTERING & DEDUPLICATING RESULTS")
        logger.info("="*60)
        
        filtration = FiltrationAgent()
        filtered_results = filtration.run(search_results)
        
        # ===== WEEK 3: Web Scraping & Topic Extraction =====
        
        # Step 5: Web Scraping
        logger.info("\n" + "="*60)
        logger.info("STEP 5: WEB SCRAPING")
        logger.info("="*60)
        
        scraping_agent = WebScrapingAgent()
        scraped_content = await scraping_agent.execute_async(filtered_results)
        
        # Convert to dict format for chunking service
        scraped_data = []
        for content in scraped_content:
            if content.success:
                scraped_data.append({
                    "url": content.url,
                    "title": content.title,
                    "content": content.content,
                    "success": True
                })

        # Step 6: Topic Extraction
        logger.info("\n" + "="*60)
        logger.info("STEP 6: TOPIC EXTRACTION")
        logger.info("="*60)
        
        all_chunks = chunking_service.chunk_content(scraped_data)
        topic_extraction_agent = TopicExtractionAgent(agent_index=1)
        extracted_topics = topic_extraction_agent.run({
            "chunks": all_chunks,
            "language": parsed_query.language,
            "domain_type": parsed_query.domain_type,
            "required_topics_count": parsed_query.calculate_required_subtopics()
        })
        
        # ===== WEEK 4: Synthetic Data Generation =====
        
        # Step 7: Synthetic Data Generation
        logger.info("\n" + "="*60)
        logger.info("STEP 7: SYNTHETIC DATA GENERATION")
        logger.info("="*60)
        
        synthetic_data_agent = SyntheticDataGeneratorAgent(agent_index=1)
        synthetic_data = synthetic_data_agent.run({
            "topics": extracted_topics,
            "data_type": parsed_query.data_type,
            "language": parsed_query.language,
            "description": parsed_query.description
        })
        
        # Step 8: Data Collection & Finalization
        logger.info("\n" + "="*60)
        logger.info("STEP 8: DATA COLLECTION & FINALIZATION")
        logger.info("="*60)
        
        collection_agent = DataCollectionAgent()
        final_result = collection_agent.run({
            "synthetic_data_lists": [synthetic_data],
            "parsed_query": parsed_query
        })
        
        # ===== PIPELINE COMPLETION =====
        
        pipeline_end_time = datetime.now()
        execution_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        # Create comprehensive final summary
        final_summary = {
            "pipeline_metadata": {
                "execution_time_seconds": execution_time,
                "start_time": pipeline_start_time.isoformat(),
                "end_time": pipeline_end_time.isoformat(),
                "pipeline_version": "Complete_Weeks_1_4"
            },
            "stage_results": {
                "query_parsing": {
                    "domain": parsed_query.domain_type,
                    "data_type": parsed_query.data_type,
                    "language": parsed_query.language,
                    "requested_samples": parsed_query.sample_count,
                    "required_subtopics": parsed_query.calculate_required_subtopics()
                },
                "web_search": {
                    "refined_queries": len(refined_queries),
                    "search_results": len(search_results),
                    "filtered_urls": len(filtered_results)
                },
                "content_processing": {
                    "scraped_pages": len([d for d in scraped_data if d.get("success", False)]),
                    "extracted_topics": len(extracted_topics)
                },
                "synthetic_generation": {
                    "final_samples": final_result["final_dataset"]["metadata"]["actual_count"],
                    "completion_rate": final_result["final_dataset"]["metadata"]["completion_rate"]
                }
            },
            "output_files": {
                "final_dataset": f"final_output/collected_datasets/final_dataset_{parsed_query.data_type}_{parsed_query.domain_type.replace(' ', '_')}.json"
            }
        }
        
        # Save final summary
        JsonHandler.save_json(
            final_summary,
            settings.FINAL_OUTPUT_PATH / "complete_pipeline_summary.json"
        )
        
        # Print completion summary
        logger.info("\n" + "="*80)
        logger.info("🎉 COMPLETE PIPELINE EXECUTION SUCCESSFUL!")
        logger.info("="*80)
        logger.info(f"⏱️  Total Execution Time: {execution_time:.2f} seconds")
        logger.info(f"🌐 Domain: {parsed_query.domain_type}")
        logger.info(f"📝 Data Type: {parsed_query.data_type}")
        logger.info(f"🗣️  Language: {parsed_query.language}")
        logger.info(f"🎯 Requested: {parsed_query.sample_count} samples")
        logger.info(f"✅ Delivered: {final_result['final_dataset']['metadata']['actual_count']} samples")
        logger.info(f"📊 Success Rate: {final_result['final_dataset']['metadata']['completion_rate']}")
        logger.info(f"📁 Final Dataset: storage/{final_summary['output_files']['final_dataset']}")
        
        logger.info("\n🚀 Pipeline Complete - Synthetic Dataset Ready!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        # No need to log the full traceback here as the agent already does.
        # We just want to show the final, clean error message.
        # logger.exception("Full error traceback:")
        # The 'raise' is removed to prevent the full traceback from being printed on exit.
        
if __name__ == "__main__":
    # It's better to catch the exception here to control the final output
    try:
        asyncio.run(main())
    except Exception as e:
        # This ensures that even if the pipeline is halted, we don't get a messy traceback at the end.
        # The error is already logged cleanly by the time we get here.
        pass
