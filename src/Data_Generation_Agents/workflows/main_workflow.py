from __future__ import annotations
import logging
import hashlib
from datetime import datetime
from typing import Optional

from ..langgraph_integration.graph_builder import build_pipeline_graph
from ..utils.data_saver import DataSaver
from ..utils.status_manager import StatusManager
from ..config.settings import settings
from ..services.gemini_service import PipelineStopRequested

logger = logging.getLogger(__name__)


def _hash_prompt(prompt: str) -> str:
    """Create SHA-256 hash of prompt for thread_id."""
    return hashlib.sha256(prompt.encode('utf-8')).hexdigest()


async def run_pipeline(
    user_query: str,
    refined_queries_count: Optional[int] = None,
    search_results_per_query: Optional[int] = None,
    rows_per_subtopic: Optional[int] = None,
    gemini_model_name: Optional[str] = None
) -> None:
    """Run the synthetic data generation pipeline using LangGraph."""
    
    _refined_queries_count = refined_queries_count or settings.REFINED_QUERIES_COUNT
    _search_results_per_query = search_results_per_query or settings.SEARCH_RESULTS_PER_QUERY
    _rows_per_subtopic = rows_per_subtopic or settings.ROWS_PER_SUBTOPIC
    
    logger.info("="*80)
    logger.info("Synthetic Data Generation Pipeline (LangGraph)")
    logger.info("="*80)
    logger.info(f"Query: {user_query[:100]}...")
    logger.info(f"Config: Queries={_refined_queries_count}, Results/Query={_search_results_per_query}, Rows/Topic={_rows_per_subtopic}")
    logger.info("="*80)
    
    pipeline_start_time = datetime.now()
    thread_id = _hash_prompt(user_query)
    logger.info(f"Thread ID: {thread_id[:16]}...")
    
    status_manager = StatusManager(thread_id)
    data_saver = DataSaver(thread_id)
    
    # Check if already completed
    if status_manager.is_completed():
        logger.info("="*80)
        logger.info("🎉 PIPELINE ALREADY COMPLETED!")
        logger.info("="*80)
        logger.info(f"Data location: {data_saver.get_data_dir()}")
        logger.info("="*80)
        return
    
    # Check if resuming
    last_node = status_manager.get_last_completed_node()
    next_node = status_manager.get_next_node()
    
    if last_node:
        logger.info("="*80)
        logger.info("RESUMING PIPELINE")
        logger.info("="*80)
        logger.info(f"Last completed: {last_node}")
        logger.info(f"Next node: {next_node}")
        logger.info("="*80)
    else:
        logger.info("="*80)
        logger.info("STARTING NEW PIPELINE")
        logger.info("="*80)
    
    # Prepare initial input
    initial_input = {
        "user_query": user_query,
        "config_params": {
            "refined_queries_count": _refined_queries_count,
            "search_results_per_query": _search_results_per_query,
            "rows_per_subtopic": _rows_per_subtopic
        },
        "status_manager_thread_id": thread_id
    }
    
    # Build graph
    logger.info("Building graph...")
    graph = build_pipeline_graph()
    logger.info("Graph built successfully")
    
    # Create configuration
    config = {
        "configurable": {
            "thread_id": thread_id,
            "gemini_model_name": gemini_model_name
        },
        "recursion_limit": 1000000000000000000000000 
    }
    
    try:
        logger.info("Starting graph execution...\n")
        
        # Stream execution
        async for event in graph.astream(initial_input, config, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_output:
                    logger.info(f"✓ Node '{node_name}' completed")
        
        # Normal completion
        pipeline_end_time = datetime.now()
        execution_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("🎉 PIPELINE EXECUTION FINISHED!")
        logger.info("="*80)
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info(f"Data location: {data_saver.get_data_dir()}")
        logger.info(f"Thread ID: {thread_id}")
        logger.info("="*80)
    
    except PipelineStopRequested as e:
        # Clean stop - quota exhausted (no traceback)
        pipeline_end_time = datetime.now()
        execution_time = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("⚠ PIPELINE STOPPED - API QUOTA EXHAUSTED")
        logger.info("="*80)
        logger.info(f"Stage: {e.stage}")
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info(f"Progress saved at: {status_manager.get_last_completed_node() or 'initial'}")
        logger.info(f"Resume by running the same command after quota resets.")
        logger.info(f"Data location: {data_saver.get_data_dir()}")
        logger.info(f"Thread ID: {thread_id}")
        logger.info("="*80)
        # Exit cleanly without re-raising
        return
        
    except KeyboardInterrupt:
        logger.info("\n" + "="*80)
        logger.info("⚠ PIPELINE INTERRUPTED BY USER")
        logger.info("="*80)
        logger.info(f"Progress saved at: {status_manager.get_last_completed_node() or 'initial'}")
        logger.info(f"Resume by running the same command again.")
        logger.info(f"Thread ID: {thread_id}")
        logger.info("="*80)
        # Exit cleanly
        return
        
    except Exception as e:
        # Unexpected errors only
        logger.critical("\n" + "="*80)
        logger.critical("❌ UNEXPECTED PIPELINE ERROR")
        logger.critical("="*80)
        logger.critical(f"Error: {str(e)}")
        logger.critical(f"Status preserved at: {status_manager.get_last_completed_node() or 'initial'}")
        logger.critical(f"Resume by running the same command again.")
        logger.critical(f"Thread ID: {thread_id}")
        logger.critical("="*80)
        # Don't show full traceback to user
        logger.debug("Full traceback:", exc_info=True)  # Only in DEBUG mode
        return
