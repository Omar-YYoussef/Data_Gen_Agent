from __future__ import annotations
from langgraph.graph import StateGraph, START, END

from .state import PipelineState
from .nodes import (
    query_parser_node,
    query_refiner_node,
    web_search_node,
    filtration_node,
    web_scraping_node,
    chunking_node,
    topic_extraction_node,
    synthetic_data_generator_node,
    check_topics_sufficient,
    check_sample_count_met
)
from ..utils.status_manager import StatusManager


def route_from_start(state: PipelineState) -> str:
    """
    Determine which node to execute based on status file.
    Called at graph START and after simple node completions.
    """
    thread_id = state.get("status_manager_thread_id")
    status_manager = StatusManager(thread_id)
    
    next_node = status_manager.get_next_node()
    
    if next_node:
        return next_node
    else:
        return END


def build_pipeline_graph() -> StateGraph:
    """
    Build the LangGraph pipeline with direct conditional routing.
    
    Returns:
        Compiled StateGraph ready for execution
    """
    builder = StateGraph(PipelineState)
    
    # Add all task nodes
    builder.add_node("query_parser", query_parser_node)
    builder.add_node("query_refiner", query_refiner_node)
    builder.add_node("web_search", web_search_node)
    builder.add_node("filtration", filtration_node)
    builder.add_node("web_scraping", web_scraping_node)
    builder.add_node("chunking", chunking_node)
    builder.add_node("topic_extraction", topic_extraction_node)
    builder.add_node("synthetic_data_generator", synthetic_data_generator_node)
    
    # Set entry point with conditional routing
    builder.add_conditional_edges(
        START,
        route_from_start,
        {
            "query_parser": "query_parser",
            "query_refiner": "query_refiner",
            "web_search": "web_search",
            "filtration": "filtration",
            "web_scraping": "web_scraping",
            "chunking": "chunking",
            "topic_extraction": "topic_extraction",
            "synthetic_data_generator": "synthetic_data_generator",
            END: END
        }
    )
    
    # Simple nodes: execute and route based on status
    for node_name in ["query_parser", "query_refiner", "web_search", "filtration", "web_scraping", "chunking"]:
        builder.add_conditional_edges(
            node_name,
            route_from_start,
            {
                "query_parser": "query_parser",
                "query_refiner": "query_refiner",
                "web_search": "web_search",
                "filtration": "filtration",
                "web_scraping": "web_scraping",
                "chunking": "chunking",
                "topic_extraction": "topic_extraction",
                "synthetic_data_generator": "synthetic_data_generator",
                END: END
            }
        )
    
    # Topic extraction: Check if sufficient
    builder.add_conditional_edges(
        "topic_extraction",
        check_topics_sufficient,
        {
            "continue_extraction": "topic_extraction",
            "sufficient": "synthetic_data_generator",  # Direct transition
            "regather_content": "query_refiner"  # Restart
        }
    )
    
    # Data generation: Check if complete
    builder.add_conditional_edges(
        "synthetic_data_generator",
        check_sample_count_met,
        {
            "continue_generation": "synthetic_data_generator",
            "complete": END
        }
    )
    
    # Compile graph
    graph = builder.compile()
    
    return graph
