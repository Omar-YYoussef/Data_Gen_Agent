import uuid
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from langgraph.graph import StateGraph, END
from langchain_core.runnables import RunnableLambda
import json
import traceback

from config.settings import settings
from models.data_schemas import ParsedQuery, WorkflowState
from agents.query_parser_agent import QueryParserAgent
from agents.query_refiner_agent import QueryRefinerAgent
from agents.web_search_agent import WebSearchAgent
from agents.filtration_agent import FiltrationAgent
from workflows.parallel_processing import parallel_orchestrator
from utils.json_handler import JsonHandler

class PipelineState(Dict):
    """State object for LangGraph workflow"""
    workflow_id: str
    current_stage: str
    user_query: str
    parsed_query: Optional[ParsedQuery]
    refined_queries: List
    search_results: List
    filtered_results: List
    scraped_data: List
    extracted_topics: List
    synthetic_data: List
    final_dataset: Dict
    error_info: Optional[Dict]
    stage_timings: Dict
    status: str

class WebSearchSyntheticDataWorkflow:
    """Main LangGraph workflow orchestrator for the complete pipeline"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.workflow_graph = self._build_workflow()
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow"""
        
        # Define workflow graph
        graph = StateGraph(PipelineState)
        
        # Add nodes for each stage
        graph.add_node("parse_query", self._parse_query_node)
        graph.add_node("refine_queries", self._refine_queries_node)
        graph.add_node("web_search", self._web_search_node)
        graph.add_node("filter_results", self._filter_results_node)
        graph.add_node("scrape_content", self._scrape_content_node)
        graph.add_node("extract_topics", self._extract_topics_node)
        graph.add_node("check_topic_coverage", self._check_topic_coverage_node)
        graph.add_node("generate_synthetic_data", self._generate_synthetic_data_node)
        graph.add_node("collect_final_data", self._collect_final_data_node)
        graph.add_node("handle_error", self._handle_error_node)
        
        # Define edges (workflow flow)
        graph.set_entry_point("parse_query")
        
        graph.add_edge("parse_query", "refine_queries")
        graph.add_edge("refine_queries", "web_search")
        graph.add_edge("web_search", "filter_results")
        graph.add_edge("filter_results", "scrape_content")
        graph.add_edge("scrape_content", "extract_topics")
        graph.add_edge("extract_topics", "check_topic_coverage")
        
        # Conditional edge for topic coverage
        graph.add_conditional_edges(
            "check_topic_coverage",
            self._should_continue_or_expand,
            {
                "continue": "generate_synthetic_data",
                "expand": "refine_queries",  # Loop back to get more topics
                "error": "handle_error"
            }
        )
        
        graph.add_edge("generate_synthetic_data", "collect_final_data")
        graph.add_edge("collect_final_data", END)
        graph.add_edge("handle_error", END)
        
        return graph.compile()
    
    def _log_stage_start(self, state: PipelineState, stage_name: str):
        """Log stage start and update state"""
        state["current_stage"] = stage_name
        state["stage_timings"][stage_name] = {"start": datetime.now().isoformat()}
        self.logger.info(f"Workflow {state['workflow_id']} - Starting stage: {stage_name}")
    
    def _log_stage_end(self, state: PipelineState, stage_name: str, success: bool = True):
        """Log stage completion"""
        state["stage_timings"][stage_name]["end"] = datetime.now().isoformat()
        start_time = datetime.fromisoformat(state["stage_timings"][stage_name]["start"])
        end_time = datetime.fromisoformat(state["stage_timings"][stage_name]["end"])
        duration = (end_time - start_time).total_seconds()
        state["stage_timings"][stage_name]["duration_seconds"] = duration
        
        status = "✅ COMPLETED" if success else "❌ FAILED"
        self.logger.info(f"Workflow {state['workflow_id']} - {status} {stage_name} in {duration:.2f}s")
    
    def _parse_query_node(self, state: PipelineState) -> PipelineState:
        """Parse user query node"""
        self._log_stage_start(state, "parse_query")
        
        try:
            agent = QueryParserAgent()
            parsed_query = agent.run(state["user_query"])
            state["parsed_query"] = parsed_query
            
            self.logger.info(f"Parsed query - Domain: {parsed_query.domain_type}, "
                           f"Type: {parsed_query.data_type}, "
                           f"Count: {parsed_query.sample_count}, "
                           f"Language: {parsed_query.language}")
            
            self._log_stage_end(state, "parse_query", True)
            
        except Exception as e:
            self._handle_stage_error(state, "parse_query", e)
        
        return state
    
    def _refine_queries_node(self, state: PipelineState) -> PipelineState:
        """Refine queries node"""
        self._log_stage_start(state, "refine_queries")
        
        try:
            agent = QueryRefinerAgent()
            refined_queries = agent.run(state["parsed_query"])
            state["refined_queries"] = refined_queries
            
            self.logger.info(f"Generated {len(refined_queries)} refined queries")
            self._log_stage_end(state, "refine_queries", True)
            
        except Exception as e:
            self._handle_stage_error(state, "refine_queries", e)
        
        return state
    
    def _web_search_node(self, state: PipelineState) -> PipelineState:
        """Web search node"""
        self._log_stage_start(state, "web_search")
        
        try:
            agent = WebSearchAgent()
            search_results = agent.run(state["refined_queries"])
            state["search_results"] = search_results
            
            self.logger.info(f"Found {len(search_results)} search results")
            self._log_stage_end(state, "web_search", True)
            
        except Exception as e:
            self._handle_stage_error(state, "web_search", e)
        
        return state
    
    def _filter_results_node(self, state: PipelineState) -> PipelineState:
        """Filter results node"""
        self._log_stage_start(state, "filter_results")
        
        try:
            agent = FiltrationAgent()
            filtered_results = agent.run(state["search_results"])
            state["filtered_results"] = filtered_results
            
            self.logger.info(f"Filtered to {len(filtered_results)} unique URLs")
            self._log_stage_end(state, "filter_results", True)
            
        except Exception as e:
            self._handle_stage_error(state, "filter_results", e)
        
        return state
    
    def _scrape_content_node(self, state: PipelineState) -> PipelineState:
        """Scrape content node"""
        self._log_stage_start(state, "scrape_content")
        
        try:
            scraped_data = parallel_orchestrator.run_web_scraping(state["filtered_results"])
            state["scraped_data"] = scraped_data
            
            successful_scrapes = len([d for d in scraped_data if d.get("success", False)])
            self.logger.info(f"Successfully scraped {successful_scrapes}/{len(scraped_data)} pages")
            self._log_stage_end(state, "scrape_content", True)
            
        except Exception as e:
            self._handle_stage_error(state, "scrape_content", e)
        
        return state
    
    def _extract_topics_node(self, state: PipelineState) -> PipelineState:
        """Extract topics node"""
        self._log_stage_start(state, "extract_topics")
        
        try:
            extracted_topics = parallel_orchestrator.run_parallel_topic_extraction(state["scraped_data"])
            state["extracted_topics"] = extracted_topics
            
            self.logger.info(f"Extracted {len(extracted_topics)} unique topics")
            self._log_stage_end(state, "extract_topics", True)
            
        except Exception as e:
            self._handle_stage_error(state, "extract_topics", e)
        
        return state
    
    def _check_topic_coverage_node(self, state: PipelineState) -> PipelineState:
        """Check if we have enough topics for the requested sample count"""
        self._log_stage_start(state, "check_topic_coverage")
        
        try:
            required_topics = state["parsed_query"].calculate_required_subtopics()
            available_topics = len(state["extracted_topics"])
            
            state["topic_coverage"] = {
                "required": required_topics,
                "available": available_topics,
                "sufficient": available_topics >= required_topics,
                "coverage_ratio": available_topics / required_topics if required_topics > 0 else 1.0
            }
            
            self.logger.info(f"Topic coverage: {available_topics}/{required_topics} "
                           f"({(available_topics/required_topics)*100:.1f}%)")
            
            self._log_stage_end(state, "check_topic_coverage", True)
            
        except Exception as e:
            self._handle_stage_error(state, "check_topic_coverage", e)
        
        return state
    
    def _should_continue_or_expand(self, state: PipelineState) -> str:
        """Decide whether to continue with synthesis or expand topic search"""
        
        if state.get("error_info"):
            return "error"
        
        coverage = state.get("topic_coverage", {})
        
        # If we have sufficient topics, continue
        if coverage.get("sufficient", False):
            return "continue"
        
        # If coverage is very low, try to expand (but limit retries)
        retry_count = state.get("expansion_retries", 0)
        if retry_count < 2 and coverage.get("coverage_ratio", 0) < 0.5:
            state["expansion_retries"] = retry_count + 1
            self.logger.info(f"Insufficient topic coverage, expanding search (attempt {retry_count + 1})")
            return "expand"
        
        # Otherwise continue with what we have
        self.logger.warning(f"Proceeding with insufficient topics: {coverage}")
        return "continue"
    
    def _generate_synthetic_data_node(self, state: PipelineState) -> PipelineState:
        """Generate synthetic data node"""
        self._log_stage_start(state, "generate_synthetic_data")
        
        try:
            synthetic_data_lists = parallel_orchestrator.run_parallel_synthetic_generation(
                state["extracted_topics"], 
                state["parsed_query"]
            )
            state["synthetic_data"] = synthetic_data_lists
            
            total_generated = sum(len(data_list) for data_list in synthetic_data_lists)
            self.logger.info(f"Generated {total_generated} synthetic data points across 3 agents")
            self._log_stage_end(state, "generate_synthetic_data", True)
            
        except Exception as e:
            self._handle_stage_error(state, "generate_synthetic_data", e)
        
        return state
    
    def _collect_final_data_node(self, state: PipelineState) -> PipelineState:
        """Collect and finalize dataset node"""
        self._log_stage_start(state, "collect_final_data")
        
        try:
            final_result = parallel_orchestrator.run_data_collection(
                state["synthetic_data"], 
                state["parsed_query"]
            )
            state["final_dataset"] = final_result
            state["status"] = "completed"
            
            delivered_samples = final_result["final_dataset"]["metadata"]["actual_count"]
            self.logger.info(f"Final dataset complete with {delivered_samples} samples")
            self._log_stage_end(state, "collect_final_data", True)
            
        except Exception as e:
            self._handle_stage_error(state, "collect_final_data", e)
        
        return state
    
    def _handle_error_node(self, state: PipelineState) -> PipelineState:
        """Handle workflow errors"""
        state["status"] = "failed"
        error_info = state.get("error_info", {})
        
        self.logger.error(f"Workflow {state['workflow_id']} failed at stage {error_info.get('stage', 'unknown')}")
        self.logger.error(f"Error: {error_info.get('message', 'Unknown error')}")
        
        return state
    
    def _handle_stage_error(self, state: PipelineState, stage_name: str, error: Exception):
        """Handle stage-specific errors"""
        error_info = {
            "stage": stage_name,
            "message": str(error),
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now().isoformat()
        }
        
        state["error_info"] = error_info
        state["status"] = "failed"
        
        self._log_stage_end(state, stage_name, False)
        self.logger.exception(f"Stage {stage_name} failed: {error}")
    
    def run_workflow(self, user_query: str, workflow_id: Optional[str] = None) -> Dict[str, Any]:
        """Run the complete workflow"""
        
        if not workflow_id:
            workflow_id = f"workflow_{uuid.uuid4().hex[:8]}"
        
        # Initialize state
        initial_state = PipelineState({
            "workflow_id": workflow_id,
            "current_stage": "initialized",
            "user_query": user_query,
            "parsed_query": None,
            "refined_queries": [],
            "search_results": [],
            "filtered_results": [],
            "scraped_data": [],
            "extracted_topics": [],
            "synthetic_data": [],
            "final_dataset": {},
            "error_info": None,
            "stage_timings": {},
            "status": "running",
            "expansion_retries": 0
        })
        
        self.logger.info(f"Starting workflow {workflow_id} with query: {user_query}")
        
        try:
            # Run the workflow
            final_state = self.workflow_graph.invoke(initial_state)
            
            # Save workflow state for monitoring
            self._save_workflow_state(final_state)
            
            return final_state
            
        except Exception as e:
            self.logger.exception(f"Workflow {workflow_id} crashed: {e}")
            initial_state["status"] = "crashed"
            initial_state["error_info"] = {
                "stage": "workflow_execution",
                "message": str(e),
                "traceback": traceback.format_exc()
            }
            return initial_state
    
    def _save_workflow_state(self, state: PipelineState):
        """Save workflow state for monitoring and debugging"""
        
        # Create summary for storage
        workflow_summary = {
            "workflow_id": state["workflow_id"],
            "status": state["status"],
            "user_query": state["user_query"],
            "parsed_query": {
                "domain_type": state["parsed_query"].domain_type if state["parsed_query"] else None,
                "data_type": state["parsed_query"].data_type if state["parsed_query"] else None,
                "language": state["parsed_query"].language if state["parsed_query"] else None,
                "sample_count": state["parsed_query"].sample_count if state["parsed_query"] else None
            },
            "stage_timings": state["stage_timings"],
            "topic_coverage": state.get("topic_coverage", {}),
            "final_metrics": {
                "total_topics": len(state["extracted_topics"]),
                "delivered_samples": state["final_dataset"]["final_dataset"]["metadata"]["actual_count"] if state.get("final_dataset") else 0
            } if state["status"] == "completed" else None,
            "error_info": state.get("error_info"),
            "timestamp": datetime.now().isoformat()
        }
        
        # Save to workflow logs
        JsonHandler.save_json(
            workflow_summary,
            settings.WORKFLOW_LOGS_PATH / f"workflow_{state['workflow_id']}.json"
        )

# Initialize workflow
main_workflow = WebSearchSyntheticDataWorkflow()
