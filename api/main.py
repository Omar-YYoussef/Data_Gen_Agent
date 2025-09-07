from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional
import uuid
from datetime import datetime
from pathlib import Path

from workflows.main_workflow import main_workflow
from config.settings import settings
from utils.json_handler import JsonHandler

app = FastAPI(
    title="Web Search & Synthetic Data Pipeline API",
    description="API for generating synthetic datasets from web search",
    version="1.0.0"
)

# Request/Response models
class PipelineRequest(BaseModel):
    query: str
    workflow_id: Optional[str] = None

class PipelineResponse(BaseModel):
    workflow_id: str
    status: str
    message: str

# Global storage for workflow states
active_workflows: Dict[str, Dict[str, Any]] = {}

@app.post("/pipeline/start", response_model=PipelineResponse)
async def start_pipeline(request: PipelineRequest, background_tasks: BackgroundTasks):
    """Start a new pipeline workflow"""
    
    workflow_id = request.workflow_id or f"api_workflow_{uuid.uuid4().hex[:8]}"
    
    if workflow_id in active_workflows:
        raise HTTPException(status_code=400, detail="Workflow ID already exists")
    
    # Initialize workflow tracking
    active_workflows[workflow_id] = {
        "status": "starting",
        "query": request.query,
        "created_at": datetime.now().isoformat(),
        "progress": "Initializing workflow..."
    }
    
    # Start workflow in background
    background_tasks.add_task(run_workflow_background, workflow_id, request.query)
    
    return PipelineResponse(
        workflow_id=workflow_id,
        status="started",
        message=f"Workflow {workflow_id} started successfully"
    )

async def run_workflow_background(workflow_id: str, query: str):
    """Run workflow in background task"""
    
    try:
        # Update progress
        active_workflows[workflow_id]["status"] = "running"
        active_workflows[workflow_id]["progress"] = "Running pipeline stages..."
        
        # Execute workflow
        final_state = main_workflow.run_workflow(query, workflow_id)
        
        # Update with results
        active_workflows[workflow_id] = {
            **active_workflows[workflow_id],
            "status": final_state["status"],
            "final_state": final_state,
            "completed_at": datetime.now().isoformat()
        }
        
        if final_state["status"] == "completed":
            active_workflows[workflow_id]["progress"] = "Pipeline completed successfully"
            active_workflows[workflow_id]["dataset_path"] = f"final_output/collected_datasets/final_dataset_{final_state['parsed_query'].data_type}_{final_state['parsed_query'].domain_type.replace(' ', '_')}.json"
        else:
            active_workflows[workflow_id]["progress"] = f"Pipeline failed: {final_state.get('error_info', {}).get('message', 'Unknown error')}"
        
    except Exception as e:
        active_workflows[workflow_id]["status"] = "crashed"
        active_workflows[workflow_id]["progress"] = f"Pipeline crashed: {str(e)}"

@app.get("/pipeline/status/{workflow_id}")
async def get_workflow_status(workflow_id: str):
    """Get workflow status and progress"""
    
    if workflow_id not in active_workflows:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    workflow_info = active_workflows[workflow_id]
    
    response = {
        "workflow_id": workflow_id,
        "status": workflow_info["status"],
        "progress": workflow_info["progress"],
        "created_at": workflow_info["created_at"]
    }
    
    # Add completion info if available
    if "completed_at" in workflow_info:
        response["completed_at"] = workflow_info["completed_at"]
    
    # Add stage timings if available
    if "final_state" in workflow_info:
        response["stage_timings"] = workflow_info["final_state"].get("stage_timings", {})
        
        if workflow_info["status"] == "completed":
            final_dataset = workflow_info["final_state"].get("final_dataset", {}).get("final_dataset", {})
            response["results"] = {
                "delivered_samples": final_dataset.get("metadata", {}).get("actual_count", 0),
                "completion_rate": final_dataset.get("metadata", {}).get("completion_rate", "0%"),
                "dataset_ready": True
            }
    
    return response

@app.get("/pipeline/download/{workflow_id}")
async def download_dataset(workflow_id: str):
    """Download the generated dataset"""
    
    if workflow_id not in active_workflows:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    workflow_info = active_workflows[workflow_id]
    
    if workflow_info["status"] != "completed":
        raise HTTPException(status_code=400, detail="Workflow not completed")
    
    if "dataset_path" not in workflow_info:
        raise HTTPException(status_code=404, detail="Dataset file not found")
    
    dataset_file = settings.STORAGE_ROOT / workflow_info["dataset_path"]
    
    if not dataset_file.exists():
        raise HTTPException(status_code=404, detail="Dataset file does not exist")
    
    return FileResponse(
        path=dataset_file,
        media_type='application/json',
        filename=f"synthetic_dataset_{workflow_id}.json"
    )

@app.get("/pipeline/list")
async def list_workflows():
    """List all workflows"""
    
    workflows = []
    for workflow_id, info in active_workflows.items():
        workflows.append({
            "workflow_id": workflow_id,
            "status": info["status"],
            "query": info["query"],
            "created_at": info["created_at"],
            "completed_at": info.get("completed_at")
        })
    
    return {"workflows": workflows}

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Web Search & Synthetic Data Pipeline API",
        "version": "1.0.0",
        "endpoints": {
            "start_pipeline": "/pipeline/start",
            "get_status": "/pipeline/status/{workflow_id}",
            "download_dataset": "/pipeline/download/{workflow_id}",
            "list_workflows": "/pipeline/list"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
