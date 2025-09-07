import click
import json
import time
from typing import Optional
from datetime import datetime

from workflows.main_workflow import main_workflow
from config.settings import settings
from utils.json_handler import JsonHandler

@click.group()
def cli():
    """Web Search & Synthetic Data Pipeline CLI"""
    pass

@cli.command()
@click.argument('query')
@click.option('--workflow-id', '-w', help='Custom workflow ID')
@click.option('--output', '-o', help='Output file path')
@click.option('--monitor', '-m', is_flag=True, help='Monitor progress in real-time')
def run(query: str, workflow_id: Optional[str], output: Optional[str], monitor: bool):
    """Run the complete pipeline with a user query"""
    
    click.echo("🚀 Starting Web Search & Synthetic Data Pipeline")
    click.echo(f"📝 Query: {query}")
    
    start_time = datetime.now()
    
    try:
        if monitor:
            click.echo("📊 Real-time monitoring enabled")
        
        # Run workflow
        final_state = main_workflow.run_workflow(query, workflow_id)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Display results
        click.echo("\n" + "="*60)
        
        if final_state["status"] == "completed":
            click.echo("✅ Pipeline completed successfully!")
            
            # Show metrics
            parsed_query = final_state["parsed_query"]
            final_dataset = final_state["final_dataset"]["final_dataset"]
            
            click.echo(f"🌐 Domain: {parsed_query.domain_type}")
            click.echo(f"📝 Data Type: {parsed_query.data_type}")
            click.echo(f"🗣️  Language: {parsed_query.language}")
            click.echo(f"🎯 Requested: {parsed_query.sample_count} samples")
            click.echo(f"✅ Delivered: {final_dataset['metadata']['actual_count']} samples")
            click.echo(f"📊 Success Rate: {final_dataset['metadata']['completion_rate']}")
            click.echo(f"⏱️  Total Time: {duration:.2f} seconds")
            
            # Save output if specified
            if output:
                JsonHandler.save_json(final_dataset, Path(output))
                click.echo(f"💾 Dataset saved to: {output}")
            else:
                dataset_path = settings.FINAL_OUTPUT_PATH / "collected_datasets" / f"final_dataset_{parsed_query.data_type}_{parsed_query.domain_type.replace(' ', '_')}.json"
                click.echo(f"💾 Dataset saved to: {dataset_path}")
        
        else:
            click.echo("❌ Pipeline failed!")
            error_info = final_state.get("error_info", {})
            click.echo(f"🚫 Error: {error_info.get('message', 'Unknown error')}")
            click.echo(f"📍 Failed at stage: {error_info.get('stage', 'unknown')}")
        
    except Exception as e:
        click.echo(f"💥 Pipeline crashed: {str(e)}")
        click.echo("Check logs for detailed error information")

@cli.command()
@click.argument('workflow_id')
def status(workflow_id: str):
    """Check the status of a workflow"""
    
    # Load workflow state from logs
    workflow_log_file = settings.WORKFLOW_LOGS_PATH / f"workflow_{workflow_id}.json"
    
    if not workflow_log_file.exists():
        click.echo(f"❌ Workflow {workflow_id} not found")
        return
    
    workflow_state = JsonHandler.load_json(workflow_log_file)
    
    click.echo(f"📊 Workflow Status: {workflow_id}")
    click.echo(f"Status: {workflow_state['status']}")
    click.echo(f"Query: {workflow_state['user_query']}")
    
    if workflow_state['status'] == 'completed':
        click.echo(f"✅ Completed successfully")
        if 'final_metrics' in workflow_state:
            metrics = workflow_state['final_metrics']
            click.echo(f"📊 Topics extracted: {metrics['total_topics']}")
            click.echo(f"📝 Samples delivered: {metrics['delivered_samples']}")
    elif workflow_state['status'] == 'failed':
        click.echo(f"❌ Failed: {workflow_state['error_info']['message']}")

@cli.command()
def list():
    """List all workflow runs"""
    
    workflow_logs = list(settings.WORKFLOW_LOGS_PATH.glob("workflow_*.json"))
    
    if not workflow_logs:
        click.echo("No workflows found")
        return
    
    click.echo("📋 Recent Workflows:")
    click.echo("-" * 80)
    
    for log_file in sorted(workflow_logs, key=lambda x: x.stat().st_mtime, reverse=True):
        workflow_state = JsonHandler.load_json(log_file)
        
        status_icon = {
            'completed': '✅',
            'failed': '❌', 
            'running': '🔄',
            'crashed': '💥'
        }.get(workflow_state['status'], '❓')
        
        click.echo(f"{status_icon} {workflow_state['workflow_id']}")
        click.echo(f"   Query: {workflow_state['user_query'][:60]}...")
        click.echo(f"   Status: {workflow_state['status']}")
        click.echo(f"   Time: {workflow_state['timestamp']}")
        click.echo()

@cli.command()
def serve():
    """Start the API server"""
    click.echo("🚀 Starting API server on http://localhost:8000")
    
    try:
        import uvicorn
        from api.main import app
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except ImportError:
        click.echo("❌ FastAPI/Uvicorn not installed. Install with: pip install fastapi uvicorn")

if __name__ == "__main__":
    cli()
