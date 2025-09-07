import gradio as gr
from fastapi import FastAPI
import uvicorn
from pathlib import Path
import json
import subprocess
import sys
import threading
import queue

# --- FastAPI App ---
app = FastAPI()

# --- Pipeline Execution Logic ---

def run_pipeline_in_subprocess(query: str, output_queue: queue.Queue):
    """
    Runs the main.py script as a subprocess and captures its output.
    """
    try:
        # Ensure the correct python executable is used, especially in virtual environments
        python_executable = sys.executable
        process = subprocess.Popen(
            [python_executable, "main.py", query],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            bufsize=1
        )
        
        # Stream output
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                output_queue.put(line)
        
        process.wait()
        
        # After process completion, find the summary file
        summary_path = Path("storage/final_output/complete_pipeline_summary.json")
        if summary_path.exists():
            with open(summary_path, 'r', encoding='utf-8') as f:
                summary_content = json.load(f)
            pretty_summary = json.dumps(summary_content, indent=2)
            output_queue.put("\n--- PIPELINE COMPLETE ---")
            output_queue.put(pretty_summary)
        else:
            output_queue.put("\n--- PIPELINE FAILED ---")
            output_queue.put("Could not find the final summary file.")

    except Exception as e:
        output_queue.put(f"\n--- SUBPROCESS ERROR ---")
        output_queue.put(str(e))
    finally:
        output_queue.put(None) # Signal that the process is finished

def run_pipeline_and_stream_logs(query: str):
    """
    Generator function that runs the pipeline and yields log updates.
    """
    if not query:
        yield "Please enter a query to start the pipeline.", None
        return

    output_queue = queue.Queue()
    
    thread = threading.Thread(target=run_pipeline_in_subprocess, args=(query, output_queue))
    thread.start()
    
    full_log = ""
    while True:
        line = output_queue.get()
        if line is None:
            break
        full_log += line
        yield full_log, None # Yield the accumulated log, no file yet
    
    thread.join()
    
    # Final yield with the complete log and file path
    summary_path = Path("storage/final_output/complete_pipeline_summary.json")
    file_path_str = str(summary_path.resolve()) if summary_path.exists() else None
    yield full_log, file_path_str


# --- Gradio Interface ---

with gr.Blocks() as demo:
    gr.Markdown("# Web Data Generation Pipeline")
    gr.Markdown("Enter a query to start the data generation process. The pipeline will run in the background, and logs will be streamed below.")
    
    with gr.Row():
        query_input = gr.Textbox(label="Enter your query", placeholder="e.g., 'Generate 100 samples of medical articles about cancer treatment in Spanish'")
    
    submit_button = gr.Button("Start Pipeline")
    
    with gr.Column():
        log_output = gr.Textbox(label="Pipeline Logs", interactive=False, lines=20, autoscroll=True)
        final_file = gr.File(label="Download Final Summary")

    submit_button.click(
        fn=run_pipeline_and_stream_logs,
        inputs=query_input,
        outputs=[log_output, final_file]
    )

# Mount Gradio app on FastAPI
app = gr.mount_gradio_app(app, demo, path="/")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)
