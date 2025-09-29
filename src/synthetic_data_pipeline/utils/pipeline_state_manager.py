import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional
import logging

from ..config.settings import settings
from ..utils.json_handler import JsonHandler

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

class PipelineStateManager:
    """
    Manages the state of the data generation pipeline, allowing for caching and resuming.
    It uses a main state file to track progress and separate asset files for bulk data.
    """

    def __init__(self, prompt: str):
        # --- FIX: Initialize logger first ---
        self.logger = logging.getLogger(__name__) 
        
        self.prompt = prompt
        self.prompt_hash = self._hash_prompt(prompt)
        
        # Define the prompt-specific directory
        self.prompt_dir = settings.DATA_PATH / self.prompt_hash
        self.prompt_dir.mkdir(parents=True, exist_ok=True)
        
        # Now it is safe to use the logger
        self.logger.info(f"Data will be saved to: {self.prompt_dir}")

        self.state_file_path = self.prompt_dir / "pipeline_state.json"
        self.state: Dict[str, Any] = {}

    def _hash_prompt(self, prompt: str) -> str:
        """Creates a SHA-256 hash of the prompt to use as a unique ID."""
        return hashlib.sha256(prompt.encode('utf-8')).hexdigest()

    def _get_asset_path(self, asset_name: str) -> Path:
        """Constructs the file path for a given data asset."""
        return self.prompt_dir / f"{asset_name}.json"

    def initialize_new_state(self):
        """Initializes a fresh state for a new pipeline run."""
        self.prompt_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists
        self.state = {
            "last_prompt": self.prompt,
            "prompt_hash": self.prompt_hash,
            "status": "initialized", # Set initial status to 'initialized'
            "parsed_query": None,
            "checkpoint": {
                "required_topics": 0,
                "topics_found": 0,
                "last_processed_chunk_index": -1,
                "last_processed_topic_index": -1,
                "synthetic_data_generated_count": 0,
                "retries": 0
            }
        }
        self.save_state()

    def load_state(self) -> bool:
        """
        Loads the pipeline state. If the prompt matches, it resumes. If not, it clears
        the old state and starts fresh.
        Returns True if a valid, matching state was loaded, False otherwise.
        """
        # Check if the current prompt's state file exists
        if self.state_file_path.exists():
            try:
                loaded_state = JsonHandler.load_json(self.state_file_path)
                if loaded_state.get("prompt_hash") == self.prompt_hash:
                    self.logger.info(f"Matching prompt found. Resuming session for hash: {self.prompt_hash}")
                    self.state = loaded_state
                    return True
                else:
                    # This case should ideally not be reached if __init__ sets state_file_path correctly,
                    # but as a safeguard, if a different hash's state is found in the current prompt_dir,
                    # it suggests a previous error or manual tampering. In this scenario, we restart.
                    self.logger.error(f"Found state for different prompt hash in {self.prompt_dir}. Restarting.")
                    self.initialize_new_state()
                    return False
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.error(f"State file is corrupt or invalid in {self.prompt_dir}, starting fresh. Error: {e}")
                self.initialize_new_state()
                return False
        else:
            # If no state file exists for the current prompt hash, don't clear old prompt directories.
            # Just initialize a new state for the current prompt.
            self.logger.info(f"No state file found for prompt hash {self.prompt_hash}. Starting a new session.")
            self.initialize_new_state()
            return False

    def save_state(self):
        """Saves the current state to the main checkpoint file."""
        JsonHandler.save_json(self.state, self.state_file_path)

    def update_status(self, new_status: str):
        """Updates the pipeline's status and saves the state."""
        if self.state.get("status") != new_status:
            self.state["status"] = new_status
            self.save_state()
            self.logger.info(f"Pipeline status updated to: {new_status}")

    def get_status(self) -> str:
        """Retrieves the current pipeline status."""
        return self.state.get("status", "initial")

    def get_status_level(self) -> int:
        """Retrieves the numerical level of the current pipeline status."""
        return STATUS_LEVELS.get(self.get_status(), 0)

    def update_checkpoint(self, **kwargs):
        """Updates one or more values in the checkpoint and saves the state."""
        self.state["checkpoint"].update(kwargs)
        self.save_state()

    def get_checkpoint_value(self, key: str, default: Any = None) -> Any:
        """Retrieves a specific value from the checkpoint."""
        return self.state.get("checkpoint", {}).get(key, default)

    def save_asset(self, asset_name: str, data: Any):
        """Saves a data asset (like topics or chunks) to its own file."""
        asset_path = self._get_asset_path(asset_name)
        JsonHandler.save_json(data, asset_path)

    def load_asset(self, asset_name: str) -> Optional[Any]:
        """Loads a data asset from its file."""
        asset_path = self._get_asset_path(asset_name)
        if asset_path.exists():
            return JsonHandler.load_json(asset_path)
        return None

    def clear_asset(self, asset_name: str):
        """Deletes a specific asset file."""
        asset_path = self._get_asset_path(asset_name)
        if asset_path.exists():
            try:
                asset_path.unlink()
                self.logger.info(f"Cleared asset: {asset_name}")
            except OSError as e:
                self.logger.error(f"Error clearing asset {asset_name} at {asset_path}: {e}")
