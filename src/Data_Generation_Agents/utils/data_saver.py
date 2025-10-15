from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Optional
from datetime import datetime
import logging

from ..config.settings import settings

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DataSaver:
    """
    Saves agent outputs to JSON files.
    Supports Arabic with UTF-8 encoding and datetime serialization.
    """
    
    def __init__(self, thread_id: str):
        """
        Initialize data saver for a specific thread.
        
        Args:
            thread_id: Unique identifier (prompt hash)
        """
        self.thread_id = thread_id
        self.data_dir = settings.DATA_PATH / thread_id
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Data will be saved to: {self.data_dir}")
    
    def save_agent_output(self, agent_name: str, data: Any) -> Path:
        """
        Save agent output to a JSON file (overwrites existing file).
        
        Args:
            agent_name: Name of the agent (e.g., 'refined_queries')
            data: Data to save (will be JSON serialized)
            
        Returns:
            Path to saved file
        """
        file_path = self.data_dir / f"{agent_name}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
        
        logger.info(f"Saved {agent_name} to {file_path}")
        return file_path
    
    def append_to_json_array(self, agent_name: str, new_items: list) -> Path:
        """
        Append items to JSON array file.
        Loads existing array, appends new items, saves back.
        
        Args:
            agent_name: Name of the agent (e.g., 'synthetic_data')
            new_items: List of new items to append
            
        Returns:
            Path to saved file
        """
        file_path = self.data_dir / f"{agent_name}.json"
        
        # Load existing data
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []
        
        # Append new items
        existing_data.extend(new_items)
        
        # Save back
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
        
        logger.info(f"Appended {len(new_items)} items to {agent_name}.json (total: {len(existing_data)})")
        return file_path

    def delete_agent_output(self, agent_name: str) -> bool:
        """
        Delete agent output JSON file.
        
        Args:
            agent_name: Name of the agent (e.g., 'refined_queries')
            
        Returns:
            True if file was deleted, False if file didn't exist
        """
        file_path = self.data_dir / f"{agent_name}.json"
        
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Deleted {agent_name}.json from {self.data_dir}")
            return True
        else:
            logger.debug(f"File {agent_name}.json does not exist, skipping deletion")
            return False

    
    def load_agent_output(self, agent_name: str) -> Optional[Any]:
        """
        Load agent output from JSON file.
        
        Args:
            agent_name: Name of the agent
            
        Returns:
            Loaded data or None if file doesn't exist
        """
        file_path = self.data_dir / f"{agent_name}.json"
        if not file_path.exists():
            return None
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def count_items_in_array(self, agent_name: str) -> int:
        """
        Count items in JSON array file.
        
        Args:
            agent_name: Name of the agent (e.g., 'synthetic_data')
            
        Returns:
            Number of items in array
        """
        data = self.load_agent_output(agent_name)
        if data and isinstance(data, list):
            return len(data)
        return 0
    
    def get_data_dir(self) -> Path:
        """Get the data directory path."""
        return self.data_dir
