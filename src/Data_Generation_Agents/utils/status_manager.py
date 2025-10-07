from __future__ import annotations
import json
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

from ..config.settings import settings

logger = logging.getLogger(__name__)


STATUS_LEVELS = {
    "initial": 0,
    "query_parsed": 1,
    "query_refined": 2,
    "web_searched": 3,
    "web_scraped": 4,
    "content_gathered": 5,
    "topics_extracted": 6,
    "data_generated": 7,
    "completed": 8,
}

# Node execution order
NODE_SEQUENCE = [
    "query_parser",
    "query_refiner",
    "web_search",
    "filtration",
    "web_scraping",
    "chunking",
    "topic_extraction",
    "synthetic_data_generator"
]


class StatusManager:
    """
    Manages pipeline execution status with node-based progression.
    Saves status and last completed node to a JSON file.
    """
    
    def __init__(self, thread_id: str):
        """
        Initialize status manager for a specific thread.
        
        Args:
            thread_id: Unique identifier (prompt hash)
        """
        self.thread_id = thread_id
        self.status_dir = settings.DATA_PATH / thread_id
        self.status_dir.mkdir(parents=True, exist_ok=True)
        self.status_file = self.status_dir / "pipeline_status.json"
        
        self.status_data = self._load_status()
    
    def _load_status(self) -> dict:
        """Load status from file or return initial status."""
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded status: {data['current_status']} | Last node: {data.get('last_completed_node', 'none')}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load status file: {e}. Starting fresh.")
        
        return {
            "current_status": "initial",
            "level": 0,
            "last_completed_node": None,  # Track last completed node
            "last_updated": datetime.now().isoformat(),
            "checkpoint_metadata": {
                "required_topics": 0,
                "topics_found": 0,
                "last_processed_chunk_index": -1,
                "last_processed_topic_index": -1,
                "synthetic_data_generated_count": 0,
                "retries": 0
            }
        }
    
    def _save_status(self):
        """Save current status to file."""
        self.status_data["last_updated"] = datetime.now().isoformat()
        with open(self.status_file, 'w', encoding='utf-8') as f:
            json.dump(self.status_data, f, ensure_ascii=False, indent=2)
    
    def get_status(self) -> str:
        """Get current status name."""
        return self.status_data["current_status"]
    
    def get_level(self) -> int:
        """Get current status level."""
        return self.status_data["level"]
    
    def get_last_completed_node(self) -> Optional[str]:
        """Get the last successfully completed node name."""
        return self.status_data.get("last_completed_node")
    
    def get_next_node(self) -> Optional[str]:
        """
        Get the next node to execute based on last completed node.
        
        Returns:
            Next node name, or None if completed
        """
        last_node = self.get_last_completed_node()
        
        if last_node is None:
            # Never started - begin from first node
            return NODE_SEQUENCE[0]
        
        if last_node not in NODE_SEQUENCE:
            # Unknown node - restart from beginning
            logger.warning(f"Unknown last node: {last_node}. Restarting from beginning.")
            return NODE_SEQUENCE[0]
        
        # Get next node in sequence
        current_index = NODE_SEQUENCE.index(last_node)
        
        # Check if completed
        if current_index >= len(NODE_SEQUENCE) - 1:
            # Last node was the final one
            if self.is_completed():
                return None
            else:
                # Continue with last node (for incremental processing)
                return NODE_SEQUENCE[-1]
        
        return NODE_SEQUENCE[current_index + 1]
    
    def mark_node_completed(self, node_name: str, new_status: Optional[str] = None):
        """
        Mark a node as completed and optionally update status.
        
        Args:
            node_name: Name of the completed node
            new_status: Optional new status level to set
        """
        self.status_data["last_completed_node"] = node_name
        
        if new_status and new_status in STATUS_LEVELS:
            self.status_data["current_status"] = new_status
            self.status_data["level"] = STATUS_LEVELS[new_status]
        
        self._save_status()
        logger.info(f"✓ Node '{node_name}' marked as completed")
    
    def update_status(self, new_status: str):
        """Update pipeline status."""
        if new_status not in STATUS_LEVELS:
            raise ValueError(f"Invalid status: {new_status}")
        
        new_level = STATUS_LEVELS[new_status]
        
        if new_level >= self.status_data["level"]:
            self.status_data["current_status"] = new_status
            self.status_data["level"] = new_level
            self._save_status()
    
    def is_status_reached(self, status: str) -> bool:
        """Check if pipeline has reached or passed a specific status."""
        if status not in STATUS_LEVELS:
            return False
        return self.get_level() >= STATUS_LEVELS[status]
    
    def reset_to_status(self, status: str):
        """Reset pipeline to a specific status."""
        if status not in STATUS_LEVELS:
            raise ValueError(f"Invalid status: {status}")
        
        self.status_data["current_status"] = status
        self.status_data["level"] = STATUS_LEVELS[status]
        self.status_data["last_completed_node"] = None
        self._save_status()
        logger.info(f"Status reset to: {status}")
    
    def get_checkpoint_metadata(self) -> dict:
        """Get checkpoint metadata."""
        return self.status_data["checkpoint_metadata"]
    
    def update_checkpoint_metadata(self, **kwargs):
        """Update checkpoint metadata fields."""
        self.status_data["checkpoint_metadata"].update(kwargs)
        self._save_status()
    
    def is_completed(self) -> bool:
        """Check if pipeline is completed."""
        return self.get_status() == "completed"
