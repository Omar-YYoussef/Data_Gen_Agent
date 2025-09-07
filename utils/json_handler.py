import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class JsonHandler:
    """Enhanced utility class for JSON file operations"""
    
    @staticmethod
    def save_json(data: Any, file_path: Path, indent: int = 2) -> bool:
        """Save data to JSON file with enhanced error handling"""
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
            
            logger.debug(f"JSON saved to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save JSON to {file_path}: {e}")
            return False
    
    @staticmethod
    def load_json(file_path: Path) -> Optional[Any]:
        """Load data from JSON file with error handling"""
        try:
            if not file_path.exists():
                logger.warning(f"JSON file does not exist: {file_path}")
                return None
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.debug(f"JSON loaded from {file_path}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to load JSON from {file_path}: {e}")
            return None
    
    @staticmethod
    def append_to_json_array(data: Any, file_path: Path) -> bool:
        """Append data to existing JSON array file"""
        try:
            existing_data = JsonHandler.load_json(file_path)
            
            if existing_data is None:
                existing_data = []
            
            if not isinstance(existing_data, list):
                existing_data = [existing_data]
            
            if isinstance(data, list):
                existing_data.extend(data)
            else:
                existing_data.append(data)
            
            return JsonHandler.save_json(existing_data, file_path)
            
        except Exception as e:
            logger.error(f"Failed to append to JSON array {file_path}: {e}")
            return False
    
    @staticmethod
    def save_with_metadata(data: Any, file_path: Path, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Save JSON with metadata wrapper"""
        wrapper = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "file_path": str(file_path),
                **(metadata or {})
            },
            "data": data
        }
        return JsonHandler.save_json(wrapper, file_path)

json_handler = JsonHandler()
