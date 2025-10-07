from __future__ import annotations
from typing import Any, Optional
from functools import wraps
import logging

from .data_saver import DataSaver
from .status_manager import StatusManager

logger = logging.getLogger(__name__)


def load_dependencies(
    state: dict,
    thread_id: str,
    *dependency_names: str,
    required: bool = True
) -> dict[str, Any]:
    """
    Load multiple dependencies from files if not already in state.
    
    Args:
        state: Current pipeline state
        thread_id: Thread identifier
        *dependency_names: Names of dependencies to load (e.g., 'parsed_query', 'all_chunks')
        required: If True, raises error if dependency not found
        
    Returns:
        Dictionary with loaded dependencies
        
    Example:
        deps = load_dependencies(state, thread_id, 'parsed_query', 'all_chunks')
        parsed_query = deps['parsed_query']
        all_chunks = deps['all_chunks']
    """
    data_saver = DataSaver(thread_id)
    loaded = {}
    
    for name in dependency_names:
        # Check if already in state
        if name in state and state.get(name):
            loaded[name] = state[name]
        else:
            # Load from file
            logger.info(f"Loading {name} from file...")
            data = data_saver.load_agent_output(name)
            
            if data is None and required:
                logger.error(f"Required dependency '{name}' not found")
                raise FileNotFoundError(f"Cannot proceed: {name}.json not found")
            
            loaded[name] = data
    
    return loaded


def load_jsonl_count(state: dict, thread_id: str, name: str) -> int:
    """
    Load count of items in JSON array file.
    Checks state first, then counts from file.
    
    Args:
        state: Current pipeline state
        thread_id: Thread identifier
        name: Name of JSON file (e.g., 'synthetic_data')
        
    Returns:
        Count of items
    """
    count_key = f"{name}_count"
    
    # Check if count already in state
    if count_key in state and state.get(count_key) is not None:
        return state[count_key]
    
    # Count from file
    data_saver = DataSaver(thread_id)
    count = data_saver.count_items_in_array(name)  # ← Changed to count_items_in_array
    
    if count > 0:
        logger.info(f"Loaded count: {count} items from {name}.json")
    
    return count



def load_checkpoint_metadata(state: dict, thread_id: str) -> dict:
    """
    Load checkpoint metadata from StatusManager.
    
    Args:
        state: Current pipeline state
        thread_id: Thread identifier
        
    Returns:
        Checkpoint metadata dictionary
    """
    # Always load fresh from status file (single source of truth)
    status_manager = StatusManager(thread_id)
    return status_manager.get_checkpoint_metadata()


def with_dependencies(*dependency_names: str):
    """
    Decorator to automatically load dependencies for a node.
    
    Usage:
        @with_dependencies('parsed_query', 'all_chunks')
        async def my_node(state, config, deps):
            parsed_query = deps['parsed_query']
            all_chunks = deps['all_chunks']
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(state, config):
            thread_id = state.get("status_manager_thread_id")
            
            # Load dependencies
            deps = load_dependencies(state, thread_id, *dependency_names)
            
            # Call original function with deps
            return await func(state, config, deps)
        
        return wrapper
    return decorator
