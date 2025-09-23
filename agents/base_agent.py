import logging
import json
import math
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
from datetime import datetime
from pathlib import Path

from config.settings import settings

class BaseAgent(ABC):
    """Enhanced base class for all agents in the pipeline"""
    
    def __init__(self, agent_name: str, config: Optional[Dict[str, Any]] = None):
        self.agent_name = agent_name
        self.config = config or {}
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup agent-specific logging with enhanced formatting"""
        logger = logging.getLogger(f"agent.{self.agent_name}")
        logger.setLevel(getattr(logging, settings.LOG_LEVEL))
        
        # Prevent duplicate handlers
        if logger.handlers:
            return logger
        
        # Create file handler
        log_file = settings.AGENT_LOGS_PATH / f"{self.agent_name}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def update_state(self, status: str, data: Dict[str, Any], 
                     error_message: Optional[str] = None):
        """Update agent state with logging"""
        self.logger.info(f"State updated: {status}")
        if error_message:
            self.logger.error(f"Error: {error_message}")
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate input data - override in child classes"""
        return True
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate output data - override in child classes"""
        return True
    
    @abstractmethod
    def execute(self, input_data: Any, context: Optional[Dict[str, Any]] = None) -> Any:
        """Main execution method - must be implemented by child classes"""
        pass
    
    def run(self, input_data: Any, context: Optional[Dict[str, Any]] = None) -> Any:
        """Public method to run the agent with comprehensive error handling"""
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Starting execution of {self.agent_name}")
            
            # Validate input
            if not self.validate_input(input_data):
                raise ValueError("Input validation failed")
            
            # Execute main logic
            result = self.execute(input_data, context)
            
            # Validate output
            if not self.validate_output(result):
                raise ValueError("Output validation failed")
            
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Completed execution of {self.agent_name} in {execution_time:.2f}s")
            
            return result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Error in {self.agent_name} after {execution_time:.2f}s: {str(e)}"
            # self.logger.exception(error_msg) # This line prints the full traceback
            self.logger.error(error_msg) # This line only prints the clean error message
            raise
    
    def save_data(self, data: Any, filename: str, directory: Path) -> Path:
        """Helper method to save data to file with error handling"""
        try:
            directory.mkdir(parents=True, exist_ok=True)
            file_path = directory / filename
            
            if isinstance(data, (dict, list)):
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(str(data))
            
            self.logger.debug(f"Data saved to {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"Failed to save data to {directory / filename}: {e}")
            raise
    
    def load_data(self, file_path: Path) -> Any:
        """Helper method to load data from file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.suffix == '.json':
                    return json.load(f)
                else:
                    return f.read()
        except Exception as e:
            self.logger.error(f"Failed to load data from {file_path}: {e}")
            raise
    
    def calculate_subtopic_requirements(self, sample_count: int, rows_per_subtopic: int = 5) -> int:
        """Calculate required number of subtopics"""
        return math.ceil(sample_count / rows_per_subtopic)
    
    def validate_subtopic_coverage(self, extracted_topics: List[str], 
                                  required_count: int) -> bool:
        """Validate if we have enough subtopics"""
        return len(extracted_topics) >= required_count
