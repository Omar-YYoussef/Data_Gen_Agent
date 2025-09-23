from typing import Dict, Any, Optional, List
from models.data_schemas import SyntheticDataPoint
from agents.base_agent import BaseAgent
from services.gemini_service import gemini_service
from config.settings import settings
from datetime import datetime
import time

class SyntheticDataGeneratorAgent(BaseAgent):
    """Agent to generate synthetic data from topics (runs 3 in parallel)"""
    
    def __init__(self, agent_index: int, config: Optional[Dict[str, Any]] = None):
        super().__init__(f"synthetic_generator_{agent_index}", config)
        self.agent_index = agent_index
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate input contains topics list, data_type, language, and optional description"""
        if not isinstance(input_data, dict):
            return False
        if "topics" not in input_data or not isinstance(input_data["topics"], list):
            return False
        if "data_type" not in input_data or not isinstance(input_data["data_type"], str):
            return False
        if "language" not in input_data or not isinstance(input_data["language"], str):
            return False
        if "description" in input_data and not isinstance(input_data["description"], (str, type(None))):
            return False
        return True
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate output is list of SyntheticDataPoint objects"""
        return (
            isinstance(output_data, list) and
            all(isinstance(item, SyntheticDataPoint) for item in output_data)
        )
    
    def _save_progress(self, data_type: str, topics_processed_count: int, successful_generations: int, failed_generations: int, synthetic_data_points: List[SyntheticDataPoint]):
        """Saves the current progress of data generation."""
        self.logger.info(f"Saving progress: {len(synthetic_data_points)} data points generated so far.")
        generated_data = {
            "agent_index": self.agent_index,
            "data_type": data_type,
            "topics_processed": topics_processed_count,
            "successful_generations": successful_generations,
            "failed_generations": failed_generations,
            "total_data_points": len(synthetic_data_points),
            "data": [
                {
                    "content": point.content,
                    "source_topics": point.source_topics,
                    "timestamp": point.generation_timestamp.isoformat()
                }
                for point in synthetic_data_points
            ]
        }
        
        self.save_data(
            generated_data,
            f"generated_data_{self.agent_index}.json",
            settings.SYNTHETIC_DATA_PATH
        )

    def execute(self, input_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> List[SyntheticDataPoint]:
        """Generate synthetic data for assigned topics, handling API errors gracefully."""
        
        topics = input_data["topics"]
        data_type = input_data["data_type"]
        language = input_data["language"]
        description = input_data.get("description", None)
        
        self.logger.info(f"Agent {self.agent_index} generating {data_type} data for {len(topics)} topics in {language}")
        
        synthetic_data_points = []
        successful_generations = 0
        failed_generations = 0
        last_save_count = 0
        save_interval = 100
        
        for i, topic in enumerate(topics):
            try:
                self.logger.debug(f"Generating data for topic ({i+1}/{len(topics)}): {topic}")
                
                generated_items = gemini_service.generate_synthetic_data(topic, data_type, language, description)
                
                if generated_items:
                    for item in generated_items:
                        synthetic_point = SyntheticDataPoint(
                            data_type=data_type,
                            content=item,
                            source_topics=[topic],
                            generation_timestamp=datetime.now()
                        )
                        synthetic_data_points.append(synthetic_point)
                    
                    successful_generations += 1
                    self.logger.debug(f"Generated {len(generated_items)} data points for topic: {topic}")

                    if len(synthetic_data_points) - last_save_count >= save_interval:
                        self._save_progress(data_type, i + 1, successful_generations, failed_generations, synthetic_data_points)
                        last_save_count = len(synthetic_data_points)
                else:
                    failed_generations += 1
                    self.logger.warning(f"Failed to generate data for topic: {topic}")
                
                time.sleep(3)

            except Exception as e:
                self.logger.error(f"An API error occurred during synthetic data generation: {e}")
                self.logger.warning("This is likely due to exhausted API quotas or rate limits.")
                self.logger.warning("The pipeline will now be terminated, but the data generated so far will be saved.")
                
                # Break the loop to stop further processing
                break
        
        self.logger.info(f"Agent {self.agent_index} finished generation phase.")
        self.logger.info(f"Total data points generated: {len(synthetic_data_points)}")
        self.logger.info(f"Success rate: {successful_generations}/{i+1} topics attempted ({(successful_generations/(i+1)*100) if i > -1 else 0:.1f}%)")
        
        # Always save any data that was generated, especially after an error
        if synthetic_data_points:
            self._save_progress(data_type, i + 1, successful_generations, failed_generations, synthetic_data_points)
        
        return synthetic_data_points