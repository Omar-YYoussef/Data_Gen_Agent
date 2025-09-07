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
    
    def execute(self, input_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> List[SyntheticDataPoint]:
        """Generate synthetic data for assigned topics"""
        
        topics = input_data["topics"]
        data_type = input_data["data_type"]
        language = input_data["language"]
        description = input_data.get("description", None)  # Extract optional description
        
        self.logger.info(f"Agent {self.agent_index} generating {data_type} data for {len(topics)} topics in {language} (Description: {description})")
        
        synthetic_data_points = []
        successful_generations = 0
        failed_generations = 0
        
        try:
            for i, topic in enumerate(topics):
                self.logger.debug(f"Generating data for topic ({i+1}/{len(topics)}): {topic}")
                
                # Generate 5 synthetic data points for this topic
                generated_items = gemini_service.generate_synthetic_data(topic, data_type, language, description)
                
                if generated_items:
                    # Convert to SyntheticDataPoint objects
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
                else:
                    failed_generations += 1
                    self.logger.warning(f"Failed to generate data for topic: {topic}")
                
                # Rate limiting to respect API limits
                if i % 5 == 0 and i > 0:
                    time.sleep(2)
            
            self.logger.info(f"Agent {self.agent_index} completed: {len(synthetic_data_points)} data points generated")
            self.logger.info(f"Success rate: {successful_generations}/{len(topics)} topics ({(successful_generations/len(topics)*100):.1f}%)")
            
            # Save generated data for this agent
            generated_data = {
                "agent_index": self.agent_index,
                "data_type": data_type,
                "topics_processed": len(topics),
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
            
            return synthetic_data_points
            
        except Exception as e:
            self.logger.error(f"Synthetic data generation failed for agent {self.agent_index}: {e}")
            raise
