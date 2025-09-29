from typing import Dict, Any, Optional, List
from models.data_schemas import SyntheticDataPoint
from agents.base_agent import BaseAgent
from services.gemini_service import gemini_service
from config.settings import settings
from datetime import datetime # Uncomment datetime import
# import time # Remove time import
import asyncio

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
    
    async def execute(self, input_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> List[SyntheticDataPoint]:
        """Generate synthetic data for assigned topics, handling API errors gracefully."""
        
        topics = input_data["topics"]
        data_type = input_data["data_type"]
        language = input_data["language"]
        description = input_data.get("description", None)
        
        self.logger.info(f"Agent {self.agent_index} starting generation for {len(topics)} topics in {language}")
        
        # Remove calls to load previous progress
        # start_index, successful_generations, failed_generations, synthetic_data_points = self._load_progress(data_type)
        
        # last_save_count = len(synthetic_data_points)
        # save_interval = 100

        # Initialize these variables for the current run
        synthetic_data_points = []
        successful_generations = 0
        failed_generations = 0
        start_index = 0

        if start_index >= len(topics):
            self.logger.info("All topics already processed. Skipping generation.")
            return synthetic_data_points

        for i, topic in enumerate(topics[start_index:], start=start_index):
            try:
                self.logger.debug(f"Generating data for topic ({i+1}/{len(topics)}): {topic}")
                
                generated_items = await gemini_service.generate_synthetic_data(topic, data_type, language, description)
                self.logger.debug(f"Gemini service returned {len(generated_items)} items for topic: {topic}")
                
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

                    # Remove conditional saving of progress
                    # if len(synthetic_data_points) - last_save_count >= save_interval:
                    #     self._save_progress(data_type, i + 1, successful_generations, failed_generations, synthetic_data_points)
                    #     last_save_count = len(synthetic_data_points)
                else:
                    failed_generations += 1
                    self.logger.warning(f"Failed to generate data for topic: {topic}")
                
                await asyncio.sleep(3)

            except Exception as e:
                self.logger.error(f"An API error occurred during synthetic data generation: {e}")
                self.logger.warning("This is likely due to exhausted API quotas or rate limits.")
                self.logger.warning("The pipeline will now be terminated, but the data generated so far will be saved.")
                
                # Break the loop to stop further processing
                break
        
        self.logger.info(f"Agent {self.agent_index} finished generation phase.")
        self.logger.info(f"Total data points generated: {len(synthetic_data_points)}")
        
        total_topics_attempted = (i + 1) - start_index if 'i' in locals() else 0
        if total_topics_attempted > 0:
            success_rate = (successful_generations / total_topics_attempted) * 100
            self.logger.info(f"Success rate for this run: {successful_generations}/{total_topics_attempted} topics attempted ({success_rate:.1f}%)")
        
        # Remove final saving of data
        # if synthetic_data_points:
        #     final_topics_processed = (i + 1) if 'i' in locals() else start_index
        #     self._save_progress(data_type, final_topics_processed, successful_generations, failed_generations, synthetic_data_points)
        
        return synthetic_data_points