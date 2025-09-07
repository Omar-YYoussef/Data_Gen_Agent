from typing import Dict, Any, Optional, List
from models.data_schemas import SyntheticDataPoint, ParsedQuery
from agents.base_agent import BaseAgent
from config.settings import settings
from datetime import datetime
import hashlib
import json

class DataCollectionAgent(BaseAgent):
    """Agent to collect and aggregate synthetic data from parallel generators"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("data_collection", config)
    
    def validate_input(self, input_data: Any) -> bool:
        """Validate input contains synthetic data and query info"""
        return (isinstance(input_data, dict) and 
                "synthetic_data_lists" in input_data and 
                "parsed_query" in input_data)
    
    def validate_output(self, output_data: Any) -> bool:
        """Validate output is final dataset structure"""
        return isinstance(output_data, dict) and "final_dataset" in output_data
    
    def _calculate_content_hash(self, content: Dict[str, Any]) -> str:
        """Calculate hash for content deduplication"""
        content_str = json.dumps(content, sort_keys=True)
        return hashlib.md5(content_str.encode()).hexdigest()
    
    def execute(self, input_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Collect, deduplicate, and finalize synthetic dataset"""
        
        synthetic_data_lists = input_data["synthetic_data_lists"]
        parsed_query = input_data["parsed_query"]
        target_count = parsed_query.sample_count
        
        self.logger.info(f"Collecting synthetic data from {len(synthetic_data_lists)} generators")
        self.logger.info(f"Target sample count: {target_count}")
        
        try:
            # Combine all synthetic data points
            all_data_points = []
            for data_list in synthetic_data_lists:
                all_data_points.extend(data_list)
            
            self.logger.info(f"Total data points before deduplication: {len(all_data_points)}")
            
            # Deduplicate based on content hash
            seen_hashes = set()
            unique_data_points = []
            
            for point in all_data_points:
                content_hash = self._calculate_content_hash(point.content)
                
                if content_hash not in seen_hashes:
                    seen_hashes.add(content_hash)
                    unique_data_points.append(point)
            
            self.logger.info(f"Data points after deduplication: {len(unique_data_points)}")
            
            # Take only the required number of samples
            final_data_points = unique_data_points[:target_count]
            
            # Create final dataset structure
            final_dataset = {
                "metadata": {
                    "original_query": parsed_query.original_query,
                    "domain_type": parsed_query.domain_type,
                    "data_type": parsed_query.data_type,
                    "language": parsed_query.language,
                    "requested_count": target_count,
                    "actual_count": len(final_data_points),
                    "completion_rate": f"{(len(final_data_points)/target_count)*100:.1f}%",
                    "total_generated": len(all_data_points),
                    "after_deduplication": len(unique_data_points),
                    "generation_timestamp": datetime.now().isoformat()
                },
                "data": [
                    {
                        "id": i + 1,
                        "content": point.content,
                        "source_topics": point.source_topics,
                        "quality_score": point.quality_score,
                        "generated_at": point.generation_timestamp.isoformat()
                    }
                    for i, point in enumerate(final_data_points)
                ]
            }
            
            # Save final dataset
            self.save_data(
                final_dataset,
                f"final_dataset_{parsed_query.data_type}_{parsed_query.domain_type.replace(' ', '_')}.json",
                settings.FINAL_OUTPUT_PATH / "collected_datasets"
            )
            
            # Create summary statistics
            statistics = {
                "collection_summary": {
                    "target_samples": target_count,
                    "delivered_samples": len(final_data_points),
                    "total_generated": len(all_data_points),
                    "deduplication_removed": len(all_data_points) - len(unique_data_points),
                    "completion_rate": (len(final_data_points)/target_count)*100,
                    "average_topics_per_sample": sum(len(point.source_topics) for point in final_data_points) / len(final_data_points) if final_data_points else 0
                }
            }
            
            self.logger.info(f"Final dataset created with {len(final_data_points)} samples")
            self.logger.info(f"Completion rate: {(len(final_data_points)/target_count)*100:.1f}%")
            
            return {
                "final_dataset": final_dataset,
                "statistics": statistics
            }
            
        except Exception as e:
            self.logger.error(f"Data collection failed: {e}")
            raise
