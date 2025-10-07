import asyncio
import logging
import sys
from typing import Optional
from pathlib import Path
from datetime import datetime

from langdetect import DetectorFactory

from .config.settings import settings
from .workflows.main_workflow import run_pipeline

# Setup enhanced logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

logging.getLogger('scraperapi_sdk._client').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def generate_synthetic_data(
    user_query: str,
    refined_queries_count: Optional[int] = None,
    search_results_per_query: Optional[int] = None,
    rows_per_subtopic: Optional[int] = None,
    gemini_model_name: Optional[int] = None
) -> None:
    """
    Generate synthetic data based on the query.
    
    Args:
        user_query: The data generation request
        refined_queries_count: Number of refined queries (default from .env: 30)
        search_results_per_query: Results per query (default from .env: 5)
        rows_per_subtopic: Rows per subtopic (default from .env: 5)
        gemini_model_name: Gemini model to use (default from .env)
    """
    asyncio.run(run_pipeline(
        user_query,
        refined_queries_count,
        search_results_per_query,
        rows_per_subtopic,
        gemini_model_name
    ))


async def main():
    """CLI entry point."""
    if len(sys.argv) > 1:
        user_query = " ".join(sys.argv[1:])
    else:
        logger.error("No query provided. Usage: synthetic-data 'your query here'")
        sys.exit(1)

    await run_pipeline(user_query)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        pass
