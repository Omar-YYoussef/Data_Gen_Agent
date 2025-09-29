import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

class Settings:
    # API Configuration
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
    SCRAPERAPI_API_KEY = os.getenv("SCRAPERAPI_API_KEY")
    # Development Settings
    DEVELOPMENT_MODE = os.getenv("DEVELOPMENT_MODE", "false").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Pipeline Configuration (Updated for 20x5 strategy)
    REFINED_QUERIES_COUNT = int(os.getenv("REFINED_QUERIES_COUNT", "5"))
    SEARCH_RESULTS_PER_QUERY = int(os.getenv("SEARCH_RESULTS_PER_QUERY", "5"))
    ROWS_PER_SUBTOPIC = int(os.getenv("ROWS_PER_SUBTOPIC", "5"))
    TOPIC_EXTRACTION_CONCURRENCY = int(os.getenv("TOPIC_EXTRACTION_CONCURRENCY", "5"))
    
    # Gemini Configuration
    GEMINI_MAX_TOKENS = int(os.getenv("GEMINI_MAX_TOKENS", "8192"))
    GEMINI_TEMPERATURE = 0.7
    
    # Storage Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    STORAGE_ROOT = PROJECT_ROOT / "storage"
    DEBUG_PATH = STORAGE_ROOT / "debug"
    
    # Logging Configuration
    LOG_ROOT = PROJECT_ROOT / "logs"
    AGENT_LOGS_PATH = LOG_ROOT / "agent_logs"
    WORKFLOW_LOGS_PATH = LOG_ROOT / "workflow_logs"
    ERROR_LOGS_PATH = LOG_ROOT / "error_logs"

settings = Settings()
