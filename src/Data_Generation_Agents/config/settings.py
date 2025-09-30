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
    DEVELOPMENT_MODE = "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Pipeline Configuration (Updated for 30x5 strategy)
       # Pipeline Configuration - Allow override from .env
    REFINED_QUERIES_COUNT = int(os.getenv("REFINED_QUERIES_COUNT", "2"))
    SEARCH_RESULTS_PER_QUERY = int(os.getenv("SEARCH_RESULTS_PER_QUERY", "5"))
    ROWS_PER_SUBTOPIC = int(os.getenv("ROWS_PER_SUBTOPIC", "5"))
    TOPIC_EXTRACTION_CONCURRENCY = 4
    
    # Gemini Configuration
    GEMINI_DEFAULT_MODEL = os.getenv("GEMINI_DEFAULT_MODEL", "gemini-2.5-flash")
    GEMINI_MAX_TOKENS = 8192
    GEMINI_TEMPERATURE = 0.7
    
    # Storage Paths
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    
    # If OUTPUT_DIR is provided, use it; otherwise, default to project root
    OUTPUT_DIR = os.getenv("OUTPUT_DIR")
    STORAGE_ROOT = Path(OUTPUT_DIR) if OUTPUT_DIR else PROJECT_ROOT
    DATA_PATH = STORAGE_ROOT / "data"
    
    # Logging Configuration
    # LOG_ROOT = PROJECT_ROOT / "logs"
    LOG_ROOT = DATA_PATH / "logs" # Place logs inside the data folder
    AGENT_LOGS_PATH = LOG_ROOT / "agent_logs"
    WORKFLOW_LOGS_PATH = LOG_ROOT / "workflow_logs"
    ERROR_LOGS_PATH = LOG_ROOT / "error_logs"

settings = Settings()
