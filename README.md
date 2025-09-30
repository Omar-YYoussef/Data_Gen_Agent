# 🚀 Synthetic Data Generation Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/synthetic-data-pipeline.svg)](https://badge.fury.io/py/synthetic-data-pipeline)

An AI-powered synthetic data generation pipeline that leverages web search, content scraping, and advanced language models to create high-quality, contextually rich datasets for machine learning and AI training.

## ✨ Features

- **🌐 Intelligent Web Search**: Uses Tavily API for comprehensive web search across multiple queries
- **📊 Advanced Content Scraping**: Robust web scraping with ScraperAPI integration and anti-bot measures
- **🧠 AI-Powered Topic Extraction**: Gemini-based topic extraction from scraped content
- **🎯 Flexible Data Generation**: Generate diverse data types including QA pairs, product reviews, conversations, and more
- **🌍 Multi-Language Support**: Generate data in multiple languages with automatic language detection
- **💾 Stateful Pipeline**: Resume interrupted processes with intelligent checkpointing
- **🔧 Highly Configurable**: Customizable parameters for queries, results, and generation strategies
- **🏷️ Domain Categories**: Optional specification of categories within domains for more targeted and precise data generation
- **📈 Scalable Architecture**: Agent-based architecture for modular and extensible functionality

## 🏗️ Architecture

The pipeline consists of several specialized AI agents working in coordination:

1. **📝 Query Parser Agent**: Analyzes user queries to extract requirements, including optional categories within the specified domain
2. **🔍 Query Refiner Agent**: Generates multiple refined search queries
3. **🌐 Web Search Agent**: Performs comprehensive web searches
4. **🔎 Filtration Agent**: Filters and validates search results
5. **🕷️ Web Scraping Agent**: Extracts content from web pages
6. **🏷️ Topic Extraction Agent**: Identifies relevant topics from content
7. **🎯 Synthetic Data Generator Agent**: Creates synthetic data based on extracted topics

## 📊 Pipeline Flowchart

![Pipeline Flowchart](pipeline.png)

## ⚠️ Important: Topic Extraction Requirement

**CRITICAL**: The data generation phase will **NOT begin** until the topic extraction stage identifies enough unique topics to support your requested data volume.

### How It Works:

The pipeline calculates the required number of topics using this formula:

```
Required Topics = Number of Requested Rows / ROWS_PER_SUBTOPIC
```

**Example**:
- If you request **1000 rows** of data
- And `ROWS_PER_SUBTOPIC = 5` (default)
- The pipeline needs to extract at least **200 unique topics** before data generation begins

### What This Means For You:

1. **Larger Datasets Need More Topics**: Requesting 5000 rows requires significantly more topics than 500 rows
2. **Search Configuration Matters**: If the pipeline can't extract enough topics, you may need to:
   - Increase `REFINED_QUERIES_COUNT` (generate more search queries)
   - Increase `SEARCH_RESULTS_PER_QUERY` (scrape more web pages)
   - Broaden your domain/categories (provide more diverse content sources)
3. **Pipeline Will Wait**: The data generation agent will not start until sufficient topics are extracted
4. **Monitor Progress**: Watch the topic extraction output to see how many topics have been identified

### Configuration Tips:

For large datasets (1000+ rows), consider adjusting these parameters:

```python
generate_synthetic_data(
    "Generate 5000 English QA pairs about machine learning...",
    refined_queries_count=40,      # More search queries
    search_results_per_query=8,    # More results per query
    rows_per_subtopic=5            # Or increase this to need fewer topics
)
```

**Pro Tip**: If you're generating a very large dataset and topic extraction is taking too long, you can increase `ROWS_PER_SUBTOPIC` to reduce the number of required topics (but this may reduce data diversity).

## 🚀 Quick Start

### Installation

```bash
pip install Data_Generation_Agents
```

### Basic Usage

```python
from Data_Generation_Agents import generate_synthetic_data

generate_synthetic_data(
    "prompt"
)
```

### CLI Usage

```bash
synthetic-data "prompt"
```

## ⚙️ Configuration

### Environment Setup

Create a `.env` file in your project directory:

```env
# Required API Keys
GEMINI_API_KEY=your_gemini_api_key_here
TAVILY_API_KEY=your_tavily_api_key_here
SCRAPERAPI_API_KEY=your_scraper_api_key_here

# Output Configuration
OUTPUT_DIR=/path/to/your/output

# Optional: Pipeline Configuration
REFINED_QUERIES_COUNT=30
SEARCH_RESULTS_PER_QUERY=5
ROWS_PER_SUBTOPIC=5
LOG_LEVEL=INFO
```

### 🔑 API Keys Setup

1. **🤖 Gemini API**: Get your key from [Google AI Studio](https://makersuite.google.com/app/apikey)
2. **🔍 Tavily API**: Sign up at [Tavily](https://tavily.com/) for web search capabilities
3. **🕷️ ScraperAPI**: Register at [ScraperAPI](https://www.scraperapi.com/) for robust web scraping


## 🔄 Stateful Pipeline Example

The pipeline's checkpointing system ensures you never lose progress. Here's how it works in practice:

```python
# First run - generates 1000 samples, but gets interrupted after 400
generate_synthetic_data("Generate 1000 English QA pairs about Python programming in categories: basics, advanced topics, libraries......")

# Output shows:
# ✅ Query parsed successfully
# ✅ 30 refined queries generated
# ✅ Web search completed (150 results)
# ✅ Content scraping completed
# ✅ Topic extraction completed (45 topics found)
# ⏸️  Data generation: 400/1000 samples (INTERRUPTED)

# Second run with the SAME query - automatically resumes from checkpoint
generate_synthetic_data("Generate 1000 English QA pairs about Python programming in categories: basics, advanced topics, libraries......")

# Output shows:
# ⏭️  Skipping completed stages (query_parsed, query_refined, web_searched, etc.)
# 🔄 Resuming data generation from topic 41/45
# ✅ Data generation: 1000/1000 samples completed
```

**File Structure Created**:
```
data/
└── 7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9/
    ├── pipeline_state.json           # {"status": "data_generated", "checkpoint": {...}}
    ├── refined_queries.json          # 30 search queries
    ├── search_results.json           # 150 web search results
    ├── scraped_content.json          # Scraped web pages
    ├── all_chunks.json               # Processed content chunks
    ├── all_extracted_topics.json     # 45 extracted topics
    └── synthetic_data.json           # 1000 generated QA pairs
```

## 🎯 Advanced Configuration

### Custom Parameters

```python
generate_synthetic_data(
    user_query="Your data generation request",
    refined_queries_count=40,      # More search queries for broader coverage
    search_results_per_query=8,    # More results per query
    rows_per_subtopic=6,           # More data per topic
    gemini_model_name="gemini-1.5-pro"  # Use different Gemini model
)
```

### Domain-Specific Generation

The pipeline automatically detects and adapts to various domains:
- **Technical**: Programming, software development, cloud computing
- **Medical**: Healthcare, pharmaceuticals, medical devices
- **Legal**: Law, regulations, compliance
- **Financial**: Banking, fintech, investment
- **E-commerce**: Products, reviews, customer service
- **Education**: Learning materials, tutorials, assessments

Users can optionally specify categories within the domain in the natural language query (e.g., "about machine learning in categories: supervised, unsupervised, reinforcement"). The query parser will detect and define the categories variable accordingly.

## 📊 Output Structure

Generated data is saved in structured JSON format with the following structure:

```json
{
    "data_type": "QA pairs",
    "content": {
        "question": "What is machine learning?",
        "answer": "Machine learning is a subset of artificial intelligence..."
    },
    "source_topics": ["machine learning"],
    "generation_timestamp": "2024-01-15T10:30:00Z"
}
```

## 🔧 Development

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/Omar-YYoussef/Data_Gen_Agent
cd Data_Gen_Agent

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .

# Install development dependencies
pip install -e ".[dev]"
```

### Project Structure

```
src/synthetic_data_pipeline/
├── agents/                    # AI agents for different pipeline stages
│   ├── __init__.py
│   ├── base_agent.py          # Base agent class with common functionality
│   ├── query_parser_agent.py  # Parses user queries to extract requirements
│   ├── query_refiner_agent.py # Generates multiple refined search queries
│   ├── web_search_agent.py    # Performs comprehensive web searches
│   ├── filtration_agent.py    # Filters and validates search results
│   ├── web_scraping_agent.py  # Extracts content from web pages
│   ├── topic_extraction_agent.py # Identifies relevant topics from content
│   └── synthetic_data_generator_agent.py # Creates synthetic data
├── services/                  # External service integrations
│   ├── __init__.py
│   ├── gemini_service.py      # Google Gemini API integration
│   ├── tavily_service.py      # Tavily web search API integration
│   ├── scraping_service.py    # Web scraping utilities
│   └── chunking_service.py    # Content chunking for processing
├── workflows/                 # Workflow orchestration
│   ├── __init__.py
│   └── main_workflow.py       # Main workflow coordination
├── models/                    # Data models and schemas
│   ├── __init__.py
│   └── data_schemas.py        # Pydantic models for data structures
├── utils/                     # Utility functions
│   ├── __init__.py
│   ├── json_handler.py        # JSON file operations
│   └── pipeline_state_manager.py # State management and checkpointing
├── config/                    # Configuration management
│   ├── __init__.py
│   └── settings.py            # Application settings and environment variables
├── __init__.py                # Package initialization
├── __main__.py                # CLI entry point
└── main.py                    # Main pipeline orchestration
```


## 🌟 Key Features Deep Dive

### 🔄 Stateful Pipeline with Intelligent Checkpointing

**Resume from Any Point**: The pipeline automatically saves its progress and can resume from exactly where it left off if interrupted. Each query is hashed using SHA-256 to create a unique identifier, and all progress is saved in a dedicated directory structure.

**How It Works**:
```python
# If you run the same query again, it automatically resumes from the last checkpoint
generate_synthetic_data("Generate 5000 English QA pairs about machine learning in categories: supervised learning, unsupervised learning, deep learning, use (Question, Answer) as the columns.")

# The pipeline creates a unique directory based on the query hash:
# data/
# └── a1b2c3d4e5f6.../  # SHA-256 hash of your query
#     ├── pipeline_state.json      # Main state file
#     ├── refined_queries.json     # Saved search queries
#     ├── search_results.json      # Web search results
#     ├── scraped_content.json     # Scraped web content
#     ├── all_chunks.json          # Content chunks
#     ├── all_extracted_topics.json # Extracted topics
#     └── synthetic_data.json      # Generated data
```

**Checkpoint Levels**:
- 🏁 `initial` (0) - Pipeline initialized
- 🎬 `initialized` (1) - Setup completed
- 📝 `query_parsed` (2) - Query analysis completed
- 🔍 `query_refined` (3) - Search queries generated
- 🌐 `web_searched` (4) - Web search completed
- 🕷️ `web_scraped` (5) - Content scraping completed
- 📦 `content_gathered` (6) - Content processing completed
- 🏷️ `topics_extracted` (7) - Topic extraction completed
- 🎯 `data_generated` (8) - Synthetic data generation completed
- ✅ `completed` (9) - Pipeline fully completed

**Benefits**:
- **No Lost Progress**: Never lose hours of work due to interruptions
- **Incremental Processing**: Add more data to existing datasets
- **Resource Efficiency**: Skip completed stages when resuming
- **Debugging**: Easy to inspect intermediate results

### Intelligent Query Processing
- Automatically extracts sample count, data type, language, domain, and optional categories from natural language queries
- Supports implicit and explicit parameter specification
- Handles complex multi-requirement queries

### Robust Web Scraping
- Content quality filtering and validation
- Language detection and filtering
- Error handling and retry mechanisms

### Advanced Topic Extraction
- Contextual topic identification from web content
- Hierarchical topic organization

### Flexible Data Generation
- Support for multiple data structures and formats
- Context-aware content generation

## 📈 Performance & Scalability

- **Concurrent Processing**: Parallel execution of web searches and content scraping
- **Checkpoint System**: Resume interrupted processes without losing progress
- **Rate Limiting**: Intelligent API rate limiting to prevent quota exhaustion

## 🛠️ Troubleshooting

### Common Issues

1. **API Key Errors**
   ```
   Error: Invalid API key
   Solution: Verify your API keys in the .env file
   ```

2. **Rate Limiting**
   ```
   Error: Rate limit exceeded
   Solution: wait before retrying
   ```

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Your pipeline code here
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Google Gemini](https://ai.google.dev/) for advanced language model capabilities
- [Tavily](https://tavily.com/) for comprehensive web search
- [ScraperAPI](https://www.scraperapi.com/) for robust web scraping

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/Omar-YYoussef/Data_Gen_Agent/issues)
- **Email**: omarjooo595@gmail.com
- **Documentation**: [Project Wiki](https://github.com/Omar-YYoussef/Data_Gen_Agent/wiki)

---

**Made with ❤️ by [Omar Youssef](https://github.com/Omar-YYoussef)**