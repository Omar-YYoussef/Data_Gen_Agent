# Synthetic Data Pipeline

AI-powered synthetic data generation pipeline with web search and topic extraction.

## Installation

Install the package using pip:

    pip install Data_Generation_Agents

## Requirements

- Python >= 3.8
- API Keys for: Gemini, Tavily, ScraperAPI

## Quick Start

**Step 1: Create Configuration File**

Create a `.env` file in your project directory:

    GEMINI_API_KEY=your_gemini_api_key_here
    TAVILY_API_KEY=your_tavily_api_key_here
    SCRAPERAPI_API_KEY=your_scraper_api_key_here
    OUTPUT_DIR=/path/to/your/output

Note: The `.env` file must be in the directory where you run your scripts.

**Step 2: Use in Python Code**

Basic usage example:

    from Data_Generation_Agents import generate_synthetic_data
    
    generate_synthetic_data("prompt")

Advanced usage with custom parameters:

    generate_synthetic_data(
        user_query="prompt",
        refined_queries_count=20,
        search_results_per_query=5,
        rows_per_subtopic=5
        gemini_model_name="gemeni-2.0-flash
    )

**Step 3: Use CLI**

Command line usage:

    synthetic-data "prompt"

## Configuration

**Environment Variables**

| Variable | Required | Description |
|----------|----------|-------------|
| GEMINI_API_KEY | Yes | Google Gemini API key |
| TAVILY_API_KEY | Yes | Tavily search API key |
| SCRAPERAPI_API_KEY | Yes | ScraperAPI key |
| OUTPUT_DIR | Yes | Output directory path |


## API Reference

### `generate_synthetic_data(user_query: str, refined_queries_count: Optional[int] = None, search_results_per_query: Optional[int] = None, rows_per_subtopic: Optional[int] = None, gemini_model_name: Optional[str] = None)`

Generate synthetic data based on a natural language prompt. The `user_query` is parsed to automatically determine the number of samples, data type, language, and a detailed description of the data to be generated.

**Categories Feature:**
When you specify categories within your domain (e.g., "cardiovascular and neurology" for medical domain), the pipeline will:
- Focus search queries specifically on those categories
- Generate more targeted and relevant content
- Distribute queries across all specified categories
- Use category-specific terminology and concepts

If no categories are specified, the pipeline will comprehensively cover the entire domain.

**Parameters:**
- `user_query` (str): **Required**. A natural language description of the data you want to generate. This query should implicitly or explicitly contain:
    - **Number of samples**: The total count of data entries to generate (e.g., "100"). (required)
    - **Data type**: The structure or format of the data (e.g., "QA pairs", "product reviews", "customer support conversations"). (required)
    - **Language**: The desired language for the generated data (e.g., "English", "French", "Egyptian_Arabic"). (required)
    - **Description**: A detailed explanation of the data's content and context. (required)
    - **Domain**: The desired domain for the generated data (e.g., "Finance", "Medical", "Law"). (optional)
    - **Categories**: Specific subcategories within the domain to focus on (e.g., "cardiovascular, neurology" for medical domain). (optional)
- `refined_queries_count` (int, optional): Number of refined search queries to generate. Defaults to a value from `.env` or internal settings.
- `search_results_per_query` (int, optional): Number of web search results to consider per refined query. Defaults to a value from `.env` or internal settings.
- `rows_per_subtopic` (int, optional): Number of synthetic data rows to generate per extracted subtopic. Defaults to a value from `.env` or internal settings.
- `gemini_model_name` (str, optional): The name of the Gemini model to use (e.g., "gemini-pro", "gemini-1.5-flash"). Defaults to "gemini-2.5-flash" or a value from `.env`.


**Examples:**

```python
from Data_Generation_Agents import generate_synthetic_data

generate_synthetic_data(
    user_query= "Generate 5000 diverse, contextually rich English-to-Egyptian Arabic translation pairs In Law domain with varying sentence complexities, ensuring authentic colloquial Egyptian Arabic translations while preserving English technical terms, proper nouns, and specialized terminology untranslated. the data the data contains two columns (English, Egyptian Arabic)"
    refined_queries_count=25,
    search_results_per_query=5,
    rows_per_subtopic=5
)
```

```python
from Data_Generation_Agents import generate_synthetic_data

generate_synthetic_data(
    user_query="Generate 2000 finance classification examples in Arabic covering banking, insurance, and investment topics, the data contains two columns (Text, classification_type)",
    refined_queries_count=30,
    search_results_per_query=5,
    rows_per_subtopic=5
    gemini_model_name="gemini-1.5-pro"
)
```

## Development

**Local Installation**

    git clone https://github.com/Omar-YYoussef/Data_Gen_Agent
    cd synthetic-data-pipeline
    pip install -e .

## License

MIT License - see LICENSE file for details.

## Support

- Issues: GitHub Issues
- Email: omarjooo595@gmail.com