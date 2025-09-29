# Synthetic Data Pipeline

AI-powered synthetic data generation pipeline with web search and topic extraction.

## Installation

Install the package using pip:

    pip install synthetic-data-pipeline

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

    from synthetic_data_pipeline import generate_synthetic_data
    
    result = generate_synthetic_data("Generate 100 QA pairs about Python")

Advanced usage with custom parameters:

    generate_synthetic_data(
        user_query="prompt",
        refined_queries_count=20,
        search_results_per_query=5,
        rows_per_subtopic=5
    )

**Step 3: Use CLI**

Command line usage:

    synthetic-data "Generate data about machine learning"

## Configuration

**Environment Variables**

| Variable | Required | Description |
|----------|----------|-------------|
| GEMINI_API_KEY | Yes | Google Gemini API key |
| TAVILY_API_KEY | Yes | Tavily search API key |
| SCRAPERAPI_API_KEY | Yes | ScraperAPI key |
| OUTPUT_DIR | Yes | Output directory path |

**Custom Output Location**

Set via environment variable in `.env`:

    OUTPUT_DIR=/custom/path

Or via code:

    generate_synthetic_data("prompt")

Or via CLI:

    synthetic-data "prompt"

## API Reference

**generate_synthetic_data(user_query, refined_queries_count, search_results_per_query, rows_per_subtopic)**

Generate synthetic data based on a natural language prompt.

Parameters:
- user_query (str): Natural language description of data to generate
- refined_queries_count (int, optional): Number of search queries to generate
- search_results_per_query (int, optional): Number of search links per query
- rows_per_subtopic (int, optional): Number of rows to generate per subtopic

Example:

    generate_synthetic_data(
        user_query="Generate customer reviews",
        refined_queries_count=20,
        search_results_per_query=10,
        rows_per_subtopic=10
    )

## Development

**Local Installation**

    git clone https://github.com/yourusername/synthetic-data-pipeline
    cd synthetic-data-pipeline
    pip install -e .

## License

MIT License - see LICENSE file for details.

## Support

- Issues: GitHub Issues
- Email: omarjooo595@gmail.com