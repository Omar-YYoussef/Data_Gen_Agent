"""ScraperAPI tools."""

from typing import Literal, Optional, Type

from langchain_core.callbacks import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from langchain_scraperapi.utils import ScraperAPIWrapper, ScraperAPIStructuredWrapper


class ScraperAPIToolInput(BaseModel):
    """Input schema for ScraperAPI tool.

    This docstring is **not** part of what is sent to the model when performing tool
    calling. The Field default values and descriptions **are** part of what is sent to
    the model when performing tool calling.
    """

    url: str = Field(..., description="The URL of the webpage to scrape")
    output_format: Optional[Literal["text", "markdown"]] = Field(
        None, 
        description="The output format, can be 'text' or 'markdown'. If not specified, returns HTML."
    )
    country_code: Optional[str] = Field(
        None, 
        description="The country code to use for the request (e.g., 'us', 'uk', 'ca')"
    )
    device_type: Optional[Literal["desktop", "mobile"]] = Field(
        None, 
        description="The device type to use for the request, can be 'desktop' or 'mobile'"
    )
    premium: Optional[bool] = Field(
        None, 
        description="Whether to use premium proxies"
    )
    render: Optional[bool] = Field(
        None, 
        description="Whether to render JavaScript"
    )
    keep_headers: Optional[bool] = Field(
        None, 
        description="Whether to keep headers in the request"
    )


class ScraperAPITool(BaseTool):  # type: ignore[override]
    """ScraperAPI tool for web scraping.

    Setup:
        Install ``langchain-scraperapi`` and set environment variable ``SCRAPERAPI_API_KEY``.

        .. code-block:: bash

            pip install -U langchain-scraperapi
            export SCRAPERAPI_API_KEY="your-api-key"

    Instantiation:
        .. code-block:: python

            tool = ScraperAPITool()

    Invocation with args:
        .. code-block:: python

            tool.invoke({
                "url": "https://www.example.com",
                "output_format": "text"
            })

        .. code-block:: python

            "Example Domain\nThis domain is for use in illustrative examples in documents..."

    Invocation with ToolCall:

        .. code-block:: python

            tool.invoke({
                "args": {
                    "url": "https://www.example.com",
                    "output_format": "text"
                }, 
                "id": "1", 
                "name": tool.name, 
                "type": "tool_call"
            })

        .. code-block:: python

            "Example Domain\nThis domain is for use in illustrative examples in documents..."
    """  # noqa: E501

    name: str = "scraperapi"
    """The name that is passed to the model when performing tool calling."""
    description: str = (
        "A tool for scraping web content. "
        "Useful for extracting information from websites. "
        "Input should be a URL and optional parameters for the scraping request."
    )
    """The description that is passed to the model when performing tool calling."""
    args_schema: Type[BaseModel] = ScraperAPIToolInput
    """The schema that is passed to the model when performing tool calling."""

    api_wrapper: ScraperAPIWrapper = Field(default_factory=ScraperAPIWrapper)  # type: ignore[arg-type]

    def _run(
        self,
        url: str,
        output_format: Optional[str] = None,
        country_code: Optional[str] = None,
        device_type: Optional[str] = None,
        premium: Optional[bool] = None,
        render: Optional[bool] = None,
        keep_headers: Optional[bool] = None,
        *,
        run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        """Use the tool to scrape a webpage."""
        try:
            return self.api_wrapper.scrape(
                url=url,
                output_format=output_format,
                country_code=country_code,
                device_type=device_type,
                premium=premium,
                render=render,
                keep_headers=keep_headers,
            )
        except Exception as e:
            return f"Error: {str(e)}"

    async def _arun(
        self,
        url: str,
        output_format: Optional[str] = None,
        country_code: Optional[str] = None,
        device_type: Optional[str] = None,
        premium: Optional[bool] = None,
        render: Optional[bool] = None,
        keep_headers: Optional[bool] = None,
        *,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None
    ) -> str:
        """Use the tool asynchronously to scrape a webpage."""
        try:
            return await self.api_wrapper.scrape_async(
                url=url,
                output_format=output_format,
                country_code=country_code,
                device_type=device_type,
                premium=premium,
                render=render,
                keep_headers=keep_headers,
            )
        except Exception as e:
            return f"Error: {str(e)}"


class ScraperAPIGoogleSearchToolInput(BaseModel):
    """Input schema for ScraperAPI Google Search tool."""

    query: str = Field(
        ..., description="Query keywords that a user wants to search for e.g. 'Pizza recipe'"
    )
    country_code: Optional[str] = Field(
        None, description="Two letter country code for Geo Targeting (e.g. 'us', 'uk', 'ca')"
    )
    tld: Optional[str] = Field(
        None,
        description="Country of Google domain to scrape (e.g. 'com', 'co.uk', 'ca'). Defaults to 'com'",
    )
    output_format: Optional[Literal["json", "csv"]] = Field(
        None, description="The output format, can be 'json' or 'csv'. Defaults to 'json'"
    )
    uule: Optional[str] = Field(
        None, description="Set a region for a search (e.g., 'w+CAIQICINUGFyaXMsIEZyYW5jZQ')"
    )
    num: Optional[int] = Field(None, description="Number of results")
    hl: Optional[str] = Field(None, description="Host Language (e.g., 'DE')")
    gl: Optional[str] = Field(
        None,
        description="Boosts matches whose country of origin matches the parameter value (e.g., 'DE')",
    )
    ie: Optional[str] = Field(
        None, description="Character encoding for the query string (e.g., 'UTF8')"
    )
    oe: Optional[str] = Field(
        None, description="Character encoding for the results (e.g., 'UTF8')"
    )
    start: Optional[int] = Field(
        None, description="Set the starting offset in the result list (e.g., 10 for page 2)"
    )


class ScraperAPIGoogleSearchTool(BaseTool):  # type: ignore[override]
    """ScraperAPI tool for Google Search queries.

    Setup:
        Install ``langchain-scraperapi`` and set environment variable ``SCRAPERAPI_API_KEY``.

        .. code-block:: bash

            pip install -U langchain-scraperapi
            export SCRAPERAPI_API_KEY="your-api-key"

    Instantiation:
        .. code-block:: python

            tool = ScraperAPIGoogleSearchTool()

    Invocation with args:
        .. code-block:: python

            tool.invoke({
                "query": "Pizza recipe",
                "output_format": "json"
            })

    Invocation with ToolCall:

        .. code-block:: python

            tool.invoke({
                "args": {
                    "query": "Pizza recipe",
                    "output_format": "json"
                },
                "id": "1",
                "name": tool.name,
                "type": "tool_call"
            })
    """

    name: str = "scraperapi_google_search"
    description: str = (
        "A tool for performing Google searches using ScraperAPI. "
        "Useful for extracting structured data from Google search results. "
        "Input should be a query and optional parameters for the search request."
    )
    args_schema: Type[BaseModel] = ScraperAPIGoogleSearchToolInput

    api_wrapper: ScraperAPIStructuredWrapper = Field(
        default_factory=ScraperAPIStructuredWrapper
    )  # type: ignore[arg-type]

    def _run(
        self,
        query: str,
        country_code: Optional[str] = None,
        tld: Optional[str] = None,
        output_format: Optional[Literal["json", "csv"]] = None,
        uule: Optional[str] = None,
        num: Optional[int] = None,
        hl: Optional[str] = None,
        gl: Optional[str] = None,
        ie: Optional[str] = None,
        oe: Optional[str] = None,
        start: Optional[int] = None,
        *,
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Use the tool to perform a Google search."""
        try:
            return self.api_wrapper.google_search(
                query=query,
                country_code=country_code,
                tld=tld,
                output_format=output_format,
                uule=uule,
                num=num,
                hl=hl,
                gl=gl,
                ie=ie,
                oe=oe,
                start=start,
            )
        except Exception as e:
            return f"Error: {str(e)}"

    async def _arun(
        self,
        query: str,
        country_code: Optional[str] = None,
        tld: Optional[str] = None,
        output_format: Optional[Literal["json", "csv"]] = None,
        uule: Optional[str] = None,
        num: Optional[int] = None,
        hl: Optional[str] = None,
        gl: Optional[str] = None,
        ie: Optional[str] = None,
        oe: Optional[str] = None,
        start: Optional[int] = None,
        *,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        """Use the tool asynchronously to perform a Google search."""
        try:
            return await self.api_wrapper.google_search_async(
                query=query,
                country_code=country_code,
                tld=tld,
                output_format=output_format,
                uule=uule,
                num=num,
                hl=hl,
                gl=gl,
                ie=ie,
                oe=oe,
                start=start,
            )
        except Exception as e:
            return f"Error: {str(e)}"


class ScraperAPIAmazonSearchToolInput(BaseModel):
    """Input schema for ScraperAPI Amazon Search tool."""

    query: str = Field(
        ..., description="Add a query you want to search e.g. 'green shoes'"
    )
    country_code: Optional[str] = Field(
        None,
        description="Two letter country code for Geo Targeting (e.g. 'us', 'uk', 'ca')",
    )
    tld: Optional[str] = Field(
        None,
        description="Amazon market to be scraped (e.g. 'com', 'co.uk', 'ca'). Defaults to 'com'",
    )
    output_format: Optional[Literal["json", "csv"]] = Field(
        None, description="The output format, can be 'json' or 'csv'. Defaults to 'json'"
    )
    page: Optional[int] = Field(None, description="Paginating the result. For example: 1")


class ScraperAPIAmazonSearchTool(BaseTool):  # type: ignore[override]
    """ScraperAPI tool for Amazon Search queries.

    Setup:
        Install ``langchain-scraperapi`` and set environment variable ``SCRAPERAPI_API_KEY``.

        .. code-block:: bash

            pip install -U langchain-scraperapi
            export SCRAPERAPI_API_KEY="your-api-key"

    Instantiation:
        .. code-block:: python

            tool = ScraperAPIAmazonSearchTool()

    Invocation with args:
        .. code-block:: python

            tool.invoke({
                "query": "green shoes",
                "tld": "co.uk"
            })

    Invocation with ToolCall:

        .. code-block:: python

            tool.invoke({
                "args": {
                    "query": "green shoes",
                    "tld": "co.uk"
                },
                "id": "1",
                "name": tool.name,
                "type": "tool_call"
            })
    """

    name: str = "scraperapi_amazon_search"
    description: str = (
        "A tool for performing Amazon searches using ScraperAPI. "
        "Useful for extracting structured data from Amazon search results. "
        "Input should be a query and optional parameters for the search request."
    )
    args_schema: Type[BaseModel] = ScraperAPIAmazonSearchToolInput

    api_wrapper: ScraperAPIStructuredWrapper = Field(
        default_factory=ScraperAPIStructuredWrapper
    )  # type: ignore[arg-type]

    def _run(
        self,
        query: str,
        country_code: Optional[str] = None,
        tld: Optional[str] = None,
        output_format: Optional[Literal["json", "csv"]] = None,
        page: Optional[int] = None,
        *,
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Use the tool to perform an Amazon search."""
        try:
            return self.api_wrapper.amazon_search(
                query=query,
                country_code=country_code,
                tld=tld,
                output_format=output_format,
                page=page,
            )
        except Exception as e:
            return f"Error: {str(e)}"

    async def _arun(
        self,
        query: str,
        country_code: Optional[str] = None,
        tld: Optional[str] = None,
        output_format: Optional[Literal["json", "csv"]] = None,
        page: Optional[int] = None,
        *,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        """Use the tool asynchronously to perform an Amazon search."""
        try:
            return await self.api_wrapper.amazon_search_async(
                query=query,
                country_code=country_code,
                tld=tld,
                output_format=output_format,
                page=page,
            )
        except Exception as e:
            return f"Error: {str(e)}"
