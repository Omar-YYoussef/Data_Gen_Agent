"""Utility for calling ScraperAPI.

Based on documentation at:
https://www.scraperapi.com/documentation/
"""

from typing import Any, Dict, Optional, Literal

import aiohttp
import requests
from langchain_core.utils import get_from_dict_or_env
from pydantic import BaseModel, ConfigDict, SecretStr, model_validator

SCRAPERAPI_BASE_URL = "https://api.scraperapi.com/"
SCRAPERAPI_STRUCTURED_BASE_URL = "https://api.scraperapi.com/structured/"


class ScraperAPIWrapper(BaseModel):
    """Wrapper for ScraperAPI."""

    scraperapi_api_key: SecretStr

    model_config = ConfigDict(
        extra="forbid",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_environment(cls, values: Dict) -> Any:
        """Validate that api key exists in environment."""
        scraperapi_api_key = get_from_dict_or_env(
            values, "scraperapi_api_key", "SCRAPERAPI_API_KEY"
        )
        values["scraperapi_api_key"] = scraperapi_api_key

        return values

    def scrape(
        self,
        url: str,
        output_format: Optional[str] = None,
        country_code: Optional[str] = None,
        device_type: Optional[str] = None,
        premium: Optional[bool] = None,
        render: Optional[bool] = None,
        keep_headers: Optional[bool] = None,
    ) -> str:
        """Scrape a webpage using ScraperAPI.

        Args:
            url: The URL to scrape.
            output_format: The output format, can be "text" or "markdown".
            country_code: The country code to use for the request.
            device_type: The device type to use for the request, can be "desktop" or "mobile".
            premium: Whether to use premium proxies.
            render: Whether to render JavaScript.
            keep_headers: Whether to keep headers in the request.

        Returns:
            The scraped content as a string.
        """
        params = {
            "api_key": self.scraperapi_api_key.get_secret_value(),
            "url": url,
        }

        if output_format:
            params["output_format"] = output_format
        if country_code:
            params["country_code"] = country_code
        if device_type:
            params["device_type"] = device_type
        if premium is not None:
            params["premium"] = "true" if premium else "false"
        if render is not None:
            params["render"] = "true" if render else "false"
        if keep_headers is not None:
            params["keep_headers"] = "true" if keep_headers else "false"
        
        params["scraper_sdk"] = "langchain"

        response = requests.get(SCRAPERAPI_BASE_URL, params=params)
        response.raise_for_status()
        return response.text

    async def scrape_async(
        self,
        url: str,
        output_format: Optional[str] = None,
        country_code: Optional[str] = None,
        device_type: Optional[str] = None,
        premium: Optional[bool] = None,
        render: Optional[bool] = None,
        keep_headers: Optional[bool] = None,
    ) -> str:
        """Scrape a webpage using ScraperAPI asynchronously.

        Args:
            url: The URL to scrape.
            output_format: The output format, can be "text" or "markdown".
            country_code: The country code to use for the request.
            device_type: The device type to use for the request, can be "desktop" or "mobile".
            premium: Whether to use premium proxies.
            render: Whether to render JavaScript.
            keep_headers: Whether to keep headers in the request.

        Returns:
            The scraped content as a string.
        """
        params = {
            "api_key": self.scraperapi_api_key.get_secret_value(),
            "url": url,
        }

        if output_format:
            params["output_format"] = output_format
        if country_code:
            params["country_code"] = country_code
        if device_type:
            params["device_type"] = device_type
        if premium is not None:
            params["premium"] = "true" if premium else "false"
        if render is not None:
            params["render"] = "true" if render else "false"
        if keep_headers is not None:
            params["keep_headers"] = "true" if keep_headers else "false"

        async with aiohttp.ClientSession() as session:
            async with session.get(SCRAPERAPI_BASE_URL, params=params) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    response.raise_for_status()


class ScraperAPIStructuredWrapper(BaseModel):
    """Wrapper for ScraperAPI structured endpoints."""

    scraperapi_api_key: SecretStr

    model_config = ConfigDict(
        extra="forbid",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_environment(cls, values: Dict) -> Any:
        """Validate that api key exists in environment."""
        scraperapi_api_key = get_from_dict_or_env(
            values, "scraperapi_api_key", "SCRAPERAPI_API_KEY"
        )
        values["scraperapi_api_key"] = scraperapi_api_key

        return values

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Helper method to make synchronous requests."""
        base_params = {"api_key": self.scraperapi_api_key.get_secret_value()}
        all_params = {**base_params, **params}

        filtered_params = {k: v for k, v in all_params.items() if v is not None}

        url = f"{SCRAPERAPI_STRUCTURED_BASE_URL}{endpoint}"
        response = requests.get(url, params=filtered_params)
        response.raise_for_status()
        return response.text

    async def _make_request_async(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Helper method to make asynchronous requests."""
        base_params = {"api_key": self.scraperapi_api_key.get_secret_value()}
        all_params = {**base_params, **params}

        filtered_params = {k: v for k, v in all_params.items() if v is not None}

        url = f"{SCRAPERAPI_STRUCTURED_BASE_URL}{endpoint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=filtered_params) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    response.raise_for_status()

    def google_search(
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
    ) -> str:
        """Perform a Google search using ScraperAPI.

        Args:
            query: The search query.
            country_code: The country code to use for the request.
            tld: The top-level domain for Google search (e.g., "com", "co.uk").
            output_format: The output format ("json" or "csv"). Defaults to "json".
            uule: Set a region for a search (e.g., "w+CAIQICINUGFyaXMsIEZyYW5jZQ").
            num: Number of results.
            hl: Host Language (e.g., "DE").
            gl: Boosts matches whose country of origin matches the parameter value (e.g., "DE").
            ie: Character encoding for the query string (e.g., "UTF8").
            oe: Character encoding for the results (e.g., "UTF8").
            start: Set the starting offset in the result list.

        Returns:
            The search results as a string.
        """
        params = {
            "query": query,
            "country_code": country_code,
            "tld": tld,
            "output_format": output_format,
            "uule": uule,
            "num": num,
            "hl": hl,
            "gl": gl,
            "ie": ie,
            "oe": oe,
            "start": start,
        }
        return self._make_request("google/search", params)

    async def google_search_async(
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
    ) -> str:
        """Perform a Google search using ScraperAPI asynchronously.

        Args:
            query: The search query.
            country_code: The country code to use for the request.
            tld: The top-level domain for Google search (e.g., "com", "co.uk").
            output_format: The output format ("json" or "csv"). Defaults to "json".
            uule: Set a region for a search (e.g., "w+CAIQICINUGFyaXMsIEZyYW5jZQ").
            num: Number of results.
            hl: Host Language (e.g., "DE").
            gl: Boosts matches whose country of origin matches the parameter value (e.g., "DE").
            ie: Character encoding for the query string (e.g., "UTF8").
            oe: Character encoding for the results (e.g., "UTF8").
            start: Set the starting offset in the result list.

        Returns:
            The search results as a string.
        """
        params = {
            "query": query,
            "country_code": country_code,
            "tld": tld,
            "output_format": output_format,
            "uule": uule,
            "num": num,
            "hl": hl,
            "gl": gl,
            "ie": ie,
            "oe": oe,
            "start": start,
        }
        return await self._make_request_async("google/search", params)

    def amazon_search(
        self,
        query: str,
        country_code: Optional[str] = None,
        tld: Optional[str] = None,
        output_format: Optional[Literal["json", "csv"]] = None,
        page: Optional[int] = None,
    ) -> str:
        """Perform an Amazon search using ScraperAPI.

        Args:
            query: The search query.
            country_code: Two letter country code for Geo Targeting (e.g. "au", "es", "it"). Mapped from 'COUNTRY' in ScraperAPI docs.
            tld: Amazon market to be scraped (e.g., "com", "co.uk").
            output_format: The output format ("json" or "csv"). Defaults to "json".
            page: Paginating the result.

        Returns:
            The search results as a string.
        """
        params = {
            "query": query,
            "country": country_code, # Map country_code to 'country' parameter for Amazon API
            "tld": tld,
            "output_format": output_format,
            "page": page,
        }
        return self._make_request("amazon/search", params)

    async def amazon_search_async(
        self,
        query: str,
        country_code: Optional[str] = None,
        tld: Optional[str] = None,
        output_format: Optional[Literal["json", "csv"]] = None,
        page: Optional[int] = None,
    ) -> str:
        """Perform an Amazon search using ScraperAPI asynchronously.

        Args:
            query: The search query.
            country_code: Two letter country code for Geo Targeting (e.g. "au", "es", "it"). Mapped from 'COUNTRY' in ScraperAPI docs.
            tld: Amazon market to be scraped (e.g., "com", "co.uk").
            output_format: The output format ("json" or "csv"). Defaults to "json".
            page: Paginating the result.

        Returns:
            The search results as a string.
        """
        params = {
            "query": query,
            "country": country_code, # Map country_code to 'country' parameter for Amazon API
            "tld": tld,
            "output_format": output_format,
            "page": page,
        }
        return await self._make_request_async("amazon/search", params)
