import logging
import requests
import copy
from .exceptions import ScraperAPIException
import time

logger = logging.getLogger(__name__)


class ScraperAPIClient:
    def __init__(self, api_key: str, api_endpoint: str = "https://api.scraperapi.com"):
        """Create a new Client instance
        API Key is passed as a query parameter to the API endpoint

        :param api_key: ScraperAPI api_key
        :param api_endpoint: ScraperAPI endpoint
        """
        super().__init__()
        self.api_key = api_key
        self._api_endpoint = api_endpoint
        self.amazon = Amazon(client=self)
        self.google = Google(client=self)
        self.walmart = Walmart(client=self)

    def _get_headers(self, headers=None):
        if headers is None:
            headers = {}
        return headers

    def _get_params(self, params=None):
        if params is None:
            params = {}
        params["api_key"] = self.api_key
        return params

    def make_request(
        self,
        *,
        url=None,
        method="GET",
        data=None,
        params=None,
        headers=None,
        timeout=55,
        endpoint=None,
    ):
        if type(params) is not dict:
            params = {}
        params = copy.deepcopy(params)
        if url:
            params["url"] = url
        service_url = f"{self._api_endpoint}"
        if endpoint is not None:
            service_url += f"/{endpoint}"
        try:
            logger.debug(
                f"Making a {method} request to {url} data={data} params={params} headers={headers}"
            )
            response = requests.request(
                method=method,
                url=service_url,
                params=self._get_params(params),
                data=data,
                headers=self._get_headers(headers),
                timeout=timeout,
            )
            response.raise_for_status()
        except Exception as e:
            raise ScraperAPIException(f"Failed to scrape {method} {url}", e)
        return response

    def scrape(self, url, method, data=None, params=None, headers=None):
        response = self.make_request(
            url=url, method=method, params=params, headers=headers, data=data
        )
        content_type = response.headers.get("Content-Type", "")
        if "text" in content_type:
            return response.text
        elif "json" in content_type:
            return response.json()
        else:
            return response.content

    def get(self, url, params=None, headers=None):
        return self.scrape(url=url, method="GET", params=params, headers=headers)

    def post(self, url, data=None, params=None, headers=None):
        return self.scrape(
            url=url, data=data, method="POST", params=params, headers=headers
        )

    def put(self, url, data=None, params=None, headers=None):
        return self.scrape(
            url=url, data=data, method="PUT", params=params, headers=headers
        )


class Amazon:
    def __init__(self, client):
        super().__init__()
        self.client = client

    def product(self, asin, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/amazon/product",
            params=dict(asin=asin, country=country, tld=tld),
        )
        return response.json()

    def search(self, query, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/amazon/search",
            params=dict(query=query, country=country, tld=tld),
        )
        return response.json()

    def offers(self, asin, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/amazon/offers",
            params=dict(asin=asin, country=country, tld=tld),
        )
        return response.json()

    def review(self, asin, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/amazon/review",
            params=dict(asin=asin, country=country, tld=tld),
        )
        return response.json()

    def prices(self, asins: list | tuple, country=None, tld=None):
        if type(asins) not in (list, tuple):
            raise ValueError("asins must be a list or tuple")
        asins_string = ",".join(asins)
        response = self.client.make_request(
            endpoint=f"structured/amazon/prices",
            params=dict(asins=asins_string, country=country, tld=tld),
        )
        return response.json()


class Google:
    def __init__(self, client):
        super().__init__()
        self.client = client

    def search(self, query, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/google/search",
            params=dict(query=query, country=country, tld=tld),
        )
        return response.json()

    def news(self, query, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/google/news",
            params=dict(query=query, country=country, tld=tld),
        )
        return response.json()

    def jobs(self, query, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/google/jobs",
            params=dict(query=query, country=country, tld=tld),
        )
        return response.json()

    def shopping(self, query, country=None, tld=None):
        response = self.client.make_request(
            endpoint="structured/google/shopping",
            params=dict(query=query, country=country, tld=tld),
        )
        return response.json()


class Walmart:
    def __init__(self, client):
        super().__init__()
        self.client = client

    def search(self, query, page=None):
        response = self.client.make_request(
            endpoint="structured/walmart/search",
            params=dict(query=query, page=page),
        )
        return response.json()

    def category(self, category, page=None):
        response = self.client.make_request(
            endpoint="structured/walmart/category",
            params=dict(category=category, page=page),
        )
        return response.json()

    def product(self, product_id):
        response = self.client.make_request(
            endpoint="structured/walmart/product",
            params=dict(product_id=product_id),
        )
        return response.json()


class ScraperAPIAsyncClient:
    def __init__(self, api_key: str, api_endpoint="https://async.scraperapi.com"):
        super().__init__()
        self.api_key = api_key
        self._api_endpoint = api_endpoint
        self.amazon = AmazonAsync(client=self)
        self.google = GoogleAsync(client=self)

    def create(
        self,
        url,
        method="GET",
        api_params=None,
        webhook_url=None,
    ):
        if api_params is None:
            api_params = {}
        payload = {
            "url": url,
            "method": method,
            "apiParams": api_params,
            "apiKey": self.api_key,
        }
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}

        logger.debug(f"ScraperAPI payload: {payload}")
        try:
            response = requests.post(
                url=f"{self._api_endpoint}/jobs",
                json=payload,
            )
        except Exception as e:
            logger.error(f"Failed to create async request {url} {e}")
            raise ScraperAPIException(f"Failed to create async request {url}", e)
        return response.json()

    def get(self, request_id):
        try:
            response = requests.get(
                url=f"{self._api_endpoint}/jobs/{request_id}",
                params={"api_key": self.api_key},
            )
            response_data = response.json()
        except Exception as e:
            logger.error(f"Failed to get async request {request_id} {e}")
            raise ScraperAPIException(f"Failed to get async request {request_id}", e)
        return response_data

    def wait(
        self,
        request_id,
        cooldown=5,
        max_retries=10,
        raise_for_exceeding_max_retries=False,
    ):
        """Wait for the async request to finish

        :param request_id: request id
        :param cooldown: wait between checks, seconds
        :param max_retries: max number of checks for results to perform. Returns the response if max_retries exceeded, raises an exception if raise_for_exceeding_max_retries is True.
        """
        retries = 0
        while True:
            logger.debug(
                f"Checking request {request_id} attempt {retries + 1} of {max_retries}"
            )
            response = self.get(request_id)
            if response["status"] == "finished":
                return response
            elif max_retries is not None and retries >= max_retries:
                if raise_for_exceeding_max_retries:
                    raise ScraperAPIException("Exceeded maximum retries", None)
                else:
                    return response
            else:
                time.sleep(cooldown)
                retries += 1


class AmazonAsync:
    def __init__(self, client):
        super().__init__()
        self.client = client

    def product(self, asin, *, api_params=None, webhook_url=None):
        payload = {"apiKey": self.client.api_key, "asin": asin}
        if api_params:
            payload["apiParams"] = api_params
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/amazon/product",
                json=payload,
            )
        except Exception as e:
            logger.error(
                f"Failed to create async Amazon Product request asin={asin} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Amazon Product request asin={asin}", e
            )
        return response.json()

    def products(
        self, asins: list | tuple, *, api_params=None, webhook_url=None
    ) -> list:
        payload = {"apiKey": self.client.api_key, "asins": asins}
        if api_params:
            payload["apiParams"] = api_params
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/amazon/product",
                json=payload,
            )
        except Exception as e:
            logger.error(
                f"Failed to create async Amazon Product request asins={asins} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Amazon Product request asins={asins}", e
            )
        return response.json()

    def search(self, query, api_params=None, webhook_url=None):
        payload = {"apiKey": self.client.api_key, "query": query}
        if api_params:
            payload["apiParams"] = api_params
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/amazon/search",
                json=payload,
            )

        except Exception as e:
            logger.error(
                f"Failed to create async Amazon Search request query={query} {e}"
            )
            raise ScraperAPIException(
                f"Faield to create async Amazon Search request query={query}", e
            )
        return response.json()

    def offers(
        self,
        asin,
        ref=None,
        condition=None,
        f_new=None,
        f_usedGood=None,
        f_usedLikeNew=None,
        f_usedVeryGood=None,
        f_usedAcceptable=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "asin": asin}
        if api_params:
            payload["apiParams"] = api_params
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        if ref:
            payload["ref"] = ref
        if condition:
            payload["condition"] = condition
        if f_new:
            payload["f_new"] = f_new
        if f_usedGood:
            payload["f_usedGood"] = f_usedGood
        if f_usedLikeNew:
            payload["f_usedLikeNew"] = f_usedLikeNew
        if f_usedVeryGood:
            payload["f_usedVeryGood"] = f_usedVeryGood
        if f_usedAcceptable:
            payload["f_usedAcceptable"] = f_usedAcceptable

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/amazon/offers",
                json=payload,
            )
        except Exception as e:
            logger.error(
                f"Failed to create async Amazon Offers request asin={asin} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Amazon Offers request asin={asin}", e
            )
        return response.json()

    def offers_multiple(
        self,
        asins: list | tuple,
        ref=None,
        condition=None,
        f_new=None,
        f_usedGood=None,
        f_usedLikeNew=None,
        f_usedVeryGood=None,
        f_usedAcceptable=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "asins": asins}
        if api_params:
            payload["apiParams"] = api_params
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        if ref:
            payload["ref"] = ref
        if condition:
            payload["condition"] = condition
        if f_new:
            payload["f_new"] = f_new
        if f_usedGood:
            payload["f_usedGood"] = f_usedGood
        if f_usedLikeNew:
            payload["f_usedLikeNew"] = f_usedLikeNew
        if f_usedVeryGood:
            payload["f_usedVeryGood"] = f_usedVeryGood
        if f_usedAcceptable:
            payload["f_usedAcceptable"] = f_usedAcceptable

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/amazon/offers",
                json=payload,
            )
        except Exception as e:
            logger.error(
                f"Failed to create async Amazon Offers request asin={asins} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Amazon Offers request asin={asins}", e
            )
        return response.json()

    def review(
        self,
        asin,
        ref=None,
        filter_by_star=None,
        reviewer_type=None,
        page_number=None,
        tld=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "asin": asin}
        if ref:
            payload["ref"] = ref
        if filter_by_star:
            payload["filterByStar"] = filter_by_star
        if reviewer_type:
            payload["reviewerType"] = reviewer_type
        if page_number:
            payload["pageNumber"] = page_number
        if tld:
            payload["tld"] = tld
        if api_params:
            payload["apiParams"] = api_params
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/amazon/review",
                json=payload,
            )
        except Exception as e:
            logger.error(
                f"Failed to create async Amazon Review request asin={asin} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Amazon Review request asin={asin}", e
            )
        return response.json()

    def reviews(
        self,
        asins: list | tuple,
        ref=None,
        filter_by_star=None,
        reviewer_type=None,
        page_number=None,
        tld=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "asins": asins}
        if ref:
            payload["ref"] = ref
        if filter_by_star:
            payload["filterByStar"] = filter_by_star
        if reviewer_type:
            payload["reviewerType"] = reviewer_type
        if page_number:
            payload["pageNumber"] = page_number
        if tld:
            payload["tld"] = tld
        if api_params:
            payload["apiParams"] = api_params
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/amazon/review",
                json=payload,
            )
        except Exception as e:
            logger.error(
                f"Failed to create async Amazon Review request asin={asins} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Amazon Review request asin={asins}", e
            )
        print(response.content)
        return response.json()


class GoogleAsync:
    def __init__(self, client):
        super().__init__()
        self.client = client

    def search(
        self,
        query,
        tld=None,
        uule=None,
        num=None,
        hl=None,
        gl=None,
        ie=None,
        oe=None,
        start=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "query": query}
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        if tld:
            payload["tld"] = tld
        if uule:
            payload["uule"] = uule
        if num:
            payload["num"] = num
        if hl:
            payload["hl"] = hl
        if gl:
            payload["gl"] = gl
        if ie:
            payload["ie"] = ie
        if oe:
            payload["oe"] = oe
        if start:
            payload["start"] = start
        if api_params:
            payload["apiParams"] = api_params

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/google/search",
                json=payload,
            )
            response_data = response.json()
        except Exception as e:
            logger.error(
                f"Failed to create async Google Search request query={query} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Google Search request query={query}", e
            )
        return response_data

    def news(
        self,
        query,
        tld=None,
        uule=None,
        num=None,
        hl=None,
        gl=None,
        ie=None,
        oe=None,
        start=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "query": query}
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        if tld:
            payload["tld"] = tld
        if uule:
            payload["uule"] = uule
        if num:
            payload["num"] = num
        if hl:
            payload["hl"] = hl
        if gl:
            payload["gl"] = gl
        if ie:
            payload["ie"] = ie
        if oe:
            payload["oe"] = oe
        if start:
            payload["start"] = start
        if api_params:
            payload["apiParams"] = api_params

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/google/news",
                json=payload,
            )
            response_data = response.json()
        except Exception as e:
            logger.error(
                f"Failed to create async Google Search request query={query} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Google Search request query={query}", e
            )
        return response_data

    def jobs(
        self,
        query,
        tld=None,
        uule=None,
        num=None,
        hl=None,
        gl=None,
        ie=None,
        oe=None,
        start=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "query": query}
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        if tld:
            payload["tld"] = tld
        if uule:
            payload["uule"] = uule
        if num:
            payload["num"] = num
        if hl:
            payload["hl"] = hl
        if gl:
            payload["gl"] = gl
        if ie:
            payload["ie"] = ie
        if oe:
            payload["oe"] = oe
        if start:
            payload["start"] = start
        if api_params:
            payload["apiParams"] = api_params

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/google/jobs",
                json=payload,
            )
            response_data = response.json()

        except Exception as e:
            logger.error(
                f"Failed to create async Google Search request query={query} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Google Search request query={query}", e
            )
        return response_data

    def shopping(
        self,
        query,
        tld=None,
        uule=None,
        num=None,
        hl=None,
        gl=None,
        ie=None,
        oe=None,
        start=None,
        api_params=None,
        webhook_url=None,
    ):
        payload = {"apiKey": self.client.api_key, "query": query}
        if webhook_url:
            payload["callback"] = {"type": "webhook", "url": webhook_url}
        if tld:
            payload["tld"] = tld
        if uule:
            payload["uule"] = uule
        if num:
            payload["num"] = num
        if hl:
            payload["hl"] = hl
        if gl:
            payload["gl"] = gl
        if ie:
            payload["ie"] = ie
        if oe:
            payload["oe"] = oe
        if start:
            payload["start"] = start
        if api_params:
            payload["apiParams"] = api_params

        try:
            response = requests.post(
                url=f"{self.client._api_endpoint}/structured/google/shopping",
                json=payload,
            )
            response_data = response.json()
        except Exception as e:
            logger.error(
                f"Failed to create async Google Search request query={query} {e}"
            )
            raise ScraperAPIException(
                f"Failed to create async Google Search request query={query}", e
            )
        return response_data
