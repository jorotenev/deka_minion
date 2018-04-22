import logging as log
from collections import namedtuple
from datetime import datetime as dt
from multiprocessing import cpu_count
from typing import Dict

import requests

from get_places.deka_config import Config
from get_places.deka_utils.misc import split_to_batches

Circle = namedtuple('Circle', ['lat', 'lng', 'radius'])

_GOOGLE_API_ACCESS_CODE = Config.google_access_key.strip()
# returned by google places api to signify that there's a next page of result for the query
NEXT_PAGE_TOKEN_RESPONSE_KEY = "next_page_token"
# sent as a URL arg when making a request to signify we request a subsequent page from an initial request
NEXT_PAGE_TOKEN_REQUEST_KEY = "pagetoken"
_endpoint_url = Config.google_places_api_url

# hide INFO logs from urllib3, used by requests
log.getLogger("urllib3").setLevel(log.WARNING)


def query_google_places(circles_coords):
    from .parallelise import parallelise

    start = dt.now()

    # split to batches to parallelise querying
    items_per_batch = len(circles_coords) // cpu_count()
    batches = list(split_to_batches(circles_coords, items_per_batch=items_per_batch))
    log.info("%i batches of circles will be processed now" % len(batches))

    result = parallelise(batches, single_query_function=_query_single_circle)

    end = dt.now()
    log.info("Finished in %s seconds" % str((end - start).seconds))

    return result


def _query_single_circle(circle: Circle) -> Dict:
    """
    Given the coordinates of an area (defined by a Circle),
    query the Google Places API for a list of all venues there.
    This method handles the fact that multiple pages of results might be returned.

    The result contains all venues within the given area (i.e. the given Circle)

    Use while-true to make sure we traverse all pages with results returned from the API.

    https://developers.google.com/places/web-service/search
    https://developers.google.com/places/web-service/
    :param circle:
    :return: Dictionary of places.
    """
    all_pages_result = {}

    next_page_token = None
    has_next_page = True

    while has_next_page:
        params = {"location": "%s,%s" % (circle.lat, circle.lng), "radius": circle.radius}
        if next_page_token:
            params[NEXT_PAGE_TOKEN_REQUEST_KEY] = next_page_token
        url = _build_api_url(params)

        try:
            page_result = _make_http_request(url)
            list_of_places = page_result['results']
            places_dict = {place['id']: place for place in list_of_places}  # dict comprehension

            all_pages_result.update(places_dict)

            has_next_page = _api_response_has_more_pages(page_result)
            next_page_token = page_result[NEXT_PAGE_TOKEN_RESPONSE_KEY] if has_next_page else None
        except Exception as ex:
            log.critical('Failed API request. Exception: %s' % str(ex))
            has_next_page = False
    return all_pages_result


def _api_response_has_more_pages(query_result):
    return NEXT_PAGE_TOKEN_RESPONSE_KEY in query_result


def _make_http_request(url):
    result = requests.get(url, timeout=4)
    if result.status_code != 200:
        raise Exception("Google API returned non-200 code for query %s" % url)
    return result.json()


def _build_api_url(params):
    base = "{google_api_url}?key={key}".format(
        key=_GOOGLE_API_ACCESS_CODE,
        google_api_url=_endpoint_url
    )
    for k, v in params.items():
        base += "&%s=%s" % (str(k), str(v))
    return base


if __name__ == "__main__":
    from pprint import pprint

    log.info("testing")
    circle = Circle(lat=42.609327274384256, lng=23.251439246938162, radius=1500)

    result = _query_single_circle(circle)
    pprint(result)
