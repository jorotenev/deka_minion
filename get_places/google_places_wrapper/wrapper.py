import logging as log
from datetime import datetime as dt
from multiprocessing import cpu_count
from time import sleep
from typing import Dict

import requests

from deka_types import Circle
from get_places.config import Config
from get_places.deka_utils.misc import split_to_batches

# hide INFO logs from urllib3, used by requests
log.getLogger("urllib3").setLevel(log.WARNING)

_GOOGLE_API_ACCESS_CODE = Config.google_access_key.strip()

# returned by google places api to signify that there's a next page of result for the query
NEXT_PAGE_TOKEN_RESPONSE_KEY = "next_page_token"

# sent as a URL arg when making a request to signify we request a subsequent page from an initial request
NEXT_PAGE_TOKEN_REQUEST_KEY = "pagetoken"

# google places's api base url
_endpoint_url = Config.google_places_api_url

# https://developers.google.com/places/web-service/search
MAX_RESULTS_PER_QUERY = 60

interesting_venue_types = set(Config.places_types)


def query_google_places(circles_coords):
    from .parallelise import parallelise

    start = dt.now()

    # split to batches to parallelise querying
    items_per_batch = len(circles_coords) // cpu_count()
    batches = list(split_to_batches(circles_coords, items_per_batch=items_per_batch))
    log.info("%i batches of circles will be processed now" % len(batches))
    log.info("Result will contain venues of types [%s]" % str(interesting_venue_types))

    result = parallelise(batches, single_query_function=_query_single_circle)

    end = dt.now()
    log.info("Finished in %s seconds" % str((end - start).seconds))

    return result


def _query_single_circle(circle: Circle, type=None) -> Dict:
    """
    Given the coordinates of an area (defined by a Circle),
    query the Google Places API for a list of all venues there.
    This method handles the fact that multiple pages of results might be returned.

    The result contains all venues within the given area (i.e. the given Circle)

    Use while-true to make sure we traverse all pages with results returned from the API.

    https://developers.google.com/places/web-service/search
    https://developers.google.com/places/web-service/
    :param circle:
    :param type: one of https://developers.google.com/places/web-service/supported_types
    :return: Dictionary of places.
    """
    all_pages_result = {}

    next_page_token = None
    has_next_page = True
    url = ""

    while has_next_page:
        params = {"location": "%s,%s" % (circle.lat, circle.lng), "radius": circle.radius}
        if next_page_token:
            params[NEXT_PAGE_TOKEN_REQUEST_KEY] = next_page_token
        if type:
            params['type'] = type

        url = _build_api_url(params)

        try:
            page_result = _make_http_request(url)
            list_of_places = page_result['results']
            places_dict = {place['place_id']: place for place in list_of_places}  # Dict comprehension

            all_pages_result.update(places_dict)

            has_next_page = _api_response_has_more_pages(page_result)
            next_page_token = page_result[NEXT_PAGE_TOKEN_RESPONSE_KEY] if has_next_page else None
        except Exception as ex:
            log.critical('Failed API request. Exception: %s' % str(ex))
            has_next_page = False

    if (len(all_pages_result) == MAX_RESULTS_PER_QUERY):
        # the query returned the max allowed items. probs there are more.
        # we're gonna run the same query several times now for specific types and combine the results
        log.debug("A Query has returned MAX_RESULTS_PER_QUERY results. "
                  "Highly likely that there are more places within this area. [%s]" % url)
        if not type:
            # guard agains infinete recursion. don't run the extended search if we are already doing it.
            return handle_busy_circle(circle)
        else:
            log.critical(
                "A query for a specific venue type returned MAX_RESULTS_PER_QUERY %s" % url)
    # filter-out some places
    return {place_id: place for place_id, place in all_pages_result.items() if should_keep_place(place)}


def handle_busy_circle(circle) -> Dict:
    """
    tl;dr wrapper around _query_sincle_circle which will 1) sequentially query @circle for all interesting_venue_types,
    2) combine the result and 3) return it.

    Google Places API allows us to filter only on *one* place type - i.e. return only restaurants.
    We don't use such a filter since we are interested in a wider scope of venue types (interesting_venue_types).
    The minion's overall strategy is to use small enough circle radius to ensure that the result set is smaller than the max number of
    items that the API can return. However, it's possible that the result set contains the max number
    of items - which means it's highly likely that there are more places in the queried area.

    This method helps remedy the above situation. It's called if during normal querying the size of the result set is equal
    to the known maximum returned by the Google API. In this case, this method will make N sequential queries to
    the API where N == len(interesting_venue_types).
    :param circle: same as _query_single_circle
    :return: same as _query_single_circle
    """
    log.debug("Starting an extended search for %s" % str(circle))
    combined_types_of_places = {}
    # each result is a dict containing places of only one type
    sequential_results = [_query_single_circle(circle, type=type) for type in interesting_venue_types]
    # merge it all into a single dict
    for single_type_result in sequential_results:
        combined_types_of_places.update(single_type_result)

    return combined_types_of_places


def should_keep_place(place):
    """
    True for places that match our preferred type and are not permanently closed

    :param place: place is an object as returned by the Places API.
    :return: boolean
    """
    result = set()
    if "types" in place and len(set(place['types']) & interesting_venue_types) > 0:
        # the union of the two sets has at least one item
        pass
    else:
        return False

    if "permanently_closed" in place and place["permanently_closed"]:
        return False
    else:
        pass

    return True


def _api_response_has_more_pages(query_result):
    return NEXT_PAGE_TOKEN_RESPONSE_KEY in query_result


retriable_statuses = ["INVALID_REQUEST", "UNKNOWN_ERROR"]


def _make_http_request(url, retries_left=6):
    result = requests.get(url, timeout=4)
    if result.status_code != 200:
        raise Exception("Google API returned non-200 code for query %s" % url)
    parsed = result.json()

    status = parsed['status']
    if status in retriable_statuses:
        # "There is a short delay between when a next_page_token is issued, and when it will become valid."
        # UNKNOWN_ERROR indicates a server-side error; trying again may be successful.

        if retries_left > 0:
            # log.debug("Going to retry query %s" % url)
            sleep(1)
            return _make_http_request(url, retries_left=retries_left - 1)
        else:
            raise Exception(
                "Google responded with [%s] for query [%s]. The retries were exceeded." % (status, url))

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
