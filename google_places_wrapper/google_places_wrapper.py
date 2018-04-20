from threading import Thread, Lock

from deka_config import Config
from typing import List, Type, Dict
from multiprocessing import Manager, Process, current_process

from deka_utils.misc import split_to_batches
import logging as log

_ACCESS_CODE = Config.google_access_key
_endpoint_url = Config.google_places_api_url

# typing annotations
Place = Dict


class Circle:
    """
    Represents circle geographical area, defined by coordinates and a radius
    """

    def __init__(self, lat, lng, radius):
        self.lat = lat
        self.lng = lng
        self.radius = radius

    def __repr__(self):
        return "Circle(lat=%s lng=%s r=%s)" % (self.lat, self.lng, self.radius)


def query_batches(batches: List[List[Circle]]):
    """
    Given circle batches, query the Google Places API for information about the venues
    within the geographical circles.

    Wraps query_batch() by using multiprocessing to speed up things.

    :param batches: a list of batches. Each batch is a list of Circle objects

    :return: a single dict with *all* places within the circles from the batches
    """
    with Manager() as manager:
        # all sub-processes will use the final_result to store their results
        final_result = manager.dict()
        procs = []
        for batch in batches:
            p = Process(target=_query_batch, kwargs={"batch": batch, "result_store": final_result})
            procs.append(p)
            p.start()
        # wait for all processes to finish
        log.info("Launched %i processes" % len(procs))

        [p.join() for p in procs]

        # return the result
        # final_result is a manager-managed :) proxy dict. convert it to a normal dict before exiting the "with"
        return dict(final_result)


def _query_batch(batch: List[Circle], result_store: dict) -> None:
    """
    Inception.
    This  method will run in its own process. To speed up things, in this, separate process,
    we will spawn couple of threads and distribute the @batch to them.
    The threads will push their results directly to the @result_store, which is held by the main process.

    :param batch
    :param result_store - this method will publish its output to this dict .duplicates are fine since it's a dict
    :return: None
    """
    lock = Lock()
    process_name = current_process().name

    def sub_batch(mini_batch):
        log.debug("Starting a new thread in process %s" % process_name)
        thread_result = {}
        list_of_places = [_query_single_circle(circle=circle) for circle in mini_batch]
        for places in list_of_places:
            thread_result.update(places)

        # todo is a lock really needed?
        lock.acquire()
        result_store.update(thread_result)
        lock.release()

    threads = []
    sub_batches = split_to_batches(batch, items_per_batch=len(batch) // 3)  # ~ 3 threads
    for sub in sub_batches:
        t = Thread(target=sub_batch, kwargs={'mini_batch': sub})
        threads.append(t)
        t.start()
    log.debug("Process %s started %i threads" % (process_name, len(threads)))
    [t.join() for t in threads]
    log.debug("[DONE] Process %s is done" % process_name)


def _build_api_url(params):
    # todo
    return "{base_url}?".format(
        base_url=_endpoint_url
    )


def _query_single_circle(circle: Circle) -> Dict:
    """
    Given the coordinates of an area (defined by a Circle),
    query the Google Places API for a list of all venues there.
    This method handles the fact that multiple pages of results might be returned.

    The result contains all venues within the given area (i.e. the given Circle)

    :param circle:
    :return: Dictionary of places.
    """
    from uuid import uuid4
    from time import sleep

    sleep(.51)
    return {str(uuid4()): 'veri gut'}
