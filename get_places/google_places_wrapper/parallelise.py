"""
The input is a potentially large list of items (circles). The work to be done is http requests for each circle.
Each request results in a dictionary of place_id -> place_data.

We split the list into batches - equal to the number of cpu cores.
Then we spawn a new Process for each batch. We pass to the Process a single batch and a reference to a dictionary in which
to store the results ("global results dictionary").

Each process itself spawn N threads (~20). Each thread is given a mini-batch (part of a batch, i.e. a sub-batch).
Then each thread sequentially processes the circles in its subbatch and collects the results of each query.
Only when the thread has processes all of its assigned circles, it will push the result to the global result dictionary

The rationale for this is that we take advantage of the multiple cores of the CPU by splitting to Processes.
However, within a single process we can further optimise by using lighter-weight Threads.
The thread only pushes once it has all of its data to minise the number of calls to the shared results dictionary.
The intuition is that calls to this process-shared dictionary are expensive, so it's better to do them more rarely.
The different threads will finish at different times - the larger the mini-batches threads need to process,
the larger the gap between when they finish. This is good because threads wouldn't need to wait for each other
when they want to publish their results to the global results dictionary.
"""

import logging as log
from multiprocessing import Manager, Process, current_process
from threading import Thread
from typing import List, Callable

from deka_types import Circle, Place
from get_places.deka_utils.misc import split_to_batches


def parallelise(batches: List[List[Circle]], single_query_function: Callable[[Circle], Place]):
    """
    Spawn a new worker Process for each batch.
    Pass the batch to the process.
    The Process writes its output to a dictionary, created by the main process.

    :param batches: a list of batches. Each batch is a list of Circle objects
    :param single_query_function - a method which receives a Circle as an argument and returns a str-> place dict
    this method will be called to perform a query for a single circle.

    :return: a single dict with *all* places within the circles from the batches
    """
    with Manager() as manager:
        # all sub-processes will use the final_result to store their results
        final_result = manager.dict()

        procs = []
        for batch in batches:
            p = Process(target=_query_batch,
                        kwargs={"batch": batch, "result_store": final_result, "query_function": single_query_function})
            procs.append(p)
            p.start()

        log.debug("Launched %i processes" % len(procs))

        # wait for all processes to finish
        [p.join() for p in procs]

        # final_result is a proxy dict managed by the Manager.
        # convert it to a normal dict before exiting the "with
        return dict(final_result)


def _query_batch(batch: List[Circle], result_store: dict, query_function) -> None:
    """
    Inception.
    This  method will run in its own process. To speed up things, in this worker process,
    we will spawn couple of threads and distribute the @batch to them - each thread gets a mini batch :)

    The threads will push their results directly to the @result_store, which is held by the main process.

    :param batch
    :param query_function - the function which will perform the actual action of querying the API for a single area (circle)
    :param result_store - this method will publish its output to this dict .duplicates are fine since it's a dict
    :return: None
    """
    process_name = current_process().name

    def sub_batch(mini_batch):
        """runs in a thread. sequentially process all queries in the mini_batch"""

        thread_result = {}
        # query_function returns a dict. collect all the dicts in a list

        list_of_places = [query_function(circle=circle) for circle in mini_batch]
        # combine all dicts in a single dict
        for places in list_of_places:
            thread_result.update(places)
        # merge the results of this thread to the global result (result_store is shared b/w multiple processes)

        result_store.update(thread_result)

    threads = []
    sub_batches = split_to_batches(batch, items_per_batch=len(batch) // 30)  # ~ 20 threads/process
    for sub in sub_batches:
        t = Thread(target=sub_batch, kwargs={'mini_batch': sub})
        threads.append(t)
        t.start()

    log.debug("Process %s started %i threads" % (process_name, len(threads)))
    [t.join() for t in threads]
    log.debug("[DONE] Process %s is done" % process_name)
