from multiprocessing import Value, Lock, Manager
from unittest import TestCase
from unittest.mock import patch

from google_places_wrapper.wrapper import query_google_places
from vroom import log

# the logger we use in the actual code outputs to a file too, apart from to the console. leave only the
# console when testing
logger = log.getLogger()
logger.handlers = []


def do_nothing(*args, **kwargs): pass


@patch('google_places_wrapper.wrapper._query_single_circle')
class TestParallelise(TestCase):
    """
        The google_places_wrapper is given a list of tasks (geographical circles, used to query an API).
        There's a simple method in the google_places_wrapper that given a circle, performs the actual query to the API.
        We want to test that this method is called the same number of times as the number of tasks.
        The difficulty stems from the fact that our google api wrapper spawns several Processes, each of which
        spawn several threads. And it's the threads that actually call the method which makes the request to the API.
    """

    def setUp(self):
        self.tasks = list(range(10000))

    @classmethod
    def setUpClass(cls):
        cls.patched_save = patch('deka_utils.file_utils.save_places_to_file')
        cls.patched_save.side_effect = do_nothing
        cls.patched_save.start()

    @classmethod
    def tearDownClass(cls):
        cls.patched_save.stop()

    def test_simple(self, patched_single_query):
        counter = Value('i', 0)
        lock = Lock()

        def fake_single_request(circle):
            with lock:
                counter.value += 1
                return {}

        patched_single_query.side_effect = fake_single_request

        # invoke the whole process of querying
        query_google_places(circles_coords=self.tasks)

        self.assertEqual(counter.value, len(self.tasks),
                         "The _query_single_circe() was not called for all tasks, and it should have been called.")

    def test_called_with_distinct_circles(self, patched_single_query):
        """"""
        # store all of the tasks (circles) with which the query method was called
        dict = Manager().dict()
        m = Manager()

        def fake_single_request(circle):
            # use the dict as a set - no dupes allowed
            dict[str(circle)] = None  # the val is dummy
            return {}

        patched_single_query.side_effect = fake_single_request

        # invoke the whole process of querying
        query_google_places(circles_coords=self.tasks)

        self.assertEqual(len(dict), len(self.tasks),
                         "Looks like the query method was called with the same argument twice")
