from multiprocessing import Value, Lock, Manager
from unittest import TestCase
from unittest.mock import patch
from uuid import uuid4 as _uuid4

from deka_types import Circle
from get_places.google_places_wrapper.wrapper import interesting_venue_types, query_google_places, \
    MAX_RESULTS_PER_QUERY, _query_single_circle, handle_busy_circle


def uuid4():
    return str(_uuid4())


def do_nothing(*args, **kwargs): pass


dummy_tasks = list(Circle(lat=i % 180, lng=i % 180, radius=i) for i in range(100))


def fake_places_api_response(result_size=100, status="OK"):
    return {
        "results": [{"place_id": uuid4()} for _ in range(result_size)],
        "status": status
    }


@patch('get_places.google_places_wrapper.wrapper._query_single_circle')
class TestParallelise(TestCase):
    """
        The google_places_wrapper is given a list of tasks (geographical circles, used to query an API).
        There's a simple method in the google_places_wrapper that given a circle, performs the actual query to the API.
        We want to test that this method is called the same number of times as the number of tasks.
        The difficulty stems from the fact that our google api wrapper spawns several Processes, each of which
        spawn several threads. And it's the threads that actually call the method which makes the request to the API.
    """

    @classmethod
    def setUpClass(cls):
        cls.patched_save = patch('shared_utils.file_utils.save_dict_to_file')
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
                return {}  # follow the contract of the original function - it should return a dict

        patched_single_query.side_effect = fake_single_request

        # invoke the whole process of querying
        query_google_places(circles_coords=dummy_tasks)

        self.assertEqual(counter.value, len(dummy_tasks),
                         "The _query_single_circe() was not called for all tasks, and it should have been called.")

    def test_called_with_distinct_circles(self, patched_single_query):
        """"""
        # store all of the tasks (circles) with which the query method was called
        dict = Manager().dict()

        def fake_single_request(circle):
            # use the dict as a set - no dupes allowed
            dict[str(circle)] = None  # the val is dummy
            return {}

        patched_single_query.side_effect = fake_single_request

        # invoke the whole process of querying
        query_google_places(circles_coords=dummy_tasks)

        self.assertEqual(len(dict), len(dummy_tasks),
                         "Looks like the query method was called with the same argument twice")


# TODO DRY mocking really nice article https://makina-corpus.com/blog/metier/2013/dry-up-mock-instanciation-with-addcleanup
class TestBusyCircle(TestCase):
    def setUp(self):
        mocked_should_keep_place = patch('get_places.google_places_wrapper.wrapper.should_keep_place')
        mocked_should_keep_place.return_value = True
        self.addCleanup(mocked_should_keep_place.stop)
        mocked_should_keep_place.start()

    @classmethod
    def setUpClass(cls):
        # the result size for each query of specific place type, when using the handle_busy_circle()
        cls.items_per_place_type_returned_by_api = 5

    @patch('get_places.google_places_wrapper.wrapper._make_http_request')
    @patch('get_places.google_places_wrapper.wrapper.handle_busy_circle')
    def test_busy_circle_is_called(self, mocked_handle_busy_circle, mocked_http_request):
        """
        Test that if the API returns result set of length MAX_RESULTS_PER_QUERY,
        the handle_busy_circle method is called and the result of the whole wrapper (i.e. query_query_google_places)
        is *whatever* the handle_busy_circle returned
        """

        # fake that the API returned the largest possible result set
        whatever = "whatevs"
        # we don't test that the method works correctly, only that _query_single_circle returns the method's result
        mocked_handle_busy_circle.return_value = whatever
        mocked_http_request.return_value = fake_places_api_response(result_size=MAX_RESULTS_PER_QUERY)
        task = dummy_tasks[0]
        all_places = _query_single_circle(task)
        self.assertTrue(mocked_handle_busy_circle.called_once_with(task),
                        "handle_busy_circle was not called, even though"
                        "the Places API returned MAX_RESULTS_PER_QUERY for a query")

        self.assertEqual(whatever, all_places)

    @patch('get_places.google_places_wrapper.wrapper._make_http_request')
    def test_busy_circle(self, mocked_http_request):
        task = dummy_tasks[0]

        def side_effect(*args, **kwargs):
            return fake_places_api_response(result_size=self.items_per_place_type_returned_by_api)

        mocked_http_request.side_effect = side_effect
        places = handle_busy_circle(task)
        self.assertEqual(len(interesting_venue_types) * self.items_per_place_type_returned_by_api, len(places),
                         "Method should have returned the combined results of sequential queries for "
                         "different places types")
