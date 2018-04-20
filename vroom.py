from deka_config import Config
from deka_utils.file_utils import readJSONFileAndConvertToDict, save_places_to_file
from deka_utils.misc import split_to_batches
from google_places_wrapper.google_places_wrapper import Circle, query_batches


def prepare_raw_input(raw_input):
    circle_radius = raw_input['circle_radius']

    return [Circle(lat=circle['lat'], lng=circle['lng'], radius=circle_radius)
            for circle in raw_input['coordinates']]


def read_input():
    raw_input_coords = readJSONFileAndConvertToDict(Config.raw_input_file)

    input_coords = prepare_raw_input(raw_input_coords)
    print("%i circles" % len(input_coords))
    return input_coords


def main():
    # read the all cordinates of circles (areas) for which we want to query the Google Places API
    input_coords = read_input()

    number_batches = 15
    items_per_batch = len(input_coords) // number_batches
    # split to batches to optimize querying
    batches = list(split_to_batches(input_coords, items_per_batch=items_per_batch))

    print("%i batches ")

    # query the API and get the list of all places returned by the Google API
    all_places = query_batches(batches=batches, split_to_batches=100)

    # save to file
    save_places_to_file(places=all_places, file_path="output/queried_places_result.json")


if __name__ == "__main__":
    main()
