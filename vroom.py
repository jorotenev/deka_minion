from deka_config import Config
from deka_utils.file_utils import readJSONFileAndConvertToDict, save_places_to_file
from deka_utils.misc import split_to_batches
from google_places_wrapper.google_places_wrapper import Circle, query_batches
from datetime import datetime as dt
import logging as log

log.basicConfig(filename='deka_minion_%s.log' % dt.now().isoformat(),
                level=log.DEBUG,
                format='%(asctime)s %(message)s')


def prepare_raw_input(raw_input):
    circle_radius = raw_input['circle_radius']

    return [Circle(lat=circle['lat'], lng=circle['lng'], radius=circle_radius)
            for circle in raw_input['coordinates']]


def read_input():
    raw_input_coords = readJSONFileAndConvertToDict(Config.raw_input_file)

    input_coords = prepare_raw_input(raw_input_coords)
    log.info("%i circles read from %s" % (len(input_coords), Config.raw_input_file))
    return input_coords


def main():
    log.info("Starting at %s" % dt.now().isoformat())
    # read the all cordinates of circles (areas) for which we want to query the Google Places API
    input_coords = read_input()

    number_batches = 15
    items_per_batch = len(input_coords) // number_batches
    # split to batches to optimize querying
    batches = list(split_to_batches(input_coords, items_per_batch=items_per_batch))

    log.info("%i batches of circles will be processed now")

    # query the API and get the list of all places returned by the Google API
    all_places = query_batches(batches=batches)
    log.info("All batchess are processed. %i places obtained" % len(all_places))
    log.info("Saving places to %s now" % Config.output_folder)
    # save to file
    file_path = "{folder}/{num_places}_{date}.json".format(
        folder=Config.output_folder,
        num_places=len(all_places),
        date=dt.now().isoformat()
    )
    save_places_to_file(places=all_places, file_path=file_path)


if __name__ == "__main__":
    main()
