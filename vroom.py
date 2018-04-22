"""
Vroom-vroom - the minion engine has started.
"""
import logging as log
from datetime import datetime as dt

from deka_config import Config
from deka_utils.file_utils import readJSONFileAndConvertToDict, save_places_to_file, touch_directory
from google_places_wrapper.wrapper import Circle, query_google_places

log_dir = 'log'
touch_directory(log_dir)

log.basicConfig(
    level=log.DEBUG,
    format='[%(levelname)s::%(asctime)s] %(message)s',
    handlers=[
        log.FileHandler("{0}/{1}.log".format(log_dir, 'deka_minion_%s' % dt.now().isoformat())),
        log.StreamHandler()
    ]
)


def main():
    log.info("Starting at %s" % dt.now().isoformat())

    # read the all coordinates of circles (areas) for which we want to query the Google Places API
    input_circles_coords = read_input()

    # query the Google Places API to get all places within the input geographical circles
    all_places = query_google_places(circles_coords=input_circles_coords)
    log.info("All batches are processed. %i places obtained" % len(all_places))

    save_places_to_file(places=all_places, )


def prepare_raw_input(raw_input):
    circle_radius = raw_input['circle_radius']

    return [Circle(lat=circle['lat'], lng=circle['lng'], radius=circle_radius)
            for circle in raw_input['coordinates']]


def read_input():
    raw_input_coords = readJSONFileAndConvertToDict(Config.raw_input_file)

    input_coords = prepare_raw_input(raw_input_coords)
    log.info("%i circles read from %s" % (len(input_coords), Config.raw_input_file))
    return input_coords


if __name__ == "__main__":
    main()
