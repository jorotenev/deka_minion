import logging as log
from datetime import datetime as dt

from get_places.deka_config import Config
from get_places.deka_utils.file_utils import readJSONFileAndConvertToDict, touch_directory, save_dict_to_file
from get_places.google_places_wrapper.wrapper import Circle, query_google_places

log_dir = 'log'
touch_directory(log_dir)

log.basicConfig(
    level=log.INFO,
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

    file_path = "{folder}/{num_places}_{date}.json".format(
        folder=Config.output_folder,
        num_places=len(all_places),
        date=dt.now().isoformat()
    )
    log.info("Saving %i places to %s" % (len(all_places), file_path))
    save_dict_to_file(data=all_places, file_path=file_path)


def read_input():
    """
    Reads the file with coordinates of the centre of geographic areas, processes it and return a simple
    list of Circles.
    :return: list of Circle objects (namedtuples)
    """
    raw_input_coords = readJSONFileAndConvertToDict(Config.raw_input_file)

    input_coords = prepare_raw_input(raw_input_coords)
    log.info("%i circles read from %s" % (len(input_coords), Config.raw_input_file))
    return input_coords


def prepare_raw_input(raw_input):
    """
    Combine the raw_input's list of coordinates and the radius (same for all circles), to a list of Circles.

    :param raw_input: dict, as read from the input file. the dict has a 'coordinates' key which
    contains a list of {"lat": number, "lng":number} dicts.
    :return:
    """
    circle_radius = raw_input['circle_radius']

    return [Circle(lat=circle['lat'], lng=circle['lng'], radius=circle_radius)
            for circle in raw_input['coordinates']]


if __name__ == "__main__":
    main()
