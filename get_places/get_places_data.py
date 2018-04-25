import logging as log
import sys
from datetime import datetime as dt

from deka_types import Circle
from get_places.config import Config
from get_places.google_places_wrapper.wrapper import query_google_places
from shared_utils.file_utils import readJSONFileAndConvertToDict, save_dict_to_file


def main():
    log.info("Starting at %s" % dt.now().isoformat())

    # read all coordinates of circles (areas) for which we want to query the Google Places API + some metadata
    input_file_fullpath = sys.argv[1]
    input_circles_coords, metadata = read_input(input_file_fullpath)

    # query the Google Places API to get all places within the input geographical circles
    all_places = query_google_places(circles_coords=input_circles_coords)
    log.info("All batches are processed. %i places obtained" % len(all_places))

    file_path = "{folder}/{area}_count{num_places}_r{radius}_{date}.json".format(
        folder=Config.output_folder,
        area=metadata['area_name'],
        num_places=len(all_places),
        radius=metadata['circle_radius'],
        date=dt.now().isoformat()
    )
    log.info("Saving %i places to %s" % (len(all_places), file_path))
    to_save = {
        'metadata': metadata,
        'places': all_places,
    }
    save_dict_to_file(data=to_save, file_path=file_path)


def read_input(input_file_path):
    """
    Reads the file with coordinates of the centre of geographic areas, processes it and return a simple
    list of Circles.
    :return: list of Circle objects (namedtuples)
    """
    raw_input_data = readJSONFileAndConvertToDict(input_file_path)

    input_coords, metadata = prepare_raw_input(raw_input_data)
    log.info("%i circles read from %s" % (len(input_coords), input_file_path))
    log.info("Area name: %s" % metadata['area_name'])
    return input_coords, metadata


def prepare_raw_input(raw_input):
    """
    Combine the raw_input's list of coordinates and the radius (same for all circles), to a list of Circles.

    :param raw_input: dict, as read from the input file. the dict has a 'coordinates' key which
    contains a list of {"lat": number, "lng":number} dicts.
    :return: the list of areas (circles) and metadata
    """
    circle_radius = raw_input['circle_radius']
    metadata = {
        "circle_radius": circle_radius,
        "area_name": raw_input['area_name'],
        "bounding_rectangle": raw_input['bounding_rectangle']
    }
    coordinates = [Circle(lat=circle['lat'], lng=circle['lng'], radius=circle_radius) for circle in
                   raw_input['coordinates']]
    return coordinates, metadata


if __name__ == "__main__":
    main()
