import logging as log
from datetime import datetime as dt

from deka_types import Circle
from get_places.config import Config
from get_places.google_places_wrapper.wrapper import query_google_places
from shared_utils.file_utils import readJSONFileAndConvertToDict, save_dict_to_file, get_directory_of_file


def main():
    log.info("Starting at %s" % dt.now().isoformat())

    # read all coordinates of circles (areas) for which we want to query the Google Places API + some metadata
    input_file = "%s/input/sofia_coords_5990_r150.json" % get_directory_of_file(__file__)
    input_circles_coords, metadata = read_input(input_file)

    # query the Google Places API to get all places within the input geographical circles
    all_places = query_google_places(circles_coords=input_circles_coords)
    log.info("All batches are processed. %i places obtained" % len(all_places))

    file_path = "{folder}/{num_places}_{date}.json".format(
        folder=Config.output_folder,
        num_places=len(all_places),
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
        "area_name": raw_input['area_name'],
        "bounding_rectangle": raw_input['bounding_rectangle']
    }
    coordinates = [Circle(lat=circle['lat'], lng=circle['lng'], radius=circle_radius) for circle in
                   raw_input['coordinates']]
    return coordinates, metadata


if __name__ == "__main__":
    main()
