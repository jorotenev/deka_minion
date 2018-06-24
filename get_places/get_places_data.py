import argparse
import json
import logging as log
from datetime import datetime as dt

from boto3 import session

from deka_types import Circle
from get_places.config import Config
from get_places.google_places_wrapper.wrapper import query_google_places
from shared_utils.file_utils import readJSONFileAndConvertToDict, save_dict_to_file


class InputFileType:
    local_file = "file"
    remote_s3 = "s3"


def main():
    log.info("Starting at %s" % dt.now().isoformat())

    input_circles_coords, metadata = read_input()

    # query the Google Places API to get all places within the input geographical circles
    all_places = query_google_places(circles_coords=input_circles_coords)

    log.info("All batches are processed. %i places obtained" % len(all_places))

    file_path = "{folder}/{area}_count{num_places}_r{radius}_{date}.json".format(
        folder=Config.output_folder,
        area=metadata['area_name'],
        num_places=len(all_places),
        radius=metadata['circle_radius'],
        date=dt.now().replace(microsecond=0).isoformat().replace(":", "_").replace("-", "_")
    )
    log.info("Saving %i places to %s" % (len(all_places), file_path))
    to_save = {
        'metadata': metadata,
        'places': all_places,
    }
    save_dict_to_file(data=to_save, file_path=file_path)

    # important that the last line of the stdout contains the path to the output file
    print(file_path)


def read_input():
    input_type, input_path = parse_args()

    if input_type == InputFileType.local_file:
        raw_input = readJSONFileAndConvertToDict(input_path)
    elif input_type == InputFileType.remote_s3:
        raw_input = read_from_s3(input_path)
    else:
        raise Exception("Invalid input file location")
    return prepare_raw_input(raw_input=raw_input)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help="specify path to a local file")
    parser.add_argument('--s3', help="specify path to a file on S3 <bucket>/<file>")

    args = parser.parse_args()
    return ("s3", args.s3) if args.s3 else ("file", args.file)


def read_from_s3(s3_url):
    bucket, file = s3_url.split("/")
    s3 = session.Session().client('s3')
    obj = s3.get_object(Bucket=bucket, Key=file, ResponseContentType='application/json')
    return json.loads(obj['Body'].read().decode('utf-8'))


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
    log.info("%i circles read" % len(coordinates))
    log.info("Area name: %s" % metadata['area_name'])
    return coordinates, metadata


if __name__ == "__main__":
    main()
