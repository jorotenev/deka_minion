import argparse
import logging as log
from typing import Tuple, Dict

from load_data.datastore_adapter import load_to_datastore
from load_data.deka_types import Metadata
from shared_utils.file_utils import readJSONFileAndConvertToDict


def main():
    places, metadata = read_input()
    log.info("Loaded input file with %i places." % len(places))
    log.info("area-name = %s" % metadata.area_name)
 
    load_to_datastore(places, metadata=metadata)


def read_input():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help="specify path to a local file")

    args = parser.parse_args()
    return parse_raw_input(readJSONFileAndConvertToDict(filepath=args.file))


def parse_raw_input(raw_input) -> Tuple[Dict, Metadata]:
    input_meta = raw_input['metadata']
    bounding_rect = input_meta['bounding_rectangle']

    metadata = Metadata(
        bounding_rectangle=bounding_rect,
        area_name=input_meta['area_name'])
    return raw_input['places'], metadata


if __name__ == "__main__":
    main()
