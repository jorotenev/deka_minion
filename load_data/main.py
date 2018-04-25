import logging as log
from typing import Tuple, Dict

from load_data.datastore_adapter import load_to_datastore
from load_data.deka_types import Metadata
from shared_utils.file_utils import readJSONFileAndConvertToDict


def main():
    input_file_abs_path = "/home/georgi/Projects/deka_minion/get_places/output/4585_2018-04-23T11:53:57.424784.json"
    places, metadata = read_input(input_file_abs_path)
    log.info("Loaded input file with %i places." % len(places))
    log.info("area-name = %s" % metadata.area_name)

    load_to_datastore(places, metadata=metadata)


def read_input(file_path):
    raw_input = readJSONFileAndConvertToDict(filepath=file_path)
    return parse_raw_input(raw_input)


def parse_raw_input(raw_input) -> Tuple[Dict, Metadata]:
    input_meta = raw_input['metadata']
    bounding_rect = input_meta['bounding_rectangle']

    metadata = Metadata(
        bounding_rectangle=bounding_rect,
        area_name=input_meta['area_name'])
    return raw_input['places'], metadata


if __name__ == "__main__":
    main()
