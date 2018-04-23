import logging as log

from load_data.redis_adapter import load_to_datastore
from shared_utils.file_utils import readJSONFileAndConvertToDict


def main():
    input_file_abs_path = "/home/georgi/Projects/deka_minion/get_places/output/4585_2018-04-23T11:53:57.424784.json"
    data = read_input(input_file_abs_path)
    log.info("Loaded input file with %i places." % len(data))

    load_to_datastore(data)


def read_input(file_path):
    return readJSONFileAndConvertToDict(filepath=file_path)


if __name__ == "__main__":
    main()
