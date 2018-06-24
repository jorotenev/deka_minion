#!/bin/bash
current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
sources_root=$(realpath ${current_dir})

places_s3_bucket="deka-cities-places"
echo "Querying Google Places API now with coordinates from $1"
cd ${sources_root}/get_places
output=$(PYTHONPATH=${sources_root} pipenv run python get_places_data.py --file=$(realpath $1))


if [ $? -eq 0 ]; then
    path=$(echo ${output} | tail -n1 | xargs realpath)
    echo "Location of output file $(realpath ${path})"

    echo "Saving $path to $places_s3_bucket"
    pipenv run python ${sources_root}/save_places/main.py ${places_s3_bucket} ${path}

    echo "Loading $path to redis"
    cd ${sources_root}/load_data
    echo $(pwd)
    PYTHONPATH=${sources_root} pipenv run python main.py --file ${path}

else
    echo "FAIL"
fi
