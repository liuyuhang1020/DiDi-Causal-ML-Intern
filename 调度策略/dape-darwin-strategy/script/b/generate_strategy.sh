#!/usr/bin/env bash

set -ex
python3Venv=/home/xiaoju/dape/python/stg3/bin/activate
basepath=$(cd `dirname $0`; pwd)
cd $basepath
source $python3Venv
export PYTHONPATH=$basepath
echo $PYTHONPATH
ret=$?

CONFIG=config/config_prod.toml
DATE=`date +%Y-%m-%d`
PARAM=$1

python3 generate_strategy.py --config "${CONFIG}" --date "${DATE}" --parameter="${PARAM}"


if [ $ret -eq 1 ]; then
    echo "run failed return $ret"
    deactivate
    exit $ret
fi
deactivate
