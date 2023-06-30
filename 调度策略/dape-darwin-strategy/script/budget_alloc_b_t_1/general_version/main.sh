#!/usr/bin/env bash

set -x
python3Venv=/home/xiaoju/dape/python/stg3/bin/activate
basepath=$(cd `dirname $0`; pwd)
cd $basepath
source $python3Venv
export PYTHONPATH=$basepath
echo $PYTHONPATH
PARAM=$1
python main.py --param="${PARAM}"
