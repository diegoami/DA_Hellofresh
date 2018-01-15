#!/bin/bash

./check_version.sh
if [ $? == 0 ]; then
  spark-submit --master yarn-client --queue default     --num-executors 2 --executor-memory 512M --executor-cores 2     --driver-memory 512M find_recipes_with_chili.py
fi
  
