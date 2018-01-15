#!/bin/bash

./check_version.sh
if [ $? == 0 ]; then
  spark-submit find_recipes_with_chili.py
fi
  
