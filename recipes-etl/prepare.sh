#!/bin/bash

mkdir -p ~/data/recipes/input

wget https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json -P ~/data/recipes/input
hadoop fs -mkdir -p hdfs://localhost:9000/user/$USER/data/recipes/input
hadoop fs -mkdir -p hdfs://localhost:9000/user/$USER/data/recipes/output
hadoop fs -copyFromLocal ~/data/recipes/input/recipes.json hdfs://localhost:9000/user/$USER/data/recipes/input/recipes.json

