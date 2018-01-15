#!/bin/bash
#rm -rf ~/data/recipes/output
hadoop fs -copyToLocal hdfs://localhost:9000/user/$USER/data/recipes/output ~/data/recipes
#rm -rf output
hadoop fs -copyToLocal hdfs://localhost:9000/user/$USER/data/recipes/output .

