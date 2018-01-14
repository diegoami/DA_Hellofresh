# INSTRUCTIONS

## FIND_VENTURES

A class implementing a tree containing ventures and accounts can be found in **hellofresh/account/tree.py**.

Tests are in **test/test_tree.py**.

**find_ventures** is a function accepting a list of links to create a tree, and the account of which we would like to find the venture.

A program to test-drive the **find_ventures** function can be found in the file **find_recipes_with_chili.py** in the main directory.

## FIND_RECIPES_WITH_CHILI

### Preparations

It is assumed that compatible versions of Hadoop, Yarn and Spark have been installed locally on localhost.

To set up the recipes in the HDFS file system execute these commands

```bash
mkdir -p ~/data/input
mkdir -p ~/data/output

wget https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json -P ~/data/input
hadoop fs -mkdir hdfs://localhost:9000/data
hadoop fs -mkdir hdfs://localhost:9000/data/input
hadoop fs -mkdir hdfs://localhost:9000/data/output
hadoop fs -copyFromLocal ~/data/recipes.json hdfs://localhost:9000/data/input/recipes.json
```
