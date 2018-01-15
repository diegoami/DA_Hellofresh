# CHILI RECIPES WITH PREPARATION DIFFICULTY

## CREATED WITH

* Linux Ubuntu 16.04
* Java 8
* Hadoop 2.7.5 (correctly configured as described under https://hadoop.apache.org/docs/r2.7.5/hadoop-project-dist/hadoop-common/SingleCluster.html )
  * it might be necessary to set also the properties _fs.checkpoint.dir_,  _hadoop.tmp.dir_ in _$HADOOP_HOME/etc/hadoop/core-site.xml_ and _dfs.name.dir_, _dfs.data.dir_  in _$HADOOP_HOME/etc/hadoop/hdfs-site.xml_ to directories,
* Spark 1.6.1 .

* Python 3.5 (Python 3.6 does not work with PySpark shipped with 1.6.1, as described in https://issues.apache.org/jira/browse/SPARK-19019 )
  *  Set up using Anaconda 4.4 (https://anaconda.org)

```bash
conda create -n spark35 python=3.5
source activate spark35
conda install pyspark
conda install ipython
```

  *  It was necessary to add these environment variables to .bashrc

```bash
export PYSPARK_PYTHON="/home/$USER/anaconda3/envs/spark35/bin/python"
export PYSPARK_DRIVER_PYTHON="/home/$USER/anaconda3/envs/spark35/bin/ipython"
```

  * and these environment variable in _.bashrc_ should also be set, for instance:

```
export JAVA_HOME="/usr/lib/jvm/java-8-oracle"
export HADOOP_PREFIX="/usr/local/hadoop/"
export SPARK_HOME="/usr/local/spark/"
export HADOOP_HOME=${HADOOP_PREFIX}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_MAPRED_HOME=${HADOOP_PREFIX}
export HADOOP_COMMON_HOME=${HADOOP_PREFIX}
export HADOOP_HDFS_HOME=${HADOOP_PREFIX}
export YARN_HOME=${HADOOP_PREFIX}
export PATH="$SPARK_HOME/bin:$SPARK_HOME:$HADOOP_HOME/bin:$HADOOP_HOME:$HADOOP_HOME/sbin:$PATH"
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

```


## PREPARATION

Start Hadoop and Yarn on localhost with _$HADOOP_HOME/sbin/start-all.sh_ and copy the recipes into HDFS .

```bash
mkdir -p ~/data/recipes/input

wget https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json -P ~/data/recipes/input
hadoop fs -mkdir -p hdfs://localhost:9000/user/$USER/data/recipes/input
hadoop fs -mkdir -p hdfs://localhost:9000/user/$USER/data/recipes/output
hadoop fs -copyFromLocal ~/data/recipes/input/recipes.json hdfs://localhost:9000/user/$USER/data/recipes/input/recipes.json
```

After this step recipes have been loaded into the hadoop file system. Verify with

```
hadoop fs -ls hdfs://localhost:9000/user/$USER/data/recipes/input/recipes.json
```



## EXECUTION STANDALONE


```
spark-submit find_recipes_with_chili.py
```

The parquet file is saved to _hdfs://localhost:9000/user/$USER/data/output/chili.parquet_


### EXECUTION ON YARN

It might be necessary to set _yarn.nodemanager.vmem-check-enabled_ to _False_ in *yarn_site.xml*

```
spark-submit --master yarn-client --queue default     --num-executors 2 --executor-memory 512M --executor-cores 2     --driver-memory 512M find_recipes_with_chili.py
```


### RETRIEVE RESULT

You can retrieve results from the output directory on hdfs using this command

```
#rm -rf ~/data/recipes/output
hadoop fs -copyToLocal hdfs://localhost:9000/user/$USER/data/recipes/output ~/data/recipes
ls ~/data/recipes/output/chili.parquet
```

The parquet file is also available in this archive under _output/chili.parquet_

To clean up the HDFS execute

```
#hadoop fs -rmr hdfs://localhost:9000/user/$USER/data/recipes
```

## UNIT TESTS

Tests can be executed from this directory

```
python -m unittest test/test_recipes.py
```



