import pyspark
from pyspark import SparkContext, SQLContext


sc = pyspark.SparkContext('local[*]')
sqlc = SQLContext(sc)

recipes = sqlc.read.json("data/recipes.json")
recipes.createOrReplaceTempView("recipes")
chili = sqlc.sql("select * from recipes where ingredients like '%chili%'")
chili.take(5)