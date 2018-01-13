from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf, lower

from hellofresh.recipes import has_chili, total_time, difficulty

conf = SparkConf().setAppName("find_chili")
sc = SparkContext(conf=conf)
#sc = SparkContext('local[*]')
sqlc = SQLContext(sc)
recipes = sqlc.read.json("data/recipes.json")


recipes = recipes.withColumn("ingredients_lower", lower(col("ingredients")))
regexTokenizer = RegexTokenizer(inputCol="ingredients_lower", outputCol="reg_tok_ingredients", pattern="\\W")
recipes = regexTokenizer.transform(recipes)

has_chili_udf = udf(has_chili)
recipes = recipes.withColumn("has_chili", has_chili_udf(col("reg_tok_ingredients")))
chili = recipes.filter("has_chili = 'true'")

total_time_udf = udf(total_time)
chili = chili.withColumn("total_time", total_time_udf(col("cookTime"), col("prepTime")) )

difficulty_udf = udf(difficulty)
chili = chili.withColumn("difficulty", difficulty_udf(col("total_time")) )

chili.write.parquet("data/parquet/chili",  mode="overwrite")