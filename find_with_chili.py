from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf, lower
import json
import requests

r = requests.get("https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json")


def has_chili(tok_ings):
    def one_change(first, second):
        if first == second:
            return True
        if len(first) == len(second):
            count = 0
            for i in range(len(first)):
                if first[i] != second[i]:
                    count += 1
                    if count > 1:
                        return False
            return True
        if len(second) > len(first):
            first, second = second, first
        gap = 0
        if (len(first) == len(second)+1):
            for i in range(len(second)):
                if first[i+gap] != second[i]:
                   gap += 1
                   if (gap > 1):
                       return False
            return True
        return False
    for tok in tok_ings:
        if (one_change(tok, "chili") or one_change(tok, "chilies") ):
            return True
    return False

def total_time(cook_time, prep_time):
    def get_minutes(str):
        if str:
            tot_amount = 0
            ptime_str = str[2:]
            if "H" in ptime_str:
                tot_amount = int(ptime_str[:ptime_str.index("H")])*60
                ptime_str = ptime_str[ptime_str.index("H")+1:]
            if "M" in ptime_str:
                tot_amount += int(ptime_str[:ptime_str.index("M")])
            return tot_amount
        else:
            return None
    ckm, ptm  = get_minutes(cook_time), get_minutes(prep_time)

    return ( ckm + ptm ) if ckm is not None and ptm is not None else None

def difficulty(total_time):
    if total_time is None:
        return "Unknown"
    elif int(total_time) < 30:
        return "Easy"
    elif int(total_time) < 60:
        return "Medium"
    else:
        return "Hard"


conf = SparkConf().setAppName("find_chili")
sc = SparkContext(conf=conf)

sqlc = SQLContext(sc)
recipes = sqlc.createDataFrame([json.loads(line) for line in r.iter_lines()])


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

chili.write.parquet("data/parquet/chili_yarn",  mode="overwrite")