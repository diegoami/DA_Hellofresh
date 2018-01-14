from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf, lower

from hellofresh.recipes.transform import has_chili_2, total_time_2, difficulty_2
INPUT_FILE = "/data/input/recipes.json"
OUTPUT_DIR = "/data/output/chili"

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
        tokl = tok.lower()
        if (one_change(tokl, "chili") or one_change(tokl, "chilies") ):
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

def filter_by_chili(recipes):
    regexTokenizer = RegexTokenizer(inputCol="ingredients", outputCol="reg_tok_ingredients", pattern="\\W")
    recipes = regexTokenizer.transform(recipes)
    has_chili_udf = udf(has_chili)
    recipes = recipes.withColumn("has_chili", has_chili_udf(col("reg_tok_ingredients")))
    chili_recipes = recipes.filter("has_chili = 'true'")
    return chili_recipes

def add_difficulty(recipes):
    total_time_udf = udf(total_time)
    recipes = recipes.withColumn("total_time", total_time_udf(col("cookTime"), col("prepTime")))
    difficulty_udf = udf(difficulty)
    recipes = recipes.withColumn("difficulty", difficulty_udf(col("total_time")))
    recipes = recipes.drop("reg_tok_ingredients").drop("has_chili").drop("total_time")
    return recipes

def verify_df(chili_recipes):
    pass

def verify_output(output_dir):
    pass

if __name__ == "__main__":
    conf = SparkConf().setAppName("find_chili")
    sc = SparkContext(conf=conf)

    sqlc = SQLContext(sc)
    recipes = sqlc.read.json(INPUT_FILE)

    chili_recipes = filter_by_chili(recipes)
    chili_recipes = add_difficulty(chili_recipes)

    chili_recipes.write.parquet(OUTPUT_DIR,  mode="overwrite")
    chili_recipes_parquet  = sqlc.read.parquet(OUTPUT_DIR)
    assert chili_recipes == chili_recipes_parquet