from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

INPUT_FILE = "data/input/recipes-etl.json"
OUTPUT_DIR = "data/output/chili"

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
    has_chili_udf = udf(has_chili, BooleanType())
    chili_recipes = recipes.filter(has_chili_udf(recipes.reg_tok_ingredients ))
    return chili_recipes

def add_difficulty(recipes):
    total_time_udf = udf(total_time)
    difficulty_udf = udf(difficulty)
    recipes = recipes.withColumn("difficulty",difficulty_udf(total_time_udf(col("cookTime"), col("prepTime"))))
    recipes = recipes.drop("reg_tok_ingredients").drop("has_chili")
    return recipes

def retrieve_recipes_json(sqlc, input_file):
    return sqlc.read.json(input_file)

def save_recipes_parquet(recipes, output_dir):
    recipes.write.parquet(output_dir, mode="overwrite")

if __name__ == "__main__":
    conf = SparkConf().setAppName("find_chili")
    sc = SparkContext(conf=conf)
    sqlc = SQLContext(sc)

    recipes = retrieve_recipes_json(sqlc,INPUT_FILE)
    chili_recipes = filter_by_chili(recipes)
    chili_recipes = add_difficulty(chili_recipes)
    save_recipes_parquet(chili_recipes, OUTPUT_DIR)
    chili_recipes.write.parquet(OUTPUT_DIR,  mode="overwrite")
