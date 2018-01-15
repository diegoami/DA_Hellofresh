

from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, IntegerType
from py4j.protocol import Py4JJavaError
import traceback
import argparse
INPUT_FILE = "data/recipes/input/recipes.json"
OUTPUT_DIR = "data/recipes/output/"
OUTPUT_FILE = "chili.parquet"
OUTPUT_PARQUET = OUTPUT_DIR + OUTPUT_FILE

CHECK_SAVE = True

VERIFY_OUTPUT_JSON = "data/output/recipes_chili.json"

def has_chili(tokens):
    """ True if any of the tokens in the list is chili, chilies or differs by one character from these words

    Parameters:
        tokens: a list of tokens from the ingredients

    Returns:
        True or False, if chili/chilies with at most one spelling error is found

    """
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
    for tok in tokens:
        tokl = tok.lower()
        if (one_change(tokl, "chili") or one_change(tokl, "chilies") ):
            return True
    return False



def total_time(cook_time, prep_time):
    """ Returns the total time of cooking and preparation

    Parameters:
        cook_time : cooking time in the format PT<hours>H<minutes>M
        prep_time : preparation time in the format PT<hours>H<minutes>M

    Returns:
        sum of cooking time and preparation time in minutes, or None if either is invalid or empty

    """
    def get_minutes(str):
        if str and str[:2] == "PT":
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
    """ difficulty of the recipe based on the total time of preparation

    Parameters:
        total_time : the total time of preparation of the recipe

    Returns:
        A string containing Easy, Medium, Hard or Unknown depending on the preparation time

    """
    if total_time is None:
        return "Unknown"
    elif total_time < 30:
        return "Easy"
    elif total_time < 60:
        return "Medium"
    else:
        return "Hard"

def filter_by_chili(recipes):
    """ filter a list of recipes keeping only those having chili as ingredient

    Parameters:
        recipes: DataFrame containing recipes

    Returns:
        only the recipes having chili as ingredient
    """

    regexTokenizer = RegexTokenizer(inputCol="ingredients", outputCol="reg_tok_ingredients", pattern="\\W")
    recipes = regexTokenizer.transform(recipes)
    has_chili_udf = udf(has_chili, BooleanType())
    chili_recipes = recipes.filter(has_chili_udf(recipes.reg_tok_ingredients ))
    return chili_recipes



def add_difficulty(recipes):
    """ adds a column difficulty to a recipes dataframe

    Parameters:
        recipes: DataFrame containing recipes

    Returns:
        the dataframe recipes having an additional column "difficulty"

    """

    total_time_udf = udf(total_time, IntegerType())
    recipes = recipes.withColumn("total_time", total_time_udf(col("cookTime"), col("prepTime")))
    difficulty_udf = udf(difficulty)
    recipes = recipes.withColumn("difficulty", difficulty_udf(col("total_time")))
    recipes = recipes.drop("reg_tok_ingredients").drop("has_chili").drop("total_time")
    return recipes

def retrieve_recipes_json(sqlc, input_file):
    """ creates a dataframe from a json file"""
    try:
        return sqlc.read.json(input_file)
    except Py4JJavaError as e:
        traceback.print_exc()
        print("{} could not be found - please read the instructions.".format(input_file))

def save_recipes_parquet(recipes, output_dir):
    """ saves a dataframe to a parquet file"""
    recipes.write.parquet(output_dir, mode="overwrite")

if __name__ == "__main__":


    conf = SparkConf().setAppName("find_chili")
    sc = SparkContext(conf=conf)
    sqlc = SQLContext(sc)

    recipes = retrieve_recipes_json(sqlc,INPUT_FILE)
    chili_recipes = filter_by_chili(recipes)
    chili_recipes = add_difficulty(chili_recipes)
    save_recipes_parquet(chili_recipes, OUTPUT_PARQUET)
    if CHECK_SAVE:
        saved_recipes = sqlc.read.parquet(OUTPUT_PARQUET)
        saved_recipes.write.json(VERIFY_OUTPUT_JSON, mode="overwrite")
