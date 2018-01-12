import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf, lower

sc = pyspark.SparkContext('local[*]')
sqlc = SQLContext(sc)
recipes = sqlc.read.json("data/recipes.json")
recipes = recipes.withColumn("ingredients_lower", lower(col("ingredients")))
tokenizer = Tokenizer(inputCol="ingredients_lower", outputCol="tok_ingredients")
recipes = tokenizer.transform(recipes)
regexTokenizer = RegexTokenizer(inputCol="ingredients_lower", outputCol="reg_tok_ingredients", pattern="\\W")
recipes = regexTokenizer.transform(recipes)
countTokens = udf(lambda words: len(words))
recipes = recipes.withColumn("cnt_tok", countTokens(col("reg_tok_ingredients")))



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

    ckm, ptm  = get_minutes(cook_time), get_minutes(prep_time)

    return ckm + ptm

has_chili_udf = udf(has_chili)
recipes = recipes.withColumn("has_chili", has_chili_udf(col("reg_tok_ingredients")))
print(recipes.take(5))

chili = recipes.filter("has_chili = 'true'")
print(chili.take(5))

total_time_udf = udf(total_time)
chili = chili.withColumn("total_time", total_time_udf(col("cookTime"), col("prepTime")) )
print(chili.take(5))
