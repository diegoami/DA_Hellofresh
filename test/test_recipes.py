from pyspark import SparkContext, SQLContext, SparkConf

from find_recipes_with_chili import has_chili, difficulty, total_time
from find_recipes_with_chili import filter_by_chili, add_difficulty

import json


import unittest





class TestRecipesUdfs(unittest.TestCase):
    def test_has_chili(self):
        self.assertTrue(has_chili(["recipe", "contains", "chili"]))
        self.assertTrue(has_chili(["Recipe", "Contains", "Chili"]))
        self.assertTrue(has_chili(["Recipe", "Contains", "Chilli"]))
        self.assertTrue(has_chili(["Recipe", "Contains", "Chilies"]))
        self.assertTrue(has_chili(["Recipe", "Contains", "Ccilies"]))
        self.assertTrue(has_chili(["Recipe", "Contains", "Ccilies"]))
        self.assertFalse(has_chili(["Recipe", "Contains", "Other", "Food"]))
        self.assertFalse(has_chili(["Recipe", "Contains", "Hulu"]))

    def test_total_time(self):
        self.assertEqual(total_time("PT30M", "PT1H20M"),110)
        self.assertEqual(total_time("PT1H", "PT10M"),70)
        self.assertIsNone(total_time("", "PT10M"))
        self.assertIsNone(total_time("", ""))
        self.assertIsNone(total_time(None, "PT10M"))

    def test_difficulty(self):
        self.assertEqual(difficulty(40), "Medium")
        self.assertEqual(difficulty(20), "Easy")
        self.assertEqual(difficulty(70), "Hard")
        self.assertEqual(difficulty(None), "Unknown")
        self.assertEqual(difficulty(30), "Medium")

class TestProcessRecipes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conf = SparkConf().setAppName("find_chili")
        sc = SparkContext('local[*]')
        sqlc = SQLContext(sc)
        cls.recipes = sqlc.read.json('resources/test_recipes.json')

    def test_count_recipes(self):
        self.assertEqual(self.recipes.count(), 6)

    def test_filter_by_chili(self):
        chilies = filter_by_chili(self.recipes)
        self.assertEqual(chilies.count(), 3)

    def test_difficulty(self):
        recipes_difficulty = add_difficulty(self.recipes)
        self.assertEqual(recipes_difficulty.select('difficulty').rdd.flatMap(lambda x: x).collect(), ["Easy", "Easy", "Medium", "Hard", "Hard", "Unknown"])









