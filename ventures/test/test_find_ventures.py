import unittest
from ventures_tree import find_venture
from .links import *

class TestFindVentures(unittest.TestCase):

    def test_tree_ordinary(self):

        self.assertEqual(find_venture(links_ordinary,"account5"),"Hellofresh US")
        self.assertEqual(find_venture(links_ordinary,"account2"), "Hellofresh UK")
        self.assertEqual(find_venture(links_ordinary,"Hellofresh UK"), "Hellofresh UK")
        self.assertEqual(find_venture(links_ordinary,"account10"), None)
        self.assertEqual(find_venture(links_ordinary,"Root"), None)
        self.assertEqual(find_venture(links_ordinary,None), None)

    def test_second_tree(self):
        self.assertEqual(find_venture(links_second_tree, "UK2"), "UK")
        self.assertEqual(find_venture(links_second_tree, "D2B"), "D")
        self.assertEqual(find_venture(links_second_tree, "US1A"), "US")
        self.assertEqual(find_venture(links_second_tree, "US3C"), None)
        self.assertEqual(find_venture(links_second_tree, "DE4B"), None)
        self.assertEqual(find_venture(links_second_tree, None), None)

    def test_invalid_trees(self):
        self.assertRaises(TreeError, find_venture, links_no_root, None )
        self.assertRaises(TreeError, find_venture, links_empty_tree, None)
        self.assertRaises(TreeError, find_venture, links_two_parents, None)
        self.assertRaises(TreeError, find_venture, links_invalid_tree, None)
        self.assertRaises(TreeError, find_venture, links_cyclic_tree, None)
