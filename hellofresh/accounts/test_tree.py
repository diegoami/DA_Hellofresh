import unittest
from .tree import Link, Tree, TreeError


links_ordinary = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("account3", "account4"),
    Link("account4", "account5"),
    Link("account5", "account6"),
]

links_empty_tree = []

links_no_root = [
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("account3", "account4"),
    Link("account4", "account5"),
    Link("account5", "account6")
]

links_two_parents = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("Hellofresh US", "account2")
]

links_invalid_tree = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "Hellofresh US"),
    Link("Hellofresh US", "Hellofresh UK"),

]

links_cyclic_tree = [
    Link("Hellofresh US", "Hellofresh DE"),
    Link("Hellofresh DE", "Hellofresh UK"),
    Link("Hellofresh UK", "Hellofresh DE"),

]


class TestParentBelowRoot(unittest.TestCase):

    def test_tree_ordinary(self):
        tree = Tree(links_ordinary)
        self.assertEqual(tree.find_parent_below_root("account5"),"Hellofresh US")
        self.assertEqual(tree.find_parent_below_root("account2"), "Hellofresh UK")
        self.assertEqual(tree.find_parent_below_root("Hellofresh UK"), "Hellofresh UK")
        self.assertEqual(tree.find_parent_below_root("account10"), None)
        self.assertEqual(tree.find_parent_below_root("Root"), None)

    def test_tree_ordinary_case_insensitive(self):
        tree = Tree(links_ordinary, case_sensitive=False )
        self.assertEqual(tree.find_parent_below_root("account5"),"hellofresh us")
        self.assertEqual(tree.find_parent_below_root("account2"), "hellofresh uk")
        self.assertEqual(tree.find_parent_below_root("Hellofresh UK"), "hellofresh uk")
        self.assertEqual(tree.find_parent_below_root("account10"), None)
        self.assertEqual(tree.find_parent_below_root("Root"), None)

    def test_invalid_trees(self):
        self.assertRaises(TreeError, Tree, links_no_root )
        self.assertRaises(TreeError, Tree, links_empty_tree)
        self.assertRaises(TreeError, Tree, links_two_parents)
        self.assertRaises(TreeError, Tree, links_invalid_tree)
        self.assertRaises(TreeError, Tree, links_cyclic_tree)
