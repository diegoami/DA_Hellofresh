import unittest
from .tree import Link, Tree, Node, find_venture


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


class TestOrdinaryTree(unittest.TestCase):
    def setUp(self):
        self.links = links_ordinary
        self.tree = Tree(self.links)

    def test_find_parent_below_root(self):

        self.assertEqual(self.tree.find_parent_below_root("account5"),"Hellofresh US")
        self.assertEqual(self.tree.find_parent_below_root("account2"), "Hellofresh UK")
