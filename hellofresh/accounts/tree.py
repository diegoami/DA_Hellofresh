import logging


class Link(object):
    def __init__(self, parent_name, child_name):
        self.parent_name = parent_name
        self.child_name = child_name


class TreeError(Exception):
    def __init__(self, message):
        self.message = message


class Node:
    def __init__(self, name):
        self.name = name
        self.children = []
        self.parent = None

    def add_child(self, node):
        self.children.append(node)
        node.parent = self

class Tree(object):
    def __init__(self, links):

        self.nodes = {}
        self.create_nodes(links)
        self.root = self.find_root()
        self.ventures = self.find_ventures()

    def find_root(self):
        nodes_without_parents = [node_name for node_name, node in self.nodes.items() if not node.parent]
        if len(nodes_without_parents ) > 1:
            raise TreeError("Cannot find root. Following accounts have no parent : {}".format(", ".join(nodes_without_parents)))
        elif len(nodes_without_parents ) == 0:
            raise TreeError("Cannot find root in tree")
        else:
            return self.nodes[nodes_without_parents[0]]

    def find_ventures(self):
        if not self.root:
            raise TreeError("Tree has no root")
        if not self.root.children:
            raise TreeError("Root has no children")
        else:
            return self.root.children

    def find_or_create_node(self, node_name):
        if not node_name in self.nodes:
            self.nodes[node_name] = Node(node_name)
        return self.nodes[node_name]

    def create_nodes(self, links):
        for link in links:
            parent_node = self.find_or_create_node(link.parent_name)
            child_node  = self.find_or_create_node(link.child_name)
            if child_node.parent:
                raise TreeError("{} has at least two parents : {}, {}".format(link.child_name, child_node.parent.name, link.parent_name))
            parent_node.add_child(child_node)

    def find_parent_below_root(self, node_name):
        if node_name not in self.nodes:
            return None
        node = self.nodes[node_name]
        while node != None and  node not in self.ventures:
            node = node.parent
        return node.name if node else None


    def verify(self):
        if not self.root:
            raise TreeError("Tree has no root")
        if not self.root.children:
            raise TreeError("Root has no children")
        node_without_parents = [node_name for node_name, node in self.nodes.items() if node != self.root and not node.parent]
        if len(node_without_parents) > 0:
            raise TreeError("Following accounts have no parent : {}".format(", ".join(node_without_parents )))

def find_venture(links, node_name):
    tree = Tree(links)
    venture = tree.find_parent_below_root(node_name)
    return venture




