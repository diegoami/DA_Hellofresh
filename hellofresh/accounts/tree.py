


class Link(object):
    def __init__(self, parent_name, child_name):
        self.parent_name = parent_name
        self.child_name = child_name


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
        self.links = links
        self.nodes = {}
        self.root = Node("Root")
        self.nodes["Root"] = self.root
        self.create_nodes()

    def find_node(self, name):
        if not name in self.nodes:
            self.nodes[name] = Node(name)
        return self.nodes[name]

    def create_nodes(self):
        for link in self.links:
            parent_node = self.find_node(link.parent_name)
            child_node  = self.find_node(link.child_name)
            parent_node.add_child(child_node)

    def find_parent_below_root(self, node_name):
        node = self.nodes[node_name]
        while node.parent != self.root:
            node = node.parent
        return node.name


def find_venture(links, node_name):
    tree = Tree(links)
    venture = tree.find_parent_below_root(node_name)
    return venture




