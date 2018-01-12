


class Link(object):
    def __init__(self, parent_name, child_name):
        self.parent_name = parent_name
        self.child_name = child_name

links = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("account3", "account4"),
    Link("account4", "account5"),
    Link("account5", "account6"),
]


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



if __name__ == "__main__":
    print(find_venture(links, "account5"))
    print(find_venture(links, "account2"))


