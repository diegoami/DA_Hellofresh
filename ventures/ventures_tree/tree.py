
class Link(object):
    """ a link representing a parent child relation between two entities"""
    def __init__(self, parent_name, child_name):
        self.parent_name = parent_name
        self.child_name = child_name


class TreeError(Exception):
    """Generic exception thrown when the tree could not be built correctly

    """

    def __init__(self, message):
        self.message = message


class Node:
    """A node in a tree"""

    def __init__(self, name):
        self.name = name
        self.children = []
        self.parent = None

    def add_child(self, node):
        """ creates a parent-child relation between two nodes

        """
        self.children.append(node)
        node.parent = self


class Tree(object):
    """Tree structure containing the hierarchy of ventures and accounts"""

    def __init__(self, links):
        """ initializes the Tree with a list of Link objects"""

        self.nodes = {}
        self.create_nodes(links)
        self.root = self.identify_root()
        self.ventures = self.identify_ventures()

    def identify_root(self):
        """ identifies the root in an initialized tree """

        nodes_without_parents = [node_name for node_name, node in self.nodes.items() if not node.parent]
        if len(nodes_without_parents ) > 1:
            raise TreeError("Cannot find root. Following ventures have no parent : {}".format(", ".join(nodes_without_parents)))
        elif len(nodes_without_parents ) == 0:
            raise TreeError("Cannot find root in ventures_tree")
        else:
            return self.nodes[nodes_without_parents[0]]

    def identify_ventures(self):
        """ identifies the ventures in an initalized tree (root's children) """
        if not self.root:
            raise TreeError("Tree has no root")
        if not self.root.children:
            raise TreeError("Root has no children")
        else:
            return self.root.children

    def find_or_create_node(self, node_name):
        """ finds a node in the tree or create it"""
        if not node_name in self.nodes:
            self.nodes[node_name] = Node(node_name)
        return self.nodes[node_name]

    def create_nodes(self, links):
        """ create the nodes of the tree using the links' list passed in the constructor """
        for link in links:
            parent_node = self.find_or_create_node(link.parent_name)
            child_node  = self.find_or_create_node(link.child_name)
            if child_node.parent:
                raise TreeError("{} has at least two parents : {}, {}".format(link.child_name, child_node.parent.name, link.parent_name))
            parent_node.add_child(child_node)

    def find_venture(self, account_name):
        """ returns the venture owning the account """
        if account_name not in self.nodes:
            return None
        node = self.nodes[account_name]
        while node != None and  node not in self.ventures:
            node = node.parent
        return node.name if node else None


def find_venture(links, account_name):
    """ given a list of Link representing a hierarchy, returns the venture owning an account

    Parameters:
        links: list of Link objects representing the company's hierarchy
        account_name : the name of an account in the company's hierarchy

    Returns:
        the name of the venture owning the account

    """
    tree = Tree(links)
    venture = tree.find_venture(account_name)
    return venture




