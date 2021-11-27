from doubly_linked_list import DoublyLinkedList


class BPlusTreeNode(object):
	def __init__(self, order, keys = [], children = []) -> None:
		self.order = order
		self.parent = None
		self.keys = keys
		self.children = children
		self.is_leaf = False

	def is_full(self):
		return len(self.keys) == self.order
		



class BPlusTree(object):
	def __init__(self) -> None:
		self.order = 4 # Keys per node (1 - 3). Children per node (2 - 4)
		self.root = BPlusTreeNode(self.order)


	def insert(self, value):
		pass

	def print_tree(self):
		pass

	def split_node(self, node):
		split = int(self.order / 2)
		left_keys, right_keys = node.keys[:split], node.keys[-split:]
		left_children, right_children = node.children[:split], node.children[-split:]
		to_float = node.keys[split]

		new_left_node = BPlusTreeNode(self.order, left_keys, left_children)
		new_right_node = BPlusTreeNode(self.order, right_keys, right_children)


	def lookup(self, value):
		pass


if __name__ == "__main__":
	bp = BPlusTree()

	nd = BPlusTreeNode(4)
	nd.keys = [5, 4, 8, 9]

	bp.split_node(nd)
	
