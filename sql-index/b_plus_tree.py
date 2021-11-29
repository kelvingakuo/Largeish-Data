from doubly_linked_list import DoublyLinkedList
from nodes import BPlusTreeNode
import random

def sorted_by_key(ls_dic):
	ff = sorted(ls_dic, key = lambda d: d["col_value"])
	return ff

	
class BPlusTree(object):
	def __init__(self) -> None:
		""" Init a B-plus tree
		"""
		self.order = 4 # Keys per node (1 - 3). Children per node (2 - 4)
		self.root = BPlusTreeNode(self.order)
		self.root.is_leaf = True

	def print_tree(self):
		""" A poorly implemented pre-order traversal of tree. Needs to re-implemented recursively as will fail where the tree depth > 3
		"""
		self.root.print_node()
		for child in self.root.children:
			print("-")
			child.print_node()
			for chi in child.children:
				chi.print_node()
		print("***************************")


	def insert(self, item):
		""" Inserts an item into the tree

		Params:
			item (dict) - The item to insert as <key, value> pair
		"""
		leaf = self.find_insertion_leaf(item)
		
		leaf.keys.append(item)
		leaf.keys = sorted_by_key(leaf.keys)
		
		self.root = leaf.update_node_attrs()
		self.linkify_leaf_nodes()

	def lookup_by_value(self, value):
		""" Lookups the correct leaf node by value

		Params:
			value (int) - The value to lookup

		Returns:
			pointer (list) - List <key, value> pairs where the col_value matches the lookup value
		"""
		leaf = self.find_insertion_leaf(value, "lookup")
		if(len(leaf.keys) != 0):
			matching_tids = []
			for key in leaf.keys:
				if(key["col_value"] == value):
					matching_tids.append(key)

			# TODO: The key may not always be unique. To make sure we get everything, we need to walk forwards and backwards through the doubly-linked list until we don't have the value
			return matching_tids
		else:
			return []

	def find_insertion_leaf(self, item, operation = "insert"):
		""" Traverses the tree to find the correct leaf node to insert the item. Also used for lookup

		Params: 
			item (dict/ int) - The item to insert as <key, value> pair. Or as int
			operation (str) - Specify the operation we're using the method for

		Returns:
			next_node (BPlusTreeNode) - The leaf node to insert the item
		"""
		value = item["col_value"] if operation == "insert" else item
		next_node = self.root
		while not next_node.is_leaf:
			children = next_node.children
			keys = next_node.keys
			lenn = len(keys)
			for i in range(lenn):
				if(i == 0 and value < keys[i]):
					next_node = children[i]
					break
				elif(i == (lenn - 1) and value >= keys[i]):
					next_node = children[i + 1]
					break
				else:
					if(value >= keys[i] and value <= keys[i+1]):
						next_node = children[i + 1]
						break
		return next_node

	def linkify_leaf_nodes(self):
		""" All leaf nodes should be a doubly-linked list
		"""
		pass

if __name__ == "__main__":
	bp = BPlusTree()
	values = [1, 4, 7, 10, 17, 31, 21, 25, 19, 20, 28, 48, 51, 52, 22, 23, 23, 53, 54, 50, 29, 30, 24, 23, 23]
	# , 25, 19, 20, 28, 48, 51, 52, 22, 23, 23, 53, 54, 50, 29, 30, 24
	for val in values:
		bp.insert({"col_value": val, "tid": val * 113 * random.random()})
	bp.print_tree()
	ff = bp.lookup_by_value(23)
	print(ff)