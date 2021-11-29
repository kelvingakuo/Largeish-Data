from os import link
from doubly_linked_list import DoublyLinkedList
from nodes import BPlusTreeNode
from nodes import DoublyLinkedNode
import random

def sorted_by_key(ls_dic):
	ff = sorted(ls_dic, key = lambda d: d["col_value"])
	return ff

def linkify_leaf_nodes(tree):
	""" All leaf nodes should be a doubly-linked list

	Params:
		tree (BPlusTree) - The completed BPlus Tree

	Returns:
		tree (LinkifiedBPlusTree) - The same tree but with the leaf nodes as doubly-linked nodes
	"""
	all_the_leaf_nodes = list(tree.root.get_tree_leaves())
	nodified = []

	for leaf in all_the_leaf_nodes:
		nd = DoublyLinkedNode(leaf)
		nodified.append(nd)

	i = 0
	head = None
	while(i  < len(nodified)):
		leaf_node = all_the_leaf_nodes[i]
		nd = nodified[i]
		if(i + 1 != len(nodified)):
			nd.next = nodified[i + 1]
			nodified[i + 1].prev = nd
		else:
			nd.next = None
		if(i == 0):
			head = nd
			linkified = DoublyLinkedList()
			linkified.head = head

		# Replace the leaf node with doubly linked node
		if(leaf_node.parent is not None):
			if(leaf_node in leaf_node.parent.children):
				for j, child in enumerate(leaf_node.parent.children):
					if(child == leaf_node):
						leaf_node.parent.children[j] = nd

		i = i + 1

	return LinkifiedBPlusTree(tree)
	
class BPlusTree(object):
	def __init__(self) -> None:
		""" Init a B-plus tree. The leaf nodes are not a doubly-linked list

		"""
		self.order = 4 # Keys per node (1 - 3). Children per node (2 - 4)
		self.root = BPlusTreeNode(self.order)
		self.root.is_leaf = True

	def print_tree(self):
		""" Traversal of the tree. Technically, traversal of the root node
		"""
		self.root.traverse()
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

	def find_insertion_leaf(self, item):
		""" Traverses the tree to find the correct leaf node to insert the item.

		Params: 
			item (dict) - The item to insert as <key, value> pair.
	
		Returns:
			next_node (BPlusTreeNode) - The leaf node to insert the item
		"""
		value = item["col_value"]
		next_node = self.root
		while(not next_node.is_leaf):
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
					if(value >= keys[i] and value < keys[i+1]):
						next_node = children[i + 1]
						break
		return next_node


class LinkifiedBPlusTree(object):
	def __init__(self, tree: BPlusTree) -> None:
		""" A new B+ tree where the leaf nodes are a doubly-linked list
		"""
		self.root = tree.root

	def find_leaf(self, item):
		""" Similar to 'find_insertion_leaf()' for the BPlusTree but the leaf nodes are a doubly-linked list
		
		Returns:
			next_node (DoublyLinkedNode)
		"""
		value = item
		next_node = self.root
		while(not isinstance(next_node, DoublyLinkedNode)):
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
					if(value >= keys[i] and value < keys[i+1]):
						next_node = children[i + 1]
						break
		return next_node

	def next_prev_keys(self, start_node, direction):
		""" Get all the nodes forward or backwards starting from a start node

		Params:
			start_node (DoublyLinkedNode) - The node to start from
			direction (str) - Direction of traversal (forward or backward)

		Returns:
			matching_tids (list) - All keys for nodes in that direction
		"""
		matching_tids = []
		if(direction == "forward"):
			while(start_node.next is not None):
				start_node = start_node.next
				for key in start_node.value.keys:
					matching_tids.append(key)
		elif(direction == "backward"):
			while(start_node.prev is not None):
				start_node = start_node.prev
				for key in start_node.value.keys:
					matching_tids.append(key)

		return matching_tids

	def equal_to(self, leaf, value):
		""" Return all node keys equal to value. These are keys in the current node and any other forward or backward.
		Keys are not always unique, so multiple nodes may have duplicated keys

		Params:
			leaf (DoublyLinkedNode) - The node to start with
			value (int) - The value to compare with

		Returns:
			matching_tids (list) - All keys for nodes that match
		"""
		matching_tids = []
		for key in leaf.value.keys:
			if(key["col_value"] == value):
				matching_tids.append(key)

			while (leaf.prev is not None and value in leaf.prev.value.keys):
				leaf = leaf.prev
				for key in leaf.prev.value.keys:
					if(key["col_value"] == value):
						matching_tids.append(key)

			while (leaf.next is not None and value in leaf.next.value.keys):
				leaf = leaf.next
				for key in leaf.next.value.keys:
					if(key["col_value"] == value):
						matching_tids.append(key)

		return matching_tids

	def greater_than(self, leaf, value):
		""" Return all node keys greater than the value. These are all keys greater in this node, and all nodes going forward
		
		Params:
			leaf (DoublyLinkedNode) - The node to start with
			value (int) - The value to compare with

		Returns:
			matching_tids (list) - All keys for nodes that match
		"""
		matching_tids = []
		for key in leaf.value.keys:
			if(key["col_value"] > value):
				matching_tids.append(key)
		others = self.next_prev_keys(leaf, "forward")
		matching_tids.extend(others)
		return matching_tids

	def less_than(self, leaf, value):
		""" Return all node keys less than the value. These are all keys lesser than the value in this node, and all nodes going backward
		
		Params:
			leaf (DoublyLinkedNode) - The node to start with
			value (int) - The value to compare with

		Returns:
			matching_tids (list) - All keys for nodes that match
		"""
		matching_tids = []
		for key in leaf.value.keys:
			if(key["col_value"] < value):
				matching_tids.append(key)
		others = self.next_prev_keys(leaf, "backward")
		matching_tids.extend(others)
		return matching_tids


	def lookup_by_value(self, value, operation = '='):
		""" Looks up the correct leaf node(s) by value

		Params:
			value (int) - The value to lookup
			operation (str) - The condition. Accepts '=', '>', '>=', '<', <='

		Returns:
			pointers (list) - List of <key, value> pairs where the col_value matches the lookup value
		"""
		# TODO: Implement between upper and lower bounds
		leaf = self.find_leaf(value)
		
		pointers = []
		if(len(leaf.value.keys) != 0):
			if(operation == '='):
				pointers = self.equal_to(leaf, value)
			elif(operation == '<'):
				pointers = self.less_than(leaf, value)
			elif(operation == '<='):
				equal = self.equal_to(leaf, value)
				less_than = self.less_than(leaf, value)
				pointers = equal + less_than

			elif(operation == '>'):
				pointers = self.greater_than(leaf, value)

			elif(operation == '>='):
				equal = self.equal_to(leaf, value)
				greater_than = self.greater_than(leaf, value)
				pointers = equal + greater_than

			else:
				print("Operation uknown")
			
		return pointers