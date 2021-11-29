import math


class BPlusTreeNode(object):
	def __init__(self, order, keys = [], children = []) -> None:
		""" Init a node of the B+ Tree
		
		Params:
			order (int) - The tree oder
			keys (int) - A list of keys for the node
			children (list<BPlusTreeNode>) - List of children nodes
		"""
		self.order = order
		self.parent = None
		self.keys = keys
		self.children = children
		self.is_leaf = False

	def print_node(self):
		""" Print info about the node
		"""
		print(f"Keys: {self.keys}. Leaf? {self.is_leaf}")
		# . Children: {self.children}. Parent: {self.parent}. ID {self}

	def is_full(self):
		""" A node is full if the number of keys > (order - 1)

		Returns
			(bool) - Whether or not the node is full
		"""
		return len(self.keys) > self.order - 1

	def split_node(self):
		""" A full node is split into two new nodes. A new key is also added to the parent node's keys
		"""
		split = math.ceil(self.order / 2) # Midpoint
		left_keys, right_keys = self.keys[:split], self.keys[split:] # Split the keys of the node
		to_float = self.keys[split] # The value to add to parent node (Pick the right value if order is odd)

		left_len = len(left_keys)
		right_len = len(right_keys)
			
		# Split the children of the node. A node with 2 keys has 3 children, with 3 keys 4 children, 4 keys 5 children etc.
		left_children = self.children[:left_len + 1] 
		right_children = self.children[-right_len:]

		# Create the two new nodes
		new_left_node = BPlusTreeNode(self.order, left_keys, left_children)
		new_right_node = BPlusTreeNode(self.order, right_keys, right_children)

		return to_float, new_left_node, new_right_node

	def update_node_attrs(self):
		""" Recursively runs the insert operation that checks if node is full, if node's parent is full etc.
		https://www.javatpoint.com/b-plus-tree
		https://www.cs.cornell.edu/courses/cs3110/2012sp/recitations/rec25-B-trees/rec25.html

		The function returns when a node no longer needs splitting i.e. isn't full

		For a B+ tree, non-leaf nodes only have numbers as keys i.e. the TID is only in leaf nodes
		"""
		if(not self.is_full()):
			# The node is not full. Continue
			if(self.parent is None):
				# This node doesn't have a parent, hence is the root. Return it
				return self
			else:
				# This node has a parent. Propagate upwards until you find a node without a root, then return it
				first_par = self
				while first_par.parent is not None:
					first_par = first_par.parent
				return first_par
		else:
			# Full node. Split it
			fl, left, right = self.split_node()
			paro = self.parent
			if(paro is None):
				# The split node is the root node
				if(len(self.children) == 0):
					# The split node doesn't have children
					# The created nodes will be leaf nodes with the floated value their parent
					left.is_leaf = True
					right.is_leaf = True
					new_paro = BPlusTreeNode(self.order, keys = [fl["col_value"]], children = [left, right])
					new_paro.is_leaf = False
					left.parent = new_paro
					right.parent = new_paro
					return new_paro.update_node_attrs()
				else:
					# The split node has children
					if(fl in left.keys):
						left.keys.remove(fl)
					if(fl in right.keys):
						right.keys.remove(fl)

					for child in left.children:
						child.parent = left

					for child in right.children:
						child.parent = right

					new_paro = BPlusTreeNode(self.order, keys = [fl["col_value"] if type(fl) == dict else fl], children = [left, right])
					new_paro.is_leaf = False
					left.parent = new_paro
					right.parent = new_paro
					return new_paro.update_node_attrs()
		
			else:
				# The split node has a parent		
				paro.keys.append(fl["col_value"] if type(fl) == dict else fl) # Append the floated key to the parent
				paro.keys = sorted(paro.keys)
				loc = 0
				for i, child in enumerate(paro.children):
					if(child == self):
						loc = i
						break
				
				# The children of the new nodes will be referring to the split node. Change their references to the newly created nodes
				for child in left.children:
					child.parent = left

				for child in right.children:
					child.parent = right


				# Set the parent of the created nodes to the split node's parent
				left.parent = paro
				left.is_leaf = False if len(left.children) != 0 else True
				# Non-leaf nodes shouldn't have repeating keys at different levels
				if(len(left.children) != 0 and left.parent is not None and fl in left.keys):
					left.keys.remove(fl)

				right.parent = paro
				right.is_leaf = False if len(right.children) != 0 else True
				if(len(right.children) != 0 and right.parent is not None and fl in right.keys):
					right.keys.remove(fl)

				# From the list of the split node's parent's children, replace the split node with the two new nodes
				paro.children[loc] = left
				paro.children.insert(loc + 1, right)

				return paro.update_node_attrs()




class DoublyLinkedNode(object):
	def __init__(self, value: BPlusTreeNode) -> None:
		""""Init a doubly-linked list node

		Params:
			value (BPlusTreeNode) - The value of the node as a B+ tree node, specifically leaf node
		"""
		if(type(value) != BPlusTreeNode):
			print("The value of the node needs to be a BPlusTreeNode")
		else:
			self.value = value
			self.prev = None
			self.next = None

	def print_node(self) -> str:
		prv = self.prev.value.print_node() if self.prev is not None else None
		nxt = self.next.value.print_node() if self.next is not None else None
		return f"([Prev node val: {prv}] Value: {self.value}. [Next node val: {nxt}])"

