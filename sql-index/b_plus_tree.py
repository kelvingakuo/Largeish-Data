from doubly_linked_list import DoublyLinkedList
import math

class BPlusTreeNode(object):
	def __init__(self, order, keys = [], children = []) -> None:
		self.order = order
		self.parent = None
		self.keys = keys
		self.children = children
		self.is_leaf = False

	def print_node(self):
		print(f"Keys: {self.keys}. Children: {self.children}. Parent: {self.parent}. ID {self}. Leaf? {self.is_leaf}")

	def is_full(self):
		return len(self.keys) > self.order - 1

	def split_node(self):
		split = math.ceil(self.order / 2)
		left_keys, right_keys = self.keys[:split], self.keys[split:]
		to_float = self.keys[split]

		left_len = len(left_keys)
		right_len = len(right_keys)
			
		left_children = self.children[:left_len + 1]
		right_children = self.children[-right_len:]

		new_left_node = BPlusTreeNode(self.order, left_keys, left_children)
		new_right_node = BPlusTreeNode(self.order, right_keys, right_children)

		return to_float, new_left_node, new_right_node

	def update_node_attrs(self):
		if(not self.is_full()):
			if(self.parent is None):
				return self
			else:
				first_par = self
				while first_par.parent is not None:
					first_par = first_par.parent
				return first_par
		else:
			fl, left, right = self.split_node()
			paro = self.parent
			if(paro is None):
				if(len(self.children) == 0):
					left.is_leaf = True
					right.is_leaf = True
					new_paro = BPlusTreeNode(self.order, keys = [fl], children = [left, right])
					new_paro.is_leaf = False
					left.parent = new_paro
					right.parent = new_paro
					return new_paro.update_node_attrs()
				else:
					if(fl in left.keys):
						left.keys.remove(fl)
					if(fl in right.keys):
						right.keys.remove(fl)

					for child in left.children:
						child.parent = left

					for child in right.children:
						child.parent = right

					new_paro = BPlusTreeNode(self.order, keys = [fl], children = [left, right])
					new_paro.is_leaf = False
					left.parent = new_paro
					right.parent = new_paro
					return new_paro.update_node_attrs()
		
			else:
				# Insert and split until not full					
				paro.keys.append(fl)
				paro.keys = sorted(paro.keys)
				loc = 0
				for i, child in enumerate(paro.children):
					if(child == self):
						loc = i
						break

				for child in left.children:
					child.parent = left

				for child in right.children:
					child.parent = right


				left.parent = paro
				left.is_leaf = False if len(left.children) != 0 else True
				if(len(left.children) != 0 and left.parent is not None and fl in left.keys):
					left.keys.remove(fl)

				right.parent = paro
				right.is_leaf = False if len(right.children) != 0 else True
				if(len(right.children) != 0 and right.parent is not None and fl in right.keys):
					right.keys.remove(fl)

				paro.children[loc] = left
				paro.children.insert(loc + 1, right)


				return paro.update_node_attrs()

	
class BPlusTree(object):
	def __init__(self) -> None:
		self.order = 4 # Keys per node (1 - 3). Children per node (2 - 4)
		self.root = BPlusTreeNode(self.order)
		self.root.is_leaf = True

	def print_tree(self):
		""" 
		"""
		self.root.print_node()
		for child in self.root.children:
			print("-")
			child.print_node()
			for chi in child.children:
				chi.print_node()
		print("***************************")


	def insert(self, value):
		# Find the correct leaf node to enter the value
		leaf = self.find_insertion_leaf(value)
		
		leaf.keys.append(value)
		leaf.keys = sorted(leaf.keys)

		self.root = leaf.update_node_attrs()

	def find_insertion_leaf(self, value):
		next_node = self.root
		while not next_node.is_leaf:
			children = next_node.children
			keys = next_node.keys
			lenn = len(keys)
			for i in range(lenn):
				if(i == 0 and value < keys[i]):
					next_node = children[i]
					break
				elif(i == (lenn - 1) and value > keys[i]):
					next_node = children[i + 1]
					break
				else:
					if(value > keys[i] and value < keys[i+1]):
						next_node = children[i + 1]
						break
		return next_node


if __name__ == "__main__":
	bp = BPlusTree()
	bp.insert(1)
	bp.insert(4)
	bp.insert(7)
	bp.insert(10)
	bp.insert(17)
	bp.insert(21)
	bp.insert(31)
	bp.insert(25)
	bp.insert(19)
	bp.insert(20)
	bp.insert(28)
	bp.insert(48)
	bp.insert(51)
	bp.insert(52)
	bp.insert(22)
	bp.insert(23)
	bp.insert(23)
	bp.insert(53)
	bp.insert(54)
	bp.insert(50)
	bp.insert(29)
	bp.insert(30)
	bp.insert(24)
	bp.print_tree()