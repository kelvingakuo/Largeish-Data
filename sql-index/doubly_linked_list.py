from typing import List


class DoublyLinkedNode(object):
	def __init__(self, value: dict) -> None:
		""""Init a doubly-linked list node

		Params:
			value (dict) - The value of the node as a {col_value, tid} dict for use in our index
		"""
		if(type(value) != dict):
			print("The value of the node needs to be a dict in the format {'col_value': int, 'tid': int}")
		else:
			self.value = value
			self.prev = None
			self.next = None

	def print_node(self) -> str:
		prv = self.prev.value["col_value"] if self.prev is not None else None
		nxt = self.next.value["col_value"] if self.next is not None else None
		return f"([Prev node val: {prv}] Value: {self.value}. [Next node val: {nxt}])"


class DoublyLinkedList(object):
	def __init__(self) -> None:
		""" Init a doubly-linked list. Inits a head node as None
		"""
		self.head = None

	def print_list(self) -> None:
		""" Traverse the doubly linked list printing each node's value, its previous and next nodes
		"""
		head = self.head
		while head is not None:
			prv = head.prev.value["col_value"] if head.prev is not None else None
			nxt = head.next.value["col_value"] if head.next is not None else None
			print(f"[({prv}) <- {head.value} -> ({nxt})]", end = " ")
			head = head.next

	def get_last_node(self) -> DoublyLinkedNode:
		""" Traverses the list to return the last node

		Returns:
			last_node (DoublyLinkedNode) - The last node in the list
		"""
		end_node = self.head
		while end_node.next is not None:
			end_node = end_node.next

		return end_node

	def insert_node(self, new_node: DoublyLinkedNode, position: str, prev_node: DoublyLinkedNode = None) -> None:
		""" Inserts a new node at the start of the list, end of the list or in the middle

		Params:
			new_node (DoublyLinkedNode) - The doubly-linked node to insert
			position (str) - The position to insert the node. Can be "head", "end" or "middle" 
			prev_node (DoublyLinkedNode) - If the position to insert is after another node, indicate which node to insert after
		"""
		if(self.head is None): # O(1)
			self.head = new_node
		else:
			if(position == "head"): # O(1)
				curr_head = self.head
				curr_head.prev = new_node
				self.head = new_node
				self.head.next = curr_head
			elif(position == "end"): # O(n)
				new_node.next = None

				end_node = self.get_last_node()

				end_node.next = new_node
				new_node.prev = end_node
			elif(position == "middle"): # O(1)
				if(prev_node is None):
					print("To insert a node after another, indicate the node to insert after")
				else:
					curr_nxt = prev_node.next
					prev_node.next = new_node
					new_node.next = curr_nxt
					new_node.next.prev = new_node
					new_node.prev = prev_node	

	def delete_node(self, position: str, node: DoublyLinkedNode = None) -> None:
		""" Deletes a node at the start of the list, end of the list or in the middle

		Params:
			node (DoublyLinkedNode) - The doubly-linked node to delete
			position (str) - The position to delete the node from. Can be "head", "end" or "middle" 
			node (DoublyLinkedNode) - To delete from the middle position, indicate which node
		"""
		if(position == "head"):
			self.head = self.head.next
			self.head.prev = None
		elif(position == "end"):
			end_node = self.get_last_node()
			end_node.prev.next = None
		elif(position == "middle"):
			node.prev.next = node.next
			node.next.prev = node.prev

	def find_nodes(self, value: int) -> List:
		""" Returns all the nodes by value

		Params:
			value (int) - The value of the node

		Returns:
		 matches (List<DoublyLinkedNode>) - A list of nodes whose values match
		"""
		matches = []
		hd = self.head
		while hd.next is not None:
			if(hd.value["col_value"] == value):
				matches.append(hd)
			hd = hd.next
		
		return matches


	def sort_list(self) -> None:
		""" TODO: (Merge) Sorts the doubly linked list by node value ascending
		"""
		pass


if __name__ == "__main__":
	ls = DoublyLinkedList()

	hd = DoublyLinkedNode({"col_value": 1, "tid": 10})
	f = DoublyLinkedNode({"col_value": 2, "tid": 20})
	t = DoublyLinkedNode({"col_value": 3, "tid": 30})
	u = DoublyLinkedNode({"col_value": 4, "tid": 40})
	v = DoublyLinkedNode({"col_value": 5, "tid": 50})
	w = DoublyLinkedNode({"col_value": 6, "tid": 60})
	xx = DoublyLinkedNode({"col_value": 25, "tid": 250})

	ls.head = hd
	ls.head.next = f

	f.next = t
	f.prev = ls.head

	t.prev = f
	t.next = u

	u.prev = t
	u.next = v

	v.prev = u
	v.next = w

	w.prev = v

	ls.insert_node(DoublyLinkedNode({"col_value": 5, "tid": 510}), "head")
	ls.insert_node(DoublyLinkedNode({"col_value": 5, "tid": 150}), "end")
	ls.insert_node(DoublyLinkedNode({"col_value": 5, "tid": 109}), "middle", w)
	ls.insert_node(DoublyLinkedNode({"col_value": 5, "tid": 189}), "middle", f)

	ls.print_list()
	print("\n----------------\n")

	nodes = ls.find_nodes(5)
	for node in nodes:
		print(node.print_node())