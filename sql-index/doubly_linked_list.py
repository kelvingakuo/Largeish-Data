from typing import List
from nodes import DoublyLinkedNode
from nodes import BPlusTreeNode
from nodes import obj_id

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
			prv = obj_id(head.prev.value) if head.prev is not None else None
			nxt = obj_id(head.next.value) if head.next is not None else None
			print(f"[({prv}) <- {obj_id(head.value)} -> ({nxt})]", end = " ")
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
			for key in hd.value.keys:
				if(key["col_value"] == value):
					matches.append(hd)
			hd = hd.next
		
		return matches


	def sort_list(self) -> None:
		""" TODO: (Merge) Sorts the doubly linked list by node value ascending
		"""
		pass


if __name__ == "__main__":
	a = {}
	b = {}
	c = {}
	d = {}

	a.next = b
	b.prev = a

	b.next = c
	c.prev = b

	c.next = d
	d.prev = c

	list.head = a