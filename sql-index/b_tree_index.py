from typing import List

class BTreeIndex(object):
	def __init__(self, data: List, col: str) -> None:
		""" Init the b-tree index class

		Params:
			data (list) - Our data as a list of dicts
			col (str) - The column (dict key) to create an index on
		"""
		self.table = data
		self.on = col

		self.create_tree_index()


	def create_tree_index(self):
		pass


	def lookup_using_tree_index(self, value):
		pass