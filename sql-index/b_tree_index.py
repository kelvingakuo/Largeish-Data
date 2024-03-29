from os import link
from typing import List
from b_plus_tree import BPlusTree
from b_plus_tree import linkify_leaf_nodes

class BTreeIndex(object):
	def __init__(self, data: List, col: str) -> None:
		""" Init the b-tree index class

		Params:
			data (list) - Our data as a list of dicts
			col (str) - The column (dict key) to create an index on
		"""
		self.table = data
		self.on = col

		self.bp = BPlusTree()
		self.index = None


		self.create_tree_index()


	def create_tree_index(self):
		""" Create the underlying B+ tree, then linkify the leaf nodes to have a usable index
		"""
		for row in self.table:
			self.bp.insert({"col_value": row[self.on], "tid": row["TID"]})
		
		self.index = linkify_leaf_nodes(self.bp)

	def show_index(self):
		self.bp.print_tree()


	def lookup_using_tree_index(self, value, operation = '='):
		self.redone_table = {row["TID"] : row for row in self.table}
		relevant_tids = self.index.lookup_by_value(value, operation)

		rows = []
		for relevant in relevant_tids:
			rr = self.redone_table[relevant["tid"]]
			rows.append(rr)
			
		return rows