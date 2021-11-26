from typing import List

class HashIndex(object):
	def __init__(self, data: List, col: str) -> None:
		""" Init the hash index class

		Params:
			data (list) - Our data as a list of dicts
			col (str) - The column (dict key) to create an index on
		"""
		self.table = data
		self.on = col

		self.create_hash_index()

	def hash_function(self, key: int) -> tuple:
		""" A hash function that generates a simple hash of our key, and a bucket value

		Params:
			key (int)- The value of the column we want to hash

		Returns:
			a_hash (int) - A hash of the key
			bucket_no (int) - The bucket number to insert the value
		"""
		buckets = 97 # An arbitrary number of buckets

		# Generate a hash (square the number twice then extract 4 chars )
		keyy = key ** 2 ** 2
		a_hash = int(str(keyy)[1:5])

		# Get the bucket number
		bucket_no = a_hash % buckets

		return a_hash, bucket_no


	def create_hash_index(self) -> None:
		""" Creates a hash index of our data based on the column
		"""
		self.hash_table = dict()

		for row in self.table:
			hsh, buck = self.hash_function(row[self.on])
			if(self.hash_table.get(buck, None) is None):
				# Empty bucket, insert TID
				self.hash_table[buck] = []
				self.hash_table[buck].append({"hash_code": hsh, "row_tid": row["TID"]})
			else:
				# Perform chaining 
				val = {"hash_code": hsh, "row_tid": row["TID"]}
				current_vals = self.hash_table[buck]
				current_vals.append(val)
				self.hash_table[buck] = current_vals

	def lookup_using_hash_index(self, value: str) -> list:
		""" Return all the rows where the indexed column contains a value

		Params:
			value (str) - The condition

		Returns:
			rows (list) - The matching rows from our table
		"""

		# Re-structure the table into a searchable dict
		self.redone_table = {row["TID"] : row for row in self.table}

		rows = []
		hsh, bucket = self.hash_function(value)

		items = self.hash_table.get(bucket, None)
		if(items is None):
			# No match
			pass
		else:
			if(len(items) == 1):
				# We have just one TID
				rows.append(self.redone_table[items[0]["row_tid"]])
			else:
				# Multiple TIDs in one slot
				relevant_tids = [item["row_tid"] for item in items if item["hash_code"] == hsh]
				
				# Retrieve each relevant row then re-check condition
				for relevant in relevant_tids:
					rr = self.redone_table[relevant]
					if(rr["col_a"] == value):
						rows.append(rr)

		return rows