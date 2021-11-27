from hash_index import HashIndex
from b_tree_index import BTreeIndex
import pprint

if __name__ == "__main__":	
	tbl = [
		{"id": 1, "col_a": 7, "col_b": "A", "TID": 101},
		{"id": 2, "col_a": 7, "col_b": "B", "TID": 102},
		{"id": 3, "col_a": 25, "col_b": "C", "TID": 103},
		{"id": 4, "col_a": 30, "col_b": "D", "TID": 104},
		{"id": 5, "col_a": 4, "col_b": "E", "TID": 105},
		{"id": 6, "col_a": 55, "col_b": "F", "TID": 106},
		{"id": 7, "col_a": 99, "col_b": "G", "TID": 107},
		{"id": 8, "col_a": 88, "col_b": "H", "TID": 108},
		{"id": 9, "col_a": 53, "col_b": "I", "TID": 109},
		{"id": 10, "col_a": 14, "col_b": "J", "TID": 110},
		{"id": 11, "col_a": 53, "col_b": "K", "TID": 111},
		{"id": 12, "col_a": 53, "col_b": "L", "TID": 112},
		{"id": 13, "col_a": 99, "col_b": "M", "TID": 113},
		{"id": 14, "col_a": 150, "col_b": "N", "TID": 114},
		{"id": 15, "col_a": 170, "col_b": "O", "TID": 115}
	]
	
	hsh = HashIndex(tbl, "col_a")
	# pprint.pprint(hsh.hash_table)

	# rows = hsh.lookup_using_hash_index(53)
	# pprint.pprint(rows)

	ls = [1, 2, 3, 4]

	print(ls[:2])
	print(ls[-2:])