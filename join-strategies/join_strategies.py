import pprint
from typing import List

def nested_loop_join(left: List[dict], right: List[dict], on: str, how: str) -> List[dict]:
	""" Nested loop JOIN strategy: For every row in outer, find matching one in inner

		If left/right JOIN and there's no match on inner, return outer. Else, skip

		Params:
			left (list<dict>) - List of dicts representing one relation
			right (list<dict>) - List of dicts representing the other relation
			on (string) - The key in the relations to JOIN on
			how (string) - The JOIN (left, right, inner) 

		Returns:
			ouput (list<dict>) - A list of dicts that match the JOIN conditions

	"""
	output = []
	
	outer = left if how == "left" else right # If LEFT JOIN, the outer table is the left
	inner = right if how == "left" else left # If LEFT JOIN, the inner table is the right

	outer_cols = list(outer[0].keys())
	inner_cols = list(inner[0].keys())
	all_cols = list(set(inner_cols + outer_cols)) # List of all the possible keys (columns)

	for row_out in outer: # For each row of the outer table
		match = 0
		attr_out = row_out[on]
		for row_in in inner: # Check each row of the inner table
			attr_in = row_in[on]

			if(attr_in == attr_out): # If matching, we need to return all the fields in both relations
				row_ret = dict(list(row_out.items()) + list(row_in.items()))
				output.append(row_ret)
				match = match + 1

		if(match == 0): # If no match, for LEFT and RIGHT JOINs, we need to return the fields in the outer table
			if(how == "left" or how == "right"):
				row_ret = {key : row_out[key] if key in row_out else None for key in all_cols}
				output.append(row_ret)

	return output
			
	

def inner_merge_join(left: List[dict], right: List[dict], on: str) -> List[dict]:
	""" (Sort) Merge JOIN: Sort both by JOIN key. Loop through both simulateneously. 

	If matching, advance inner till no match. If no match, advance the smaller attr

	INNER JOIN only implemented
	"""
	output = []

	sorted_left = sorted(left, key = lambda dic: dic[on])
	sorted_right = sorted(right, key = lambda dic: dic[on])

	outer = sorted_left if len(sorted_left) < len(sorted_right) else sorted_right
	inner = sorted_left if len(sorted_left) > len(sorted_right) else sorted_right

	outer_pointer = 0
	inner_pointer = 0 
	mtches = 0

	while(outer_pointer < len(outer) and inner_pointer < len(inner)):
		outer_row = outer[outer_pointer]
		inner_row = inner[inner_pointer]

		if(outer_row[on] != inner_row[on]):
			# Advance the pointer of the lower attr
			if(outer_row[on] < inner_row[on]):
				outer_pointer = outer_pointer + 1
			else:
				inner_pointer = inner_pointer + 1
		else:
			if(outer_row[on] == inner_row[on]):
				# Return rows and advance inner until no match
				row_ret = dict(list(outer_row.items()) + list(inner_row.items()))
				output.append(row_ret)
				inner_pointer = inner_pointer + 1


	return output
	
def hash_func(ind: int, mp_size: int):
	""" A simple hash function (uses the mid-square method and modulus)

	Params:
		ind (int) - The index to hash
		mp_size (int) - The size of the hash map

	Returns:
		key (int) - The hashed output
	"""
	ind_sq = ind ** 2
	ls_t = int(str(ind_sq)[-2:])
	key = ls_t % mp_size
	return key



def inner_hash_join(left: List[dict], right: List[dict], on: str) -> List[dict]:
	""" Hash JOIN: Compute hash table using smaller table. Hash outer table and probe hash map

	INNER JOIN only implemented
	"""
	output = []
	hash_map = dict()

	inner = left if len(left) < len(right) else right
	outer = left if len(left) > len(right) else right

	# Build
	for record in inner:
		rec_key = hash_func(record[on], 90)
		hash_map[rec_key] = record

	# Probe
	for record in outer:
		rec_key = hash_func(record[on], 90)
		mtch = hash_map.get(rec_key, None)
		if(mtch is None):
			pass
			# No match
		else:
			# Check the records themselves incase collisions
			if(record[on] == mtch[on]):
				row_ret = dict(list(record.items()) + list(mtch.items()))
				output.append(row_ret)

	return output



def main():
	tbl1 = [
		{"id": 11, "tbl1_field": "A"},
		{"id": 22, "tbl1_field": "AB"},
		{"id": 55, "tbl1_field": "AE"},
		{"id": 33, "tbl1_field": "AC"},
		{"id": 44, "tbl1_field": "AD"}
	]

	tbl2 = [
		{"id": 11, "tbl2_field": "B"},
		{"id": 11, "tbl2_field": "BB"},
		{"id": 22, "tbl2_field": "BC"},
		{"id": 66, "tbl2_field": "BD"},
		{"id": 77, "tbl2_field": "BE"},
		{"id": 88, "tbl2_field": "BF"},
		{"id": 44, "tbl2_field": "BG"}
		
	]

	# SELECT * FROM tb1 INNER JOIN tbl2 ON tbl1.id = tbl2.id
	l_out = inner_hash_join(tbl1, tbl2, "id")
	pprint.pprint(l_out)

	# SELECT * FROM tbl2 RIGHT JOIN tbl1 ON tbl2.id = tbl1.id
	# r_out = hash_join(tbl1, tbl2, "id", "right")
	

if __name__ == "__main__":
	main()