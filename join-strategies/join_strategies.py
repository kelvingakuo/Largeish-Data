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
			
	

def merge_join(left, right, on, how):
	""" Sort both by JOIN key. Loop through both simulateneously. 

	If matching, return that row. Else, discard the row with the smaller value since it'll never get a match


	"""
	output = []

	sorted_left = sorted(left, key = lambda dic: dic[on])
	sorted_right = sorted(right, key = lambda dic: dic[on])

	i = 0
	while i <= len(sorted_left) and i <= len(sorted_right):
		leff = sorted_left[i]
		rii = sorted_right[i]

		if(leff[on] < rii[on]):
			row_rr = leff


		

	return output

	


def hash_join():
	pass



def main():
	tbl1 = [
		{"id": 1, "tbl1_field": "A"},
		{"id": 2, "tbl1_field": "AB"},
		{"id": 5, "tbl1_field": "AE"},
		{"id": 3, "tbl1_field": "AC"},
		{"id": 4, "tbl1_field": "AD"}
	]

	tbl2 = [
		{"id": 1, "tbl2_field": "B"},
		{"id": 1, "tbl2_field": "BB"},
		{"id": 2, "tbl2_field": "BC"},
		{"id": 6, "tbl2_field": "BD"},
		{"id": 7, "tbl2_field": "BE"}
		
	]

	# SELECT * FROM tb1 LEFT JOIN tbl2 ON tbl1.id = tbl2.id
	l_out = nested_loop_join(tbl1, tbl2, "id", "left")

	# SELECT * FROM tbl2 RIGHT JOIN tbl1 ON tbl2.id = tbl1.id
	r_out = nested_loop_join(tbl1, tbl2, "id", "right")

	pprint.pprint(r_out)
	

if __name__ == "__main__":
	main()