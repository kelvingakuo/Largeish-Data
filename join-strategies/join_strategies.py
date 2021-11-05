import random
import pprint

def nested_loop_join(left, right, on, how):
	""" For every row in outer, find matching one in inner

		If left/right JOIN and no match on inner, return outer. Else, skip
	"""
	output = []
	
	outer = left if how == "left" else right
	inner = right if how == "left" else left

	outer_cols = list(outer[0].keys())
	inner_cols = list(inner[0].keys())
	all_cols = list(set(inner_cols + outer_cols))

	for row_out in outer:
		match = 0
		attr_out = row_out[on]
		for row_in in inner:
			attr_in = row_in[on]

			if(attr_in == attr_out):
				row_ret = dict(list(row_out.items()) + list(row_in.items()))
				output.append(row_ret)
				match = match + 1

		if(match == 0):
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
		{"id": 1, "attr": "A"},
		{"id": 2, "attr": "AB"},
		{"id": 5, "attr": "AE"},
		{"id": 3, "attr": "AC"},
		{"id": 4, "attr": "AD"}
	]

	tbl2 = [
		{"id": 1, "field": "B"},
		{"id": 1, "field": "BB"},
		{"id": 2, "field": "BC"},
		{"id": 6, "field": "BD"},
		{"id": 7, "field": "BE"}
		
	]

	# SELECT * FROM tb1 LEFT JOIN tbl2 ON tbl1.id = tbl2.id
	out = nested_loop_join(tbl1, tbl2, "id", "left")
	out1 = merge_join(tbl1, tbl2, "id", "inner")
	pprint.pprint(out1)

if __name__ == "__main__":
	main()