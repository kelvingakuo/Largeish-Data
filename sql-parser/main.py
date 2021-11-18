from a_parser import Parser

if __name__ == "__main__":
	# txt = "SELECT * FROM table WHERE 1_col_a = 1.5 AND col_b = 250;"
	txt = "SELECT ?? FROM table WHERE col_1 = 1.5 and col_b = 300.0;"
	prsr = Parser(txt)
	
	print(prsr.parse())
	print(prsr.parse())
	print(prsr.parse())
	print(prsr.parse())