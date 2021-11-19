from a_parser import Parser

if __name__ == "__main__":
	txt = "SELECT col_a, col_b FROM table_name WHERE col_d = col_e AND col_f > 20;"
	psr = Parser(txt)
	psr.parse()