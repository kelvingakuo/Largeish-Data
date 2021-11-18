from a_parser import Parser

if __name__ == "__main__":
	txt = "SELECT col_a, col_b FROM table_x WHERE col_a = 30 OR col_c = col_f OR col_d > 55;"
	psr = Parser(txt)
	psr.parse()