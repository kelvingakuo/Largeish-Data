from a_parser import Parser

if __name__ == "__main__":
	txt = "FROM * SELECT ABCD"
	psr = Parser(txt)
	psr.parse()