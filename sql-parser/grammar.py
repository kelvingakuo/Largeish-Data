import re

# Accepted tokens
keywords = r"(SELECT|WHERE|FROM|AND|OR|NOT)"
patterns = [
	(keywords, lambda scanner, token: {"token_type": "keyword", "token": token}),
	(r"[a-zA-Z_][a-zA-Z_0-9]*", lambda s, t: {"token_type": "name", "token": t}),
	(r"\*", lambda s, t: {"token_type": "all_cols","token":  t}),
	(r"[=>>=<<=]", lambda s, t: {"token_type": "operator","token":  t}),
	(r"[-+]?\d*\.\d+", lambda s, t: {"token_type": "float","token":  t}),
	(r"\d+", lambda s, t: {"token_type": "integer","token":  t}),
	(r"[,]", lambda s, t: {"token_type": "punctuation","token":  t}),
	(r"[" " \\n]", lambda s, t: {"token_type": "whitespace","token":  t}),
	(r";", lambda s, t: {"token_type": "terminal","token":  t})
	# (r".", lambda s, t: None) # Skip tokens that couldn't be matched
]

acceptable = re.Scanner(patterns)

# Rules AKA productions in BNF

bnf = """
	<query> ::= "SELECT " <columns> " FROM " <name> <terminal> | "SELECT " <columns> " FROM " <name> " WHERE " <conditionList> <terminal>
	<columns> ::= (<name> ", ")+ | "*"
	<name> ::= <letter>+ | <letter>+ "_" | <letter>+ "_" <digit>+
	<conditionList> ::= <condition> <comparator> <condition>
	<comparator> ::= " AND " | " OR "
	<condition> ::= <name> <operator> <term>
	<operator> ::= " = " | " > " | " >= " | " < " | " <= "
	<term> ::= <digit> | <digit> "." <digit> | <name>
	<letter> ::= [a-z]+ | [A-Z]+
	<digit> ::= [1-9]+
	<terminal> ::= ";"
"""



