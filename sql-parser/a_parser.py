from lexer import Lexer
from grammar import bnf
import pprint

class ParsingErrorException(Exception):	
	__module__ = 'builtins'

# ================

class Node(object):
	def __init__(self, val):
		self.value = val # Value of node
		self.children = [] # Can have multiple children

	def add_child(self, node):
		self.children.append(node)
		return node

def show_tree(tree):
	""" (Level order) Traverse and print parse tree
	"""
	queue = []
	queue.append(tree)

	while(len(queue) != 0):
		items = len(queue)

		while(items > 0):
			node = queue[0]
			queue.pop(0)

			kids = len(node.children)
			if(kids == 0):
				print(node.value, end = ' ')
			else:
				print(f"<{node.value}>", end = ' ')

			for i in range(kids):
				queue.append(node.children[i])
			items = items - 1
		print()

# ================

class Parser(object):
	def __init__(self, input_txt):
		self.input = input_txt
		lexx = Lexer(self.input)
		self.tokens = lexx.yield_token()
		self.next_token = None
		self.current_token = None
		self.advance()

	def advance(self):
		""" Advance the tokens in use
		"""
		self.current_token, self.next_token = self.next_token, next(self.tokens, None)

	
	def accept(self, expected, raise_err = True):
		""" Helper function to check if the next token is what we expect

		Params:
			expected (str) - Either the exact token we expect e.g. "SELECT" or the token type we expect e.g. "name"
			raise_err (bool) - Whether or not to raise an error and halt program

		Returns:
			(bool) - Whether the next token is what's expected
		"""
		if(self.next_token["token"] == expected or self.next_token["token_type"] == expected):
			print(f"{self.next_token} == ({expected})")
			self.advance()
			if(self.next_token is None):
				if(self.current_token["token_type"] == "terminal"):
					print("No more tokens. Printing parse tree.... \n")
				else:
					raise ParsingErrorException(f"Semi-colon (;) expected at the end of the query")
			return True
		else:
			if(raise_err):
				raise ParsingErrorException(f"Unexpected token {self.next_token}. Expected {expected}. Please refer to the grammar {bnf}")
			return False

	
	def parse(self):
		""" Our entry point
		"""
		self.query()


	def query(self):
		""" <query> ::= "SELECT " <columns> " FROM " <name> <terminal> | "SELECT " <columns> " FROM " <name> " WHERE " <conditionList> <terminal>
		"""
		self.tree = Node("Query")
		if(self.accept("SELECT")):
			self.tree.add_child(Node("SELECT"))
			self.cols_node = self.tree.add_child(Node("columns"))
			if(self.columns()):
				if(self.accept("FROM")):
					self.tree.add_child(Node("FROM"))
					self.tbl_name_node = self.tree.add_child(Node("name"))
					if(self.name()):
						self.tbl_name_node.add_child(Node(self.current_token["token"]))
						if(self.terminal(False)):
							self.termin = self.tree.add_child(Node("terminal"))
							self.termin.add_child(Node(';'))
							show_tree(self.tree)
						elif(self.accept("WHERE")):
							self.tree.add_child(Node("WHERE"))
							self.conds_node = self.tree.add_child(Node("condition_list"))
							if(self.condition_list()):
								if(self.terminal(True)):
									self.termin = self.tree.add_child(Node("terminal"))
									self.termin.add_child(Node(';'))
									show_tree(self.tree)
					


	def columns(self):
		""" <columns> ::= (<name> ", ")+ | "*"

		Accepts:
			- *
			- col_a
			_ col_a, col_b
		"""
		if(self.accept("all_cols", False)):
			self.all_cols = self.cols_node.add_child(Node("all_cols"))
			self.all_cols.add_child(Node("*"))
			return True
		else:
			if(self.accept("name")):
				self.a = self.cols_node.add_child(Node("name"))
				self.a.add_child(Node(self.current_token["token"]))
				if(self.accept("punctuation", False)):
					self.b = self.cols_node.add_child(Node("punctuation"))
					self.b.add_child(Node(self.current_token["token"]))
					self.columns()
				else:
					return True
			else:
				return False
		
			return True
		
	def name(self, is_exp = True):
		""" Accepts tokens of type 'name'
		"""
		if(self.accept("name", is_exp)):
			return True

	def condition_list(self):
		""" <conditionList> ::= <condition> <comparator> <condition>

		Accepts:
			- col_a = 20 
			- col_a = 20 AND col_b = 30
			- col_a = 20 OR col_b = col_c
		"""
		self.a = self.conds_node.add_child(Node("condition"))
		if(self.condition()):
			if(self.comparator()):
				self.b = self.conds_node.add_child(Node("comparator"))
				self.b.add_child(Node(self.current_token["token"]))
				self.condition_list()
			return True
			
			

	def comparator(self):
		""" <comparator> ::= " AND " | " OR "

		Accepts:
			- AND
			- OR
		"""
		if(self.accept("AND", False) or self.accept("OR", False)):
			return True

	def condition(self):
		""" <condition> ::= <name> <operator> <term>

		Accepts:
			- col_a = 20
			- col_a = 20.5
			- col_a = col_b
		"""
		if(self.name()):
			self.nm = self.a.add_child(Node("name"))
			self.nm.add_child(Node(self.current_token["token"]))
			if(self.operator()):
				self.op = self.a.add_child(Node("operator"))
				self.op.add_child(Node(self.current_token["token"]))
				if(self.term()):
					self.tm = self.a.add_child(Node("term"))
					self.tmm = self.tm.add_child(Node(self.current_token["token_type"]))
					self.tmm.add_child(Node(self.current_token["token"]))
					return True

	def term(self):
		""" <term> ::= <digit> | <digit> "." <digit> | <name>
		"""
		if(self.accept("integer", False) or self.accept("float", False) or self.accept("name", False)):
			return True

	def terminal(self, to_raise = True):
		""" Accepts tokens of type 'terminal'
		"""
		if(self.accept("terminal", to_raise)):
			return True

	def operator(self):
		""" Accepts tokens of type 'operator'
		"""
		if(self.accept("operator")):
			return True


# <Query>
# SELECT <columns> 							FROM <name> 				<condition_list>
# 		<name> <punctuation> <name> 			table_name 			<condition> 			<comparator> 		<condition>
# 		col_a 		, 			col_b 							<name> <operator> <term> 		AND 		<name>  <operator>  <term>
# 																col_d 		= 	  <name> 					col_f 		> 		<integer>
# 																				   col_e 											20