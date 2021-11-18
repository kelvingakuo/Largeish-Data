from lexer import Lexer
from grammar import bnf
import sys

class Node(object):
	def __init__(self, val):
		self.value = val #Value of node
		self.right_child = None # Child node to the right
		self.left_child = None # Child node to the left

	def show_tree(self):
		pass

class ParsingErrorException(Exception):	
	__module__ = 'builtins'


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
					print("No more tokens. Exiting....")
					sys.exit()
				else:
					raise ParsingErrorException(f"Semi-colon (;) expected at the end of the query")
			return True
		else:
			if(raise_err):
				raise ParsingErrorException(f"Unexpected token {self.next_token}. Expected {expected}. Please refer to the grammar {bnf}")
			return False


	def query(self):
		""" <query> ::= "SELECT " <columns> " FROM " <name> <terminal> | "SELECT " <columns> " FROM " <name> " WHERE " <conditionList> <terminal>
		"""
		self.tree = Node("Query")
		if(self.accept("SELECT")):
			if(self.columns()):
				if(self.accept("FROM")):
					self.name()
					if(self.terminal(False)):
						pass
					elif(self.accept("WHERE")):
						if(self.condition_list()):
							self.terminal(True)
					


	def columns(self):
		""" <columns> ::= (<name> ", ")+ | "*"

		Accepts:
			- *
			- col_a
			_ col_a, col_b
		"""

		if(self.accept("all_cols", False)):
			return True
		else:
			if(self.accept("name")):
				if(self.accept("punctuation", False)):
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
		if(self.condition()):
			if(self.comparator()):
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
			if(self.operator()):
				if(self.term()):
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

	def parse(self):
		""" Our entry point
		"""
		self.query()
