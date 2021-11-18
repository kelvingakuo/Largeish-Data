from lexer import Lexer
from grammar import bnf


class ParsingErrorException(Exception):	
	__module__ = 'builtins'


class Parser(object):
	def __init__(self, input_txt):
		self.input = input_txt
		lexx = Lexer(self.input)
		self.tokens = lexx.yield_token()
		self.current_token = next(self.tokens)
	
	def accept(self, expected, raise_err):
		""" Helper function to check if the current token is what we expect

		Params:
			expected (str) - Either the exact token we expect e.g. "SELECT" or the token type we expect e.g. "name"
			raise_err (bool) - Whether or not to raise an error and halt program

		Returns:
			(bool) - Whether the current token is what's expected
		"""
		if(self.current_token["token"] == expected or self.current_token["token_type"] == expected):
			self.current_token = next(self.tokens)
			return True
		else:
			if(raise_err):
				raise ParsingErrorException(f"Unexpected token {self.current_token}. Please refer to the language {bnf}")
			return False


	def query(self):
		pass

	def parse(self):
		return self.current_token
