from grammar import acceptable
import pprint

class LexingErrorException(Exception):
	__module__ = 'builtins'



class Lexer(object):
	def __init__(self, string):
		""" Pass the string in object initialisation of the lexer
		"""
		self.string = string

	def tokenise(self):
		""" Return a list of tokens [{"token_type": value, "token_type": value}] of the string based on grammar

		Raises a custom exception if a token doesn't match
		"""
		matching_tokens, unknown_tokens = acceptable.scan(self.string)

		if(len(unknown_tokens) != 0):			
			raise LexingErrorException(f"Syntax error near: {unknown_tokens.split(' ')[0]}")
		else:
			self.tokens =  matching_tokens		

	def yield_token(self):
		""" Yields each matching token 
		"""
		self.tokenise()
		for token in self.tokens:
			if(token["token_type"] == "whitespace"):
				continue
			else:
				yield token
