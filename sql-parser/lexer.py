from grammar import acceptable

class LexingErrorException(Exception):
	__module__ = 'builtins'

# ========

class Lexer(object):
	def __init__(self, string):
		""" Pass the input string in object initialisation of the lexer
		"""
		self.string = string.strip()

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
		""" A generator to yield each matching token, excluding whitespace tokens
		"""
		self.tokenise()
		for token in self.tokens:
			if(token["token_type"] == "whitespace"):
				continue
			else:
				yield token
