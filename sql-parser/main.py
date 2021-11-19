from a_parser import Parser

if __name__ == "__main__":
	txt = """ 
SELECT col_a, col_b, col_c FROM table_name 
WHERE col_a = 20
AND col_d > 35
OR col_f <= 40
AND col_k = col_j;
	"""
	psr = Parser(txt)
	psr.parse()

# Parseviz (http://brenocon.com/parseviz/)

# (<Query> 
#     (SELECT) 
#     (<columns>
#        (<name>
#            (col_a)
#        )
#        (<punctuation>
#           (,)
#       )
#        (<name>
#            (col_b)
#        )
#        (<punctuation>
#           (,)
#       )
#        (<name>
#          (col_c)
#      )
#     ) 
#     (FROM) 
#    (<name>
#       (table_name) 
#   ) 
#    (WHERE) 
#    (<condition_list>
#     (<condition>
#      (<name>
#       (col_a)
# )
#      (<operator>
#       (=)
# )
#      (<term>
#       (<integer>
#         (20)
# )
# )
# )
#     (<comparator>
#        (AND) 
#    )
#    (<condition>
#      (<name>
#       (col_d)
# )
#      (<operator>
#       (>)
# )
#      (<term>
#       (<integer>
#         (35)
# )
# )
# )
#     (<comparator>
#        (OR)
# )
#     (<condition>
#      (<name>
#       (col_f)
# )
#      (<operator>
#       (<=)
# )
#      (<term>
#       (<integer>
#         (40)
# )
# )
# )
#     (<comparator>
#        (AND)
# )
#     (<condition>
#      (<name>
#       (col_k)
# )
#      (<operator>
#       (=)
# )
#      (<term>
#       (<name>
#         (col_j)
# )
# )
# )

# ) 
#    (<terminal>
#         (;)
#    )
# )