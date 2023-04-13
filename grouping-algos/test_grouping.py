from grouping_strategies import GroupingAlgos
from faker import Faker
import random
import pprint

# TODO: Somehow implement unit tests?

fake = Faker()
Faker.seed(5)
methods = ["mpesa", "card", "cash"] #List of payment methods
businesses = [f"{fake.name()} {fake.company_suffix()}" for _ in range(5)] #List of random business names

test_data = [] # Final list of dictionaries
for _ in range(10): # Generate 10000 random combinations
    row = {"business_name": random.choice(businesses), "payment_method": random.choice(methods), "amount": random.randint(random.randint(1, 10), random.randint(100, 1000))}
    test_data.append(row)

grouped_hsh = GroupingAlgos(test_data, on_cols = ["business_name", "payment_method"], agg_col = "amount", agg = "MEDIAN").hashing_aggregate()
grouped_str = GroupingAlgos(test_data, on_cols = ["business_name", "payment_method"], agg_col = "amount", agg = "MEDIAN").streaming_aggregate()

pprint.pprint(grouped_hsh)
print("----------------")
for group in grouped_str:
    pprint.pprint(group)