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

ss_su = GroupingAlgos(test_data, on_cols = ["business_name"], agg_col = "amount", agg = "SUM")
ds_su = GroupingAlgos(test_data, on_cols = ["business_name", "payment_method"], agg_col = "amount", agg = "SUM")

pprint.pprint(ss_su.hashing_aggregate())
print("----------------")
pprint.pprint(list(ss_su.streaming_aggregate()))