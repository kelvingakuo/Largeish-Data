from grouping_strategies import GroupingAlgos
from faker import Faker
import random

fake = Faker()

methods = ["mpesa", "card", "cash"] #List of payment methods
businesses = [fake.company() for _ in range(10)] #List of random business names

test_data = [] # Final list of dictionaries
for _ in range(5): # Generate 10000 random combinations
    row = {"business_name": random.choice(businesses), "payment_method": random.choice(methods), "amount": random.randint(10, 999999)}
    test_data.append(row)


grouped_output = GroupingAlgos(test_data, on_cols = ["business_name"], agg_col = "amount", agg = ["SUM"], algorithm = "StreamAggregate")

