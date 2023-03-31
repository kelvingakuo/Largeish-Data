from grouping_strategies import GroupingAlgos
from faker import Faker
import random

fake = Faker()
Faker.seed(5)
methods = ["mpesa", "card", "cash"] #List of payment methods
businesses = [f"{fake.name()} {fake.company_suffix()}" for _ in range(2)] #List of random business names

test_data = [] # Final list of dictionaries
for _ in range(10): # Generate 10000 random combinations
    row = {"business_name": random.choice(businesses), "payment_method": random.choice(methods), "amount": random.randint(random.randint(1, 10), random.randint(100, 1000))}
    test_data.append(row)

grouper_sum = GroupingAlgos(test_data, on_cols = ["business_name"], agg_col = "amount", agg = "SUM")
grouper_max = GroupingAlgos(test_data, on_cols = ["business_name"], agg_col = "amount", agg = "MAX")
grouper_min = GroupingAlgos(test_data, on_cols = ["business_name"], agg_col = "amount", agg = "MIN")
grouper_avg = GroupingAlgos(test_data, on_cols = ["business_name"], agg_col = "amount", agg = "AVG")
grouper_med = GroupingAlgos(test_data, on_cols = ["business_name"], agg_col = "amount", agg = "MEDIAN")

grouper_sum_output = grouper_sum.streaming_aggregate()
grouper_max_output = grouper_max.streaming_aggregate()
grouper_min_output = grouper_min.streaming_aggregate()
grouper_avg_output = grouper_avg.streaming_aggregate()
grouper_med_output = grouper_med.streaming_aggregate()

for group in grouper_sum_output:
    print(group)
print("-------------------")
for group in grouper_max_output:
    print(group)
print("-------------------")
for group in grouper_min_output:
    print(group)
print("-------------------")
for group in grouper_avg_output:
    print(group)
print("-------------------")
for group in grouper_med_output:
    print(group)
print("-------------------")
