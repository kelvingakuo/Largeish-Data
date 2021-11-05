import pandas as pd
from faker.providers.person.en import Provider
from faker import Faker
import random
import string

# 150K rows
# Table 1 - person_id, first_name, last_name
# Table 2 - person_id, transaction_id
# Table 3 - transaction_id, transaction_amount, transaction_date_time, transaction_currency
first_names = list(set(Provider.first_names))
last_names = list(set(Provider.last_names))

people = []

for first in first_names:
	for last in last_names:
		alpha = random.choice(string.ascii_letters)
		nm = {"first_name": f"{first}{alpha}ww", "last_name": f"{last}{alpha}ll"}
		people.append(nm)

people_with_ids = []

for k, person in enumerate(people):
	nm = {"first_name": person["first_name"], "last_name": person["last_name"], "person_id": k}
	people_with_ids.append(nm)

table_1 = pd.DataFrame(people_with_ids)
table_1.to_csv("db-data/persons.csv", index = False)

peeps_with_transactions = random.sample(table_1["person_id"].tolist() + table_1["person_id"].tolist(), 1000000)
transaction_ids = [*range(0, 1000000)]

people_with_transactions = []

i = 0
while i < len(peeps_with_transactions):
	peep = peeps_with_transactions[i]
	id = transaction_ids[i]
	pee_trans = {"person_id_fk": peep, "transaction_id_fk": id}
	people_with_transactions.append(pee_trans)
	i = i + 1

table_2 = pd.DataFrame(people_with_transactions)
table_2.to_csv("db-data/people_transactions.csv", index = False)


transes = []
fake = Faker()
for trns_id in transaction_ids:
	amount = random.randint(0, 3000000)
	dtt = fake.date_time().strftime("%Y-%m-%dT%H:%M:%SZ")
	currency = "KES"

	a_trans = {"transaction_id": trns_id, "transaction_amount": amount, "transaction_date_time": dtt, "transaction_currency": currency}
	transes.append(a_trans)

table_3 = pd.DataFrame(transes)
table_3.to_csv("db-data/transactions.csv", index = False)















