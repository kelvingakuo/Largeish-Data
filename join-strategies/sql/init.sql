CREATE TABLE IF NOT EXISTS public.persons(
	person_id INT PRIMARY KEY,
	first_name VARCHAR,
	last_name VARCHAR

);

CREATE TABLE IF NOT EXISTS public.transactions(
	transaction_id INT PRIMARY KEY,
	transaction_amount INT,
	transaction_date_time TIMESTAMP,
	transaction_currency VARCHAR

);

CREATE TABLE IF NOT EXISTS public.person_transactions(
	person_id_fk INT,
	transaction_id_fk INT,

	CONSTRAINT fk_person
		FOREIGN KEY(person_id_fk)
		REFERENCES persons(person_id),

	CONSTRAINT fK_transaction
		FOREIGN KEY(transaction_id_fk)
		REFERENCES transactions(transaction_id)

);

COPY public.persons(first_name, last_name, person_id)
FROM '/var/lib/postgresql/data/persons.csv' DELIMITER ','  CSV HEADER;

COPY public.transactions(transaction_id, transaction_amount, transaction_date_time, transaction_currency)
FROM '/var/lib/postgresql/data/transactions.csv' DELIMITER ','  CSV HEADER;

COPY public.person_transactions(person_id_fk, transaction_id_fk)
FROM '/var/lib/postgresql/data/people_transactions.csv' DELIMITER ','  CSV HEADER;


