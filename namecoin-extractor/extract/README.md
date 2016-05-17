# newkidsontheblock

Use extract_blockchain.py to connect to a running NMC daemon and extract
useful blockchain data.

Use blockchain_to_storage.py to store to DB. Use create_namecoin_schema.sql
to create the schema in PostgreSQL.

Dependencies:
Python 2.7
pika (if you want to use AMQP, default)
psycopg2 (if you want to store to DB, default)
