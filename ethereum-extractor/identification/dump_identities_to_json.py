import psycopg2
import psycopg2.extras
from collections import defaultdict
import json

db = psycopg2.connect("dbname='blockchain_ethereum4' user='postgres' host='localhost' password=''", cursor_factory=psycopg2.extras.DictCursor)
cursor = db.cursor()

addresses = defaultdict(list)
cursor.execute("""SELECT name, address, description, source FROM ethereum.Address_Identifications""")
for row in cursor.fetchall():
    addresses[row.get("address")].append({
        "name": row.get("name"),
        "source": row.get("source"),
        "description": row.get("description")
        });

print(json.dumps(addresses))
