import requests
import psycopg2
import psycopg2.extras
import time
import pprint

pp = pprint.PrettyPrinter()


API_URL_MULTIPLE = "https://etherchain.org/api/account/multiple/%s" 
API_URL_SINGLE = "https://etherchain.org/api/account/%s" 

db = psycopg2.connect("dbname='blockchain_ethereum4' user='postgres' host='localhost' password=''", cursor_factory=psycopg2.extras.DictCursor)
cursor = db.cursor()

cursor.execute("SELECT DISTINCT(address) FROM ethereum.Addresses");

BATCH_LOOKUP_COUNT = 50
BATCH_LOOKUP_DELAY = 1 #seconds

etherchain_fails = []

def lookup_address_names_etherchain(addresses=[]):
    url = API_URL_MULTIPLE % (",".join(addresses),)
    response = requests.get(url)
    if not response.status_code == 200:
        raise Exception("Got a HTTP error code from Etherchain: %d" % (response.status_code,))
    data = response.json()
    if not data["status"] == 1:
        raise Exception("Got an error status from Etherchain: %d" % (data["status"],))

    if len(data["data"]) != len(addresses):
        # Missing addressess!
        missing = set(addresses).difference(set([d["address"] for d in data["data"]]))
        etherchain_fails.extend(missing)
        print("!!!!!! ETHERCHAIN FAIL: %s" % (", ".join(missing),))


    return {d["address"]: d["name"] for d in data["data"]}


def save_address_names_to_db(address_names):
    for address, name in address_names.items():
        if not name is None:
            print("\n======")
            print("Saving %s address to db (%s)" % (name, address))
            print("======")
            cursor.execute("""
                INSERT INTO ethereum.Address_Identifications
                (name, address, description, source) VALUES
                (%s, %s, %s, %s)""", (name, address, "As presumed by EtherChain.org", API_URL_SINGLE))
        else:
            print("not found %s" % (address,), end=", ")




def etherchain_find_all_address_names():
    batch_addresses = []
    for row in cursor.fetchall():
        address = row.get("address")
        if len(batch_addresses) < BATCH_LOOKUP_COUNT:
            batch_addresses.append(address)
        else:
            print("Looking up %d addresses on Etherchain..." % (BATCH_LOOKUP_COUNT,))
            names = lookup_address_names_etherchain(batch_addresses)
            save_address_names_to_db(names)
            batch_addresses = []
            time.sleep(BATCH_LOOKUP_DELAY)
