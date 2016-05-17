import requests
import psycopg2
import psycopg2.extras
import time
import pprint
from bs4 import BeautifulSoup
from multiprocessing import Process, JoinableQueue, Pool, Event
import os

pp = pprint.PrettyPrinter()

from bs4 import BeautifulSoup


WEB_URL_SINGLE = "http://etherscan.io/address/%s"

NUM_WORKERS = int(os.cpu_count() * 2.5)

REPORTING_FREQUENCY_SECS = 3


class CrawlerWorkerProcess(Process):

    def __init__(self, queue):
        self.queue = queue
        super(CrawlerWorkerProcess,self).__init__()
        self.stop_working = Event()

    def run(self):
        self.connect_to_db()

        while not self.stop_working.is_set():
            address = self.queue.get(True)
            print(str(os.getpid()) + ": Looking up address %s... " % (address,), end="")
            name = self.lookup_address_name_etherscan(address)
            if name:
                self.save_address_name_to_db(address, name)
            else:
                print("not found.")
            self.queue.task_done()
            
    def signal_exit(self):
        self.stop_working.set()

    def connect_to_db(self):
        self.db = psycopg2.connect("dbname='blockchain_ethereum4' user='postgres' host='localhost' password=''", cursor_factory=psycopg2.extras.DictCursor)
        self.cursor = self.db.cursor()

    def save_address_name_to_db(self, address, name):
        if not name is None:
            print("\n" + "%!" * 40)
            print("Saving %s address to db (%s)" % (name, address))
            print("%!" * 40 + "\n")
            self.cursor.execute("""
                INSERT INTO ethereum.Address_Identifications
                (name, address, description, source) VALUES
                (%s, %s, %s, %s)""", (name, address, "As presumed by Etherscan.io", WEB_URL_SINGLE % (address,)))
            self.db.commit()
        else:
            print("not found %s" % (address,), end=", ")

    def lookup_address_name_etherscan(self, address):
        url = WEB_URL_SINGLE % (address,)
        response = requests.get(url)
        if not response.status_code == 200:
            raise Exception("Got a HTTP error code from Etherchain: %d" % (response.status_code,))

        tags = BeautifulSoup(response.text, "html.parser").findAll("font", {"title": "NameTag"})
        if tags:
            return tags[0].next
        else:
            return None


if __name__ == '__main__':
    db = psycopg2.connect("dbname='blockchain_ethereum4' user='postgres' host='localhost' password=''", cursor_factory=psycopg2.extras.DictCursor)
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT(address) FROM ethereum.Addresses");
    work_queue = JoinableQueue()
    addresses_count = 0

    for i in range(NUM_WORKERS):
        CrawlerWorkerProcess(work_queue).start()

    for row in cursor.fetchall():
        work_queue.put(row.get("address"))
        addresses_count += 1
    print("Looking up %d addresses on Etherscan.io using %d workers." % (addresses_count, NUM_WORKERS))

    last_time = time.time()
    previous_addresses_left = work_queue.qsize()
    while work_queue.qsize() > 0:
        time_now = time.time()
        time_since = (time_now - last_time)
        addresses_left = work_queue.qsize()
        addresses_done_since_last = previous_addresses_left - addresses_left
        try:
            time_left = int(addresses_left * (time_since/addresses_done_since_last))
        except ZeroDivisionError:
            time_left = 9999999999999
        m,s = divmod(time_left, 60)
        h,m = divmod(m, 60)
        time_remaining_string = "%dh:%02dm:%02ds" % (h,m,s)
        print("#" * 50)
        print("###### Last %d addresses took: % 4.2fs - %d addresses left (%s) ######" %
                (addresses_done_since_last, time_since, addresses_left, time_remaining_string))
        print("#" * 50)
        last_time = time.time()
        previous_addresses_left = addresses_left
        time.sleep(REPORTING_FREQUENCY_SECS)


    work_queue.join()
