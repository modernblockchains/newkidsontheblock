import multiprocessing
from librabbitmq import Connection
import time
import psycopg2
import json
import rlp
import sha3
import binascii
from util import Helper

BLOCK_QUEUE_NAME = "ethblocks"
NUM_WORKERS = 1
SERIAL_PROCESSING = (NUM_WORKERS == 1)
PG_DUPLICATE_KEY_ERR_CODE = "23505"
DB_INFO = {
    "dbname": 'db_blockchains',
    "user": 'blockchains',
    "host": '221.199.215.46',
    "password": '',
    "port": "5432"
}
RABBIT_INFO = {
    "host": "nugura.research.nicta.com.au",
    "port": 5672,
    "credentials": pika.credentials.PlainCredentials(username="blockchain_observer", password="")
}
BLOCK_LOAD_REPORT_FREQUENCY = 500
HIGHEST_BLOCK = 1358548

LAST_TIME = time.time()

helper = Helper()


class Block(object):


    pending_addresses = []


    def __init__(self, block_data=None, db=None):
        self.db = db
        self.cursor = db.cursor()
        if int(block_data["number"]) % BLOCK_LOAD_REPORT_FREQUENCY == 0:
            print("Loading %d blocks to DB from #%s (at % 11.2f )"
                  % (BLOCK_LOAD_REPORT_FREQUENCY, block_data["number"], time.time()))
        try:
            self.block_number = block_data["number"]
            self.save_to_db(block_data)

        except psycopg2.IntegrityError as e:
            if not SERIAL_PROCESSING:
                # If we're not processing serially, there might be hash collisions or something.
                # nothing to worry about if everything is out-of-sequence
                print("===== IntegrityError ===== ... probably okay, usually a hash collision")
                print(e)
            else:
                # Shouldn't have this here if we're processing serially

               # raise e
                pass #just gonna ignore this for now



    def save_to_db(self, block):
        self.queue_address_for_insertion(block["miner"], block["hash"])
        self.insert_block(block)
        for tx in block["transactions"]:
            if not tx["to"]:
                tx["tx_type"] = 2 # Contract creation
                self.queue_address_for_insertion(helper.calculate_contract_address(tx), block["hash"], 1)
            else:
                # Should check what the 'to' field is here (contract/person)
                tx["tx_type"] = 0
                if SERIAL_PROCESSING:
                    if self.is_known_contract(tx["to"]):
                        tx["tx_type"] = 1
                    else:
                        # Don't need to add existing contract addresss
                        self.queue_address_for_insertion(tx["to"], block["hash"], tx["tx_type"])
                else:
                    self.queue_address_for_insertion(tx["to"], block["hash"], tx["tx_type"])

            try:
                tx["value"] = int(float(tx["value"]))
            except ValueError as e:
                print(block)
                raise e

            self.queue_address_for_insertion(tx["from"], block["hash"])
            self.insert_transaction(tx)
            self.insert_transaction_block(tx, block)


    def queue_address_for_insertion(self, address, block_hash, address_type=0):
        self.pending_addresses.append((address, block_hash, address_type))



    def insert_block(self, block):
        block["difficulty"] = int(block["difficulty"])
        self.cursor.execute("""INSERT INTO ethereum.Blocks
          (block_number, block_hash, timestamp, prev_block_hash, nonce, miner_addr, difficulty, size_bytes, extra_data, block_reward)
          VALUES (%(number)s, %(hash)s, to_timestamp(%(timestamp)s), %(parentHash)s, %(nonce)s, %(miner)s, %(difficulty)s, %(size)s, %(extraData)s, 5)""",
                            block);

    def is_known_contract(self, contract_address):
        self.cursor.execute("""SELECT * FROM ethereum.Addresses WHERE address = %s""", (contract_address,))
        address = self.cursor.fetchone()
        if address:
            return address[1] == 1
        else:
            return False

    def insert_pending_addresses(self):
        while len(self.pending_addresses) > 0:
            addr = self.pending_addresses.pop()
            self.insert_address(addr[0], addr[1], addr[2])


    def insert_address(self, address, block_hash, is_contract=0):
        if is_contract not in [0,1]:
            raise Exception("Invalid value for is_contract" + str(is_contract))
        if SERIAL_PROCESSING:
            # Handle integrity exceptions in Python
            try:
                self.cursor.execute("""INSERT INTO ethereum.Addresses
                  (address, address_type)
                  VALUES (%s, %s)""", (address, is_contract,))
                self.cursor.execute("""INSERT INTO ethereum.Addresses_Blocks
                  (address, block_hash)
                  VALUES (%s, %s)""", (address, block_hash))
                self.db.commit()
            except psycopg2.IntegrityError as e:
                # Duplicate key contraint
                if e.pgcode == PG_DUPLICATE_KEY_ERR_CODE:
                    #address is already seen, that's okay if it's not a contract
                    # No longer any special treatment for contracts, as we might see a fork when inserting blocks
                    if is_contract == 1 and False:
                        #This is bad if it's a contract, because we only set is_contract = 1
                        # on contract craeation which should only happen once.
                        print("===== Trying to create contract twice?? =====")
                        print("address: %s - block_hash: %s" % (address, block_hash))
                        self.db.rollback()
                        raise e
                    else:
                        # No problem if it's already in existence and it's not contract creation
                        self.db.rollback()
                        # But we still need to re-associate the address with the block
                        self.cursor.execute("""INSERT INTO ethereum.Addresses_Blocks
                          (address, block_hash)
                          VALUES (%s, %s)""", (address, block_hash))
                        self.db.commit()
                        pass

                else:
                    # Different integrity error, pass it on.
                    self.db.rollback()
                    raise e

        else:
            # Let Postgres do the updating, since is_contract = 1 is more correct that = 0
            self.cursor.execute("""INSERT INTO ethereum.Addresses
              (address, address_type)
              VALUES (%s, %s)
              ON CONFLICT DO UPDATE SET address_type = %s WHERE %s = 1""", (address, is_contract, is_contract, is_contract))
            self.cursor.execute("""INSERT INTO ethereum.Addresses_Blocks
                          (address, block_hash)
                          VALUES (%s, %s)""", (address, block_hash))




    def insert_transaction(self, tx):
        self.cursor.execute("""INSERT INTO ethereum.Transactions
            (tx_hash, tx_index, input, tx_type, nonce)
            VALUES (%(hash)s, %(transactionIndex)s, %(input)s, %(tx_type)s, %(nonce)s)""", tx)
        self.cursor.execute("""INSERT INTO ethereum.TxFromAddress
            (address, tx_hash, input_value)
            VALUES (%(from)s, %(hash)s, %(value)s)""", tx)
        if tx["tx_type"] < 2:
            self.cursor.execute("""INSERT INTO ethereum.TxToAddress
            (address, tx_hash, output_value)
            VALUES (%(to)s, %(hash)s, %(value)s)""", tx)

    def insert_transaction_block(self, tx, block):
        self.cursor.execute("""INSERT INTO ethereum.Transactions_Blocks
            (tx_hash, block_hash)
            VALUES (%(tx)s, %(block)s)""",
                    {"tx": tx["hash"],
                     "block": block["hash"]})

class ConsumerWorkerProcess(multiprocessing.Process):

    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_working = multiprocessing.Event()
        self.last_time = time.time()

    def run(self):
        self.setup_channel()
        self.connect_to_db()

        self.channel.basic_consume(callback=self.handle_block, queue=BLOCK_QUEUE_NAME)

        while self.channel.is_open and not self.stop_working.is_set():
            try:
                self.connection.drain_events()
            except KeyboardInterrupt:
                break

        self.teardown_channel()

    def setup_channel(self):
        # Settup connection and channel to Rabbit MQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(**RABBIT_INFO))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.queue_declare(queue=BLOCK_QUEUE_NAME, auto_delete=False, durable=True)

    def connect_to_db(self):
        # Build the connect string from DB_INFO dict
        self.db = psycopg2.connect(" ".join([i[0]+"='"+i[1]+"'" for i in DB_INFO.items()]))

    def teardown_channel(self):
        self.channel.close()
        self.connection.close()

    def signal_exit(self):
        self.stop_working.set()

    def exit(self):
        self.signal_exit()
        while self.is_alive(): # checking is_alive on zombies kills them
            time.sleep(1)

    def kill(self):
        self.terminate()
        self.join()

    def handle_block(self, task):
        delivery_info, properties, body = (task.delivery_info, task.properties, task.body)
        # Function called with Rabbit MQ work unit
        decoded_body = json.loads(bytes.decode(str(body), 'utf-8'))
        block = Block(decoded_body, self.db)
        self.db.commit()
        # Only the addresses have primary key constraints for the moment, so do those in a second commit.
        # This commits itself
        block.insert_pending_addresses()
        if block.block_number % BLOCK_LOAD_REPORT_FREQUENCY == 0:
            time_now = time.time()
            time_since = (time_now - self.last_time)
            blocks_left = HIGHEST_BLOCK - block.block_number
            time_left = int(blocks_left * (time_since/BLOCK_LOAD_REPORT_FREQUENCY))
            m, s = divmod(time_left, 60)
            h, m = divmod(m, 60)
            time_remaining_string = "%dh:%02dm:%02ds" % (h, m, s)

            print("(last %d blocks took: % 4.2fs - %d blocks left (%s)" %
                  (BLOCK_LOAD_REPORT_FREQUENCY, time_since, blocks_left, time_remaining_string))
            self.last_time = time.time()

        task.ack()

    def handle_log_line(self, channel, method, properties, body):
        decoded_body = json.loads(bytes.decode(body, 'utf-8'))
        tag, decoded_data = self.identifier.identify(decoded_body)
        for dest in self.destinations:
            if tag in dest.tags_to_receive:
                dest.handle(tag, decoded_body, decoded_data)


if SERIAL_PROCESSING:
    print("Only running 1 worker - processing serially - this will be slower.")

for i in range(NUM_WORKERS):
    print("Starting worker #%d" % (i,))
    print("Reporting every %d blocks" % BLOCK_LOAD_REPORT_FREQUENCY)
    process = ConsumerWorkerProcess()
    process.start()
