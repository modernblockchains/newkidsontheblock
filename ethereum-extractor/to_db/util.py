import time
import psycopg2
import psycopg2.extras
import json
import rlp
import sha3
import math
import binascii
from collections import defaultdict
ENCODINGS = ["ascii","big5","big5hkscs","cp037","cp424","cp437","cp500","cp737","cp775","cp850","cp852","cp855","cp856","cp857","cp860","cp861","cp862","cp863","cp864","cp865","cp866","cp869","cp874","cp875","cp932","cp949","cp950","cp1006","cp1026","cp1140","cp1250","cp1251","cp1252","cp1253","cp1254","cp1255","cp1256","cp1257","cp1258","euc_jp","euc_jis_2004","euc_jisx0213","euc_kr","gb2312","gbk","gb18030","hz","iso2022_jp","iso2022_jp_1","iso2022_jp_2","iso2022_jp_2004","iso2022_jp_3","iso2022_jp_ext","iso2022_kr","latin_1","iso8859_2","iso8859_3","iso8859_4","iso8859_5","iso8859_6","iso8859_7","iso8859_8","iso8859_9","iso8859_10","iso8859_13","iso8859_14","iso8859_15","johab","koi8_r","koi8_u","mac_cyrillic","mac_greek","mac_iceland","mac_latin2","mac_roman","mac_turkish","ptcp154","shift_jis","shift_jis_2004","shift_jisx0213","utf_16","utf_16_be","utf_16_le","utf_7","utf_8"]
class Helper(object):

    def calculate_contract_address(self, tx=None, from_nonce_tuple=None):
        if tx:
            from_address = tx["from"]
            nonce = tx["nonce"]
        elif from_nonce_tuple:
            from_address = from_nonce_tuple[0]
            nonce = from_nonce_tuple[1]

        return "0x" + sha3.sha3_256(
            rlp.encode([
                binascii.unhexlify(from_address[2:]),
                nonce
            ])
        ).hexdigest()[-40:]


    def wei_to_either(self, wei):
        return wei / math.pow(10,18)

    def ether_to_wei(self, ether):
        return ether * math.pow(10,18)


class Explorer(object):

    def __init__(self):
        self.helper = Helper()
        self.initialise_db()

    def initialise_db(self):
        self.db = psycopg2.connect("dbname='db_blockchains' user='blockchains' host='221.199.215.46' password=''", cursor_factory=psycopg2.extras.DictCursor)
        self.cursor = self.db.cursor()


    def get_transaction(self, tx_hash):
        self.cursor.execute("""SELECT
                    t.tx_hash AS hash,
                    t.nonce AS nonce,
                    tb.block_hash AS blockHash,
                    t.tx_index AS transactionIndex,
                    fa.address AS from,
                    ta.address AS to,
                    t.value AS value,
                    t.input AS  input
                FROM ethereum.transactions t
                LEFT JOIN ethereum.txtoaddress ta ON t.tx_hash = ta.tx_hash
                LEFT JOIN ethereum.txfromaddress fa ON t.tx_hash = fa.tx_hash
                LEFT JOIN ethereum.transactions_blocks tb ON t.tx_hash = tb.tx_hash
                WHERE t.tx_hash = %s""", (tx_hash,))


    def find_contract_creation_transactions_without_address(self):
        # First get all the transactions that had a null sender
        self.cursor.execute("""SELECT
                    t.tx_hash AS hash,
                    t.nonce AS nonce,
                    tb.block_hash AS blockHash,
                    t.tx_index AS transactionIndex,
                    fa.address AS from,
                    ta.address AS to,
                    t.value AS value,
                    t.input AS  input
                FROM ethereum.transactions t
                LEFT JOIN ethereum.txtoaddress ta ON t.tx_hash = ta.tx_hash
                JOIN ethereum.txfromaddress fa ON t.tx_hash = fa.tx_hash
                JOIN ethereum.transactions_blocks tb ON t.tx_hash = tb.tx_hash
                WHERE ta.tx_hash IS NULL""")
        creation_transactions = self.cursor.fetchall()

        # Identify the addresses that are believed to be contracts
        self.cursor.execute("""SELECT address FROM ethereum.addresses
                WHERE address_type = 1""")
        contract_addresses = self.cursor.fetchall()

        print("Found %d creation transactions and %d contract addresses" % (len(creation_transactions), len(contract_addresses)))

        txs = set()
        addrs = set()
        for tx in creation_transactions:
            txs.add(self.helper.calculate_contract_address(dict(tx)))
        for addr in contract_addresses:
            addrs.add(addr[0])

        return txs.difference(addrs)

    def block_numbers_involving_addresses(self, address):
        self.cursor.execute("""SELECT b.block_number
                    FROM ethereum.blocks b
                    JOIN addresses_blocks ab ON b.block_hash = ab.block_hash
                    WHERE ab.address = %s
                    ORDER BY block_number DESC;""", (address,))
        return [int(row.get("block_number")) for row in self.cursor.fetchall()]

    def get_distinct_extra_data_and_block_range(self):
        """
        Looks at the 'extra' data field on the Ethereum blockchain and returns a unique list of all existing
        values for the field.
        Provides a range and count for how many time each extra_data value was seen.

        :return: List of unique decoded extra-data, showing the blocks for which it was active
        """
        self.cursor.execute("""
                SELECT extra_data,
                        MIN(block_number) min_block_number,
                        MAX(block_number) max_block_number,
                        COUNT(block_number) count
                FROM ethereum.blocks
                GROUP BY extra_data
                ORDER BY min_block_number ASC""")

        result = []
        for row in self.cursor.fetchall():
            extra_data_bytes = binascii.unhexlify(row.get("extra_data")[2:])
            try:
                extra_data = extra_data_bytes.decode("ascii")
                decoded = True
            except UnicodeDecodeError as e:
                print("Extra data not ascii... trying UTF-8")
                try:
                    extra_data = extra_data_bytes.decode("utf-8")
                    decoded = True
                except UnicodeDecodeError as f:
                    print("Extra data not utf-8 either, trying anything!")
                    try:
                        extra_data = extra_data_bytes.decode('latin_1')
                        decoded = True
                    except UnicodeDecodeError as g:
                        print("Extra data not latin_1 either - unknown")
                        extra_data = row.get("extra_data")
                        decoded = False

            result.append({
                "extra_data": extra_data,
                "decoded": decoded,
                "from_block": row.get("min_block_number"),
                "to_block": row.get("max_block_number"),
                "count": row.get("count"),
                "unique": row.get("min_block_number") == row.get("max_block_number")
            })
        return result


    def get_transaction_history(self, all_time=True):
        if not all_time:
            raise Exception("Only tx history for all time is currently supported")
        self.cursor.execute("""SELECT
                    t.tx_hash AS hash,
                    t.nonce AS nonce,
                    tb.block_hash AS blockHash,
                    t.tx_index AS transactionIndex,
                    fa.address AS from,
                    ta.address AS to,
                    t.value AS value,
                    t.input AS  input,
                    b.block_number AS block_number,
                    b.timestamp_utc AS block_timestamp_utc,
                    t.tx_type AS tx_type,
                    fraddr.address_type AS from_contract,
                    toaddr.address_type AS to_contract
                FROM ethereum.transactions t
                LEFT JOIN ethereum.txtoaddress ta ON t.tx_hash = ta.tx_hash
                LEFT JOIN ethereum.addresses toaddr ON ta.address = toaddr.address
                LEFT JOIN ethereum.txfromaddress fa ON t.tx_hash = fa.tx_hash
                LEFT JOIN ethereum.addresses fraddr ON fa.address = fraddr.address
                LEFT JOIN ethereum.transactions_blocks tb ON t.tx_hash = tb.tx_hash
                LEFT JOIN ethereum.blocks b ON tb.block_hash = b.block_hash""")

        self.cursor.execute("""SELECT
                    t.tx_hash AS hash,
                    t.nonce AS nonce,
                    tb.block_hash AS blockHash,
                    t.tx_index AS transactionIndex,
                    fa.address AS from,
                    ta.address AS to,
                    t.value AS value,
                    t.input AS  input,
                    b.block_number AS block_number,
                    b.timestamp_utc AS block_timestamp_utc,
                    t.tx_type AS tx_type,
                    fraddr.address_type AS from_contract,
                    toaddr.address_type AS to_contract
                FROM ethereum.blocks b
                INNER JOIN ethereum.transactions_blocks tb ON b.block_hash = tb.block_hash
                LEFT JOIN ethereum.transactions t ON tb.tx_hash = t.tx_hash
                LEFT JOIN ethereum.txtoaddress ta ON t.tx_hash = ta.tx_hash
                LEFT JOIN ethereum.addresses toaddr ON ta.address = toaddr.address
                LEFT JOIN ethereum.txfromaddress fa ON t.tx_hash = fa.tx_hash
                LEFT JOIN ethereum.addresses fraddr ON fa.address = fraddr.address
                """)

    def get_basic_transaction_history(self):
        self.cursor.execute("""SELECT
                    t.tx_hash AS hash,
                    b.utc_timestamp AS block_timestamp,
                    b.block_number AS block_number,
                    fa.input_value AS value_wei,
                    t.tx_type AS tx_type
                FROM ethereum.transactions t
                LEFT JOIN ethereum.txfromaddress fa ON t.tx_hash = fa.tx_hash
                LEFT JOIN ethereum.transactions_blocks tb ON t.tx_hash = tb.tx_hash
                LEFT JOIN ethereum.blocks b ON tb.block_hash = b.block_hash""")


    def get_empty_contract_creation_commands(self):
        self.cursor.execute("""SELECT
                      fa.address AS from_address,
                      t.tx_hash AS tx_hash,
                      t.nonce AS sender_nonce,
                      t.tx_index AS tx_index_in_block,
                      fa.input_value AS value_wei,
                      b.block_number AS block_number
                  FROM ethereum.transactions t
                  JOIN ethereum.txfromaddress fa ON t.tx_hash = fa.tx_hash
                  JOIN ethereum.transactions_blocks tb ON t.tx_hash = tb.tx_hash
                  JOIN ethereum.blocks b ON tb.block_hash = b.block_hash
                  WHERE t.tx_type = 2
                    AND t.input = '0x'
                """)
        results = []
        for row in self.cursor.fetchall():
            results.append({
                "from_address": row.get("from_address"),
                "tx_hash": row.get("tx_hash"),
                "sender_nonce": row.get("sender_nonce"),
                "tx_index_in_block": row.get("tx_index_in_block"),
                "value_ether": self.helper.wei_to_either(float(row.get("value_wei"))),
                "contract_address": self.helper.calculate_contract_address(
                    from_nonce_tuple=(row.get("from_address"), row.get("sender_nonce"))
                ),
                "block_number": int(row.get("block_number"))
            });
        return results

    def get_sum_of_empty_contracts(self):
        self.cursor.execute("""SELECT COUNT(t.tx_hash), SUM(fa.input_value)
                 FROM ethereum.transactions t
                 JOIN ethereum.txfromaddress fa ON t.tx_hash = fa.tx_hash
                 JOIN ethereum.transactions_blocks tb ON t.tx_hash = tb.tx_hash
                 JOIN ethereum.blocks b ON tb.block_hash = b.block_hash
                 WHERE t.tx_type = 2
                   AND t.input = '0x'""")
        return self.cursor.fetchone()



    def cleanup_doubled_contract_addresses(self):
        self.cursor.execute("""DELETE
                  FROM ethereum.addresses
                  WHERE address_type = 0
                    AND address IN(SELECT address FROM ethereum.addresses WHERE address_type = 1)
              """)

    def get_primary_blockchain(self, starting_block_num=None, ending_block_num=None):
        self.cursor.execute("""WITH RECURSIVE
            ForkedBlocks(parent_hash, block_hash, block_number, depth) AS (
                SELECT parent_hash, block_hash, block_number, %s
                FROM ethereum.Blocks
                WHERE block_number = %s
              UNION ALL
                SELECT b.parent_hash, b.block_hash, b.block_number, fb.depth - 1
                FROM ethereum.ForkedBlocks fb, Blocks b
                WHERE b.block_hash = fb.parent_hash
                  AND b.block_number > %s
            )
            SELECT *
            FROM ForkedBlocks
        """, (starting_block_num, starting_block_num, ending_block_num));
        results = []
        for row in self.cursor.fetchall():
            results.append({"parent_hash": row.get("parent_hash")})
        return results


    def get_forked_blocks(self, starting_block_num=None, ending_block_num=None):
        self.cursor.execute("""WITH RECURSIVE
            ForkedBlocks(parent_hash, block_hash, block_number, depth) AS (
                SELECT parent_hash, block_hash, block_number, 1310001
                FROM ethereum.Blocks
                WHERE block_number = %s
              UNION ALL
                SELECT b.parent_hash, b.block_hash, b.block_number, fb.depth - 1
                FROM ForkedBlocks fb, ethereum.Blocks b
                WHERE b.block_hash = fb.parent_hash
                  AND b.block_number > %s
            )
            SELECT *
            FROM ForkedBlocks
        """, (starting_block_num, ending_block_num));


    def find_missing_block_numbers(self, end, start=0):
        self.cursor.execute("""SELECT num AS missing
            FROM generate_series(%d, %d) t(num)
            LEFT JOIN ethereum.Blocks ON (t.num = blocks.block_number)
            WHERE blocks.block_number IS NULL""" % (start, end))
        return [row.get("missing") for row in self.cursor.fetchall()]

    def find_duplicate_block_hashes(self):
        self.cursor.execute("""SELECT block_hash, count(*) AS count
            FROM ethereum.blocks
            GROUP BY block_hash
            HAVING count(*) > 1""")
        return {row.get("block_hash"): row.get("count") for row in self.cursor.fetchall()}

    def find_duplicate_tx_hashes(self):
        self.cursor.execute("""SELECT tx_hash, count(*) AS count
            FROM ethereum.Transactions
            GROUP BY tx_hash
            HAVING count(*) > 1""")
        return {row.get("block_hash"): row.get("count") for row in self.cursor.fetchall()}


    def get_address_tx_aggregate(self, limit=300, order_by="tx_total_count", flat_output=False):
        self.cursor.execute("""SELECT    a.address,
                    fraddr.count AS tx_sent_count,
                    toaddr.count AS tx_recv_count,
                    fraddr.count + toaddr.count AS tx_total_count,
                    fraddr.sum / (10^18) AS tx_sent_sum,
                    toaddr.sum / (10^18) AS tx_recv_sum,
                    fraddr.sum + toaddr.sum / (10^18) AS tx_total_sum
            FROM ethereum.addresses a
            JOIN (SELECT address, count(*), sum(input_value)
                    FROM ethereum.TxFromAddress
                    GROUP BY address) fraddr
                ON a.address = fraddr.address
            JOIN (SELECT address, count(*), sum(output_value)
                    FROM ethereum.TxToAddress
                    GROUP BY address) toaddr
                ON a.address = toaddr.address
            ORDER BY %s DESC
            LIMIT %d""" % (order_by, limit,))
        if flat_output:
            return [{"address": row.get("address"),
                    "count_sent": row.get("tx_sent_count"),
                    "count_recv": row.get("tx_recv_count"),
                    "count_total": row.get("tx_total_count"),
                    "sum_sent": row.get("tx_sent_sum"),
                    "sum_recv": row.get("tx_recv_sum"),
                    "sum_total": row.get("tx_total_sum")
                    } for row in self.cursor.fetchall()]
        else:
            return {row.get("address"): {
                    "counts": {
                        "sent": row.get("tx_sent_count"),
                        "recv": row.get("tx_recv_count"),
                        "total": row.get("tx_total_count")
                    },
                    "sums": {
                        "sent": row.get("tx_sent_sum"),
                        "recv": row.get("tx_recv_sum"),
                        "total": row.get("tx_total_sum")
                    }
                }  for row in self.cursor.fetchall()}

    def get_contracts_referenced_before_creation(self):
        self.cursor.execute("""SELECT address, count(*) AS count
            FROM ethereum.addresses
            GROUP BY address
            HAVING count > 1""")
        return [row.get("address") for row in cursor.fetchall()]

    def get_address_history(self, address):
        self.cursor.execute("""SELECT
                a.address,
                fromb.block_hash AS sent_block_hash,
                fromb.block_number AS sent_block_number,
                fromaddr.tx_hash AS sent_tx,
                fromtx.tx_index AS sent_tx_idx,
                tob.block_hash AS recv_block_hash,
                tob.block_number AS recv_block_number,
                toaddr.tx_hash AS recv_tx,
                totx.tx_index AS recv_tx_idx
            FROM ethereum.Addresses a

            LEFT JOIN ethereum.TxToAddress toaddr
                ON a.address = toaddr.address
            LEFT JOIN ethereum.Transactions totx
                ON toaddr.tx_hash = totx.tx_hash
            LEFT JOIN ethereum.Transactions_Blocks totxb
                ON totx.tx_hash = totxb.tx_hash
            LEFT JOIN ethereum.Blocks tob
                ON totxb.block_hash = tob.block_hash

            LEFT JOIN ethereum.TxFromAddress fromaddr
                ON a.address = fromaddr.address
            LEFT JOIN ethereum.Transactions fromtx
                ON fromaddr.tx_hash = fromtx.tx_hash
            LEFT JOIN ethereum.Transactions_Blocks fromtxb
                ON fromtx.tx_hash = fromtxb.tx_hash
            LEFT JOIN ethereum.Blocks fromb
                ON fromtxb.block_hash = fromb.block_hash

           WHERE a.address = '0x2c785fa15498fe27f1fc5f809bcd9c10c9481752'
                AND a.address_type = 0
            ORDER BY tob.block_number, fromb.block_number, totx.tx_index, fromtx.tx_index ASC""", (address,))


    def get_transactions_per_day(self):
        self.cursor.execute("""SELECT
                b.timestamp::DATE AS date,
                COUNT(tb.*) AS tx_count,
                SUM(tfa.input_value) / (10^18) AS tx_sent_sum,
                SUM(tta.output_value) / (10^18) AS tx_recv_sum
            FROM ethereum.Blocks b
            JOIN ethereum.Transactions_Blocks tb ON b.block_hash = tb.block_hash
            LEFT JOIN ethereum.TxFromAddress tfa ON tb.tx_hash = tfa.tx_hash
            LEFT JOIN ethereum.TxToAddress tta ON tb.tx_hash = tta.tx_hash
            GROUP BY b.timestamp::DATE""")
        return [{
            "date": row.get("date"),
            "count": row.get("tx_count"),
            "sent_sum": row.get("tx_sent_sum"),
            "recv_sum": row.get("tx_recv_sum")
        } for row in self.cursor.fetchall()]


    def get_full_blocks_by_miners(self):
        self.cursor.execute("""SELECT block_number, block_hash, timestamp, miner_addr, difficulty
                FROM ethereum.blocks""")

        miners = defaultdict(list)

        for row in self.cursor.fetchall():
            miners[row.get("miner_addr")].append((row.get("block_number"), row.get("block_hash"), row.get("timestamp")))

        return miners


    def get_daily_top_miners(self, num_miners=12):
        miner_stats = self.get_full_blocks_by_miners()

        daily_stats_dict = defaultdict(list)
        daily_stats = list()

        # fill daily stats with everyone
        for miner, block_infos in miner_stats.items():
            for block_info in block_infos:
                daily_stats_dict[block_info[2].date()].append((miner, block_info))


        for day, miner_block_infos in daily_stats_dict.items():
            # First find the top miners for the day
            miners = defaultdict(int)
            for mbi in miner_block_infos:
                miners[mbi[0]] += 1
            top_miners = sorted(miners, key=miners.get, reverse=True)[:num_miners]
            other_blocks = sum([ miners[m] for m in miners if m not in top_miners])

            day_stats = [(tm, miners[tm]) for tm in top_miners]
            day_stats.append(("other", other_blocks))
            daily_stats.append((day, day_stats))

        return sorted(daily_stats, key=lambda x: x[0])[1:]
