#!/usr/bin/env python
from jsonrpc import ServiceProxy
import sys
import csv
import argparse
import ConfigParser
import logging
import os
import pika
import json
from collections import OrderedDict

#TODO backport pickle support from other extractors

# Initialize argument parser
parser = argparse.ArgumentParser(description="Pull blockchain data from AMQP and write to storage (DB or CSV).")

# A config file is needed.
parser.add_argument("-c", "--config", action="store", help="config file name", default="peercoin_extractor.conf")

# Debug output - this will be written to log file
parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output")

# Enable DB dry-run -- do not send to DB, but print out to stdout
parser.add_argument("--dryrun", action="store_true", help="Do dry run for DB, i.e. print to stdout instead of sending to DB.")


# Go through options passed.
args = parser.parse_args()

# set up dry run
dry_run = True if args.dryrun else False
if not dry_run:
    import psycopg2

# Fetch a config file name, if given
config_fn = args.config if args.config else "peercoin_extractor.conf"

# Now parse config file options
scp = ConfigParser.SafeConfigParser()
scp.read(config_fn)

config_read_fail = False
for section in ["amqp", "logging", "db"]:
    if not scp.has_section(section):
        print("Missing section %s in config file." % section)
        config_read_fail = True

# Set up log
log_file = scp.get("logging", "log_file") if scp.has_option("logging", "log_file") and scp.get("logging", "log_file") != "" else "blockchain_to_storage.log"
level = logging.DEBUG if args.debug else logging.INFO
logging.basicConfig(filename=log_file, filemode="w", level=level, format='%(asctime)s:%(levelname)s:%(threadName)s: %(message)s') 

# Test and get AMQP configuration
for option in ["amqp_host", "amqp_port", "amqp_exchange", "amqp_queue", "amqp_user", "amqp_password", "amqp_routing_key"]:
    if option not in scp.options("amqp"):
        print("Missing option %s in AMQP confguration." % option)
        config_read_fail = True
amqp_host = scp.get("amqp", "amqp_host") if not scp.get("amqp", "amqp_host") == "" else "localhost"
amqp_port = scp.getint("amqp", "amqp_port") if not scp.get("amqp", "amqp_port") == "" else 5672
amqp_exchange = scp.get("amqp", "amqp_exchange") if not scp.get("amqp", "amqp_exchange") == "" else "peercoin"
amqp_queue = scp.get("amqp", "amqp_queue") if not scp.get("amqp", "amqp_queue") == "" else "peercoin"
amqp_user = scp.get("amqp", "amqp_user") if not scp.get("amqp", "amqp_user") == "" else "guest"
amqp_password = scp.get("amqp", "amqp_password") if not scp.get("amqp", "amqp_password") == "" else "guest"
credentials = pika.PlainCredentials(amqp_user, amqp_password)
parameters = pika.ConnectionParameters(host=amqp_host, port=amqp_port, virtual_host="/", credentials=credentials)


# Test and get DB configuration
for db_option in ["db_host", "db_port", "db_user", "db_password", "db_name", "db_schema"]:
    if db_option not in scp.options("db"):
        print("Missing option %s in DB section in config file." % db_option)
        config_read_fail = True
    else:
        db_host = scp.get("db", "db_host") if not scp.get("db", "db_host") == "" else "localhost"
        db_port = scp.getint("db", "db_port") if not scp.get("db", "db_port") == "" else 5432
        db_user = scp.get("db", "db_user") if not scp.get("db", "db_user") == "" else  "blockchain"
        db_password= scp.get("db", "db_password") if not scp.get("db", "db_password") == "" else ""
        db_name= scp.get("db", "db_name") if not scp.get("db", "db_name") == "" else "blockchain"
        db_schema= scp.get("db", "db_schema") if not scp.get("db", "db_schema") == "" else "peercoin"

if config_read_fail:
    sys.exit(-1)


# set up AMQP
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()
channel.exchange_declare(amqp_exchange, type="fanout")
channel.queue_declare(queue=amqp_queue)
channel.queue_bind(exchange=amqp_exchange, queue=amqp_queue)


# set up DB connection
if not dry_run:
    connect_string = "port='" + str(db_port) + "' dbname='" + db_name + "' user='" + db_user + "' host='" + db_host + "' password='" + db_password + "'"
    conn = psycopg2.connect(connect_string)
    cursor = conn.cursor()



query_counter = 0
def db_query_execute(query, parms):
    global query_counter
    if dry_run:
        if parms is not None:
            print(query % parms)
            logging.info(query % parms)
        else:
            print(query)
    else:
        try:
            if parms is not None:
                res = cursor.execute(query, parms)
                logging.debug(query % parms)
            else:
                res = cursor.execute(query)
                logging.debug(query)
            conn.commit()
            query_counter = query_counter + 1
            print("Number of queries so far: %s \r" % query_counter)
        except psycopg2.Error, e:
            # Test for violation of uniqueness constraint
            if e.pgcode == '23505':
                logging.error("Error code is %s. Query was:" % e.pgcode)
                logging.error(query % parms)
                conn.commit()
                raise e
            else:
                print("Exception when running query. See log for details.")
                logging.error(e)
                logging.error(query % parms)
                sys.exit(-1)
        return res






def amqp_callback(ch, method, properties, body):
    body_json = json.loads(body, object_pairs_hook=OrderedDict)
    block = OrderedDict(body_json["block"])
    # retrieve the parsed TXs, sort them by index, and store as OrderedDict
    parsed_txs_tmp = body_json["parsed_txs"]
    parsed_txs = OrderedDict()
    for key in sorted(parsed_txs_tmp):
        parsed_txs[key] = parsed_txs_tmp[key]

    tx_volume = do_compute_tx_volume(parsed_txs)
    tx_fees = do_compute_tx_fee_volume(parsed_txs)

    # We first insert the block
    do_insert_block(block, tx_volume, tx_fees)
    # we next insert the TX
    for tx_index in parsed_txs:
        do_insert_tx(parsed_txs[tx_index])
    # finally, the vout
    do_insert_vouts(parsed_txs)
    # and the spk
    do_insert_spks(parsed_txs)
    # and the vins
    do_insert_vins(parsed_txs)




def do_insert_vins(parsed_txs):
    for tx_index in parsed_txs:
        tx = parsed_txs[tx_index]
        tx_id = tx["txid"]
        for vin in tx["vin"]:
            coinbase = vin["coinbase"] if "coinbase" in vin else None
            script_sig_dec = vin["scriptSig"]["dec"] if "scriptSig" in vin else None
            ref_tx_id = vin["txid"] if "txid" in vin else None
            ref_vout_n = vin["vout"] if "vout" in vin else None
            sequence = vin["sequence"] if "sequence" in vin else None
            sql_insert_vin = "INSERT INTO " + db_schema + ".vins (coinbase, script_sig, ref_tx_id, ref_vout_n, sequence, tx_id) VALUES (%s, %s, %s, %s, %s, %s)"
            db_query_execute(sql_insert_vin, (coinbase, json.dumps(script_sig_dec), ref_tx_id, ref_vout_n, sequence, tx_id))



def do_insert_spks(parsed_txs):
    for tx_index in parsed_txs:
        tx = parsed_txs[tx_index]
        tx_id = tx["txid"]
        for vout in tx["vout"]:
            vout_n = vout["n"]
            spk = vout["scriptPubKey"]
            addresses = spk["addresses"] if "addresses" in spk else []
            asm = spk["asm"]
            hex = spk["hex"]
            req_sigs = spk["reqSigs"] if "reqSigs" in spk else None
            type = spk["type"]

            # First, let's insert addresses
            for address in addresses:
                is_valid = parsed_txs[tx_index]["addresses_valid"][address]
                sql_insert_address = "INSERT INTO " + db_schema + ".addresses (address, block_first_seen, is_valid) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                db_query_execute(sql_insert_address, (address, tx["block_hash"], is_valid))

            # Now, let's go for the spk.
            # Just as with TX and vouts, we need to check for duplicates due to duplicate TXs.
            # See do_insert_tx() for details.
            if addresses == []:
                addresses = None
            sql_insert_spk = "INSERT INTO " + db_schema + ".spks (addresses, asm, hex, req_sigs, tx_id, type, vout_n) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            try:
                db_query_execute(sql_insert_spk, (addresses, asm, hex, req_sigs, tx_id, type, vout_n))
            except psycopg2.Error, e:
                logging.error("While inserting a vout, the uniqueness constraint for TX %s was violated" % tx_id)
                logging.error("Doing an UPDATE instead of an INSERT.")
                sql_update_spk = "UPDATE " + db_schema + ".spks SET addresses = %s, asm = %s, hex = %s, req_sigs = %s, type = %s WHERE tx_id = %s AND vout_n = %s"
                db_query_execute(sql_update_spk, (addresses, asm, hex, req_sigs, type, tx_id, vout_n))



def do_insert_vouts(parsed_txs):
    for tx_index in parsed_txs:
        tx = parsed_txs[tx_index]
        tx_id = tx["txid"]
        for vout in tx["vout"]:
            value = btc_to_peerbits(vout["value"])
            vout_n = vout["n"]
            sql_insert_vout = "INSERT INTO " + db_schema + ".vouts (tx_id, value, vout_n) VALUES (%s, %s, %s)"
            # Just as with TX, we need to check for duplicate TX ID.
            # See do_insert_tx() for details.
            try:
                db_query_execute(sql_insert_vout, (tx_id, value, vout_n))
            except psycopg2.Error, e:
                logging.error("While inserting a vout, the uniqueness constraint for TX %s was violated" % tx_id)
                logging.error("Doing an UPDATE instead of an INSERT.")
                sql_update_vout = "UPDATE " + db_schema + ".vouts SET value = %s, vout_n = %s WHERE tx_id = %s"
                db_query_execute(sql_update_vout, (value, vout_n, tx_id))


def do_insert_tx(tx):
    # We computed this in the do_compute_tx_fee_volume
    block_hash = tx["block_hash"]
    lock_time = tx["locktime"]
    size = tx["size"]
    tx_fee = tx["tx_fee"]
    tx_id = tx["txid"]
    tx_index = tx["tx_index"]
    version = tx["version"]
    sql_insert_tx = "INSERT INTO " + db_schema + ".transactions (block_hash, fee, lock_time, size, tx_id, tx_index, version) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    logging.debug("INSERT TX %s " % tx_id)
    try:
        db_query_execute(sql_insert_tx, (block_hash, tx_fee, lock_time, size, tx_id, tx_index, version))
    except psycopg2.Error, e:
        # It is a known phenomenon (and bug) that TX with the same ID exist in more than one block.
        # The blockchain's index stores only the last one and the previous one must be completely spent
        # (which it generally is). For all other purposes, it is "overwritten" in the blockchain.
        # We deal with this in exactly the same way - since the TX is spent, we can simply store one copy.
        # There is no way for us to get the older one anyway: the blockchain's index does not retrieve it
        # for us.
        logging.error("Uniqueness constraint for TX %s violated" % tx_id)
        logging.error("Doing an UPDATE instead of an INSERT.")
        sql_update_tx = "UPDATE " + db_schema + ".transactions SET block_hash = %s, fee = %s, lock_time =%s, size = %s, tx_index = %s, version = %s WHERE tx_id = %s"
        db_query_execute(sql_update_tx, (block_hash, tx_fee, lock_time, size, tx_index, version, tx_id))



# TODO Meticulously check in DB if this works correctly
def do_compute_tx_fee(tx_index, parsed_txs):
    # Get the TX we are working on
    tx = parsed_txs[tx_index]
    # Get the output sum in peerbits.
    sum_vout = helper_compute_vout_sum(tx)
    logging.debug("Sum of all outputs for TX %s: %s" % (tx["txid"], sum_vout))
    
    # Get the input sum:
    # Take all vin, get the vout and tx_id they are referring to.
    # Many tx_id will already be in the DB. Some, however, may be in the current
    # parsed_txs.
    vout_dict = {}
    vin_counter = 0
    for vin in tx["vin"]:
        if "vout" in vin and "txid" in vin:
            vout_dict[vin_counter] = { "ref_tx_id" : vin["txid"], "ref_vout_n" : vin["vout"] }
            vin_counter = vin_counter + 1

    # if no vouts are found at all: this is a pure coinbase TX, return sum_vout
    if len(vout_dict) == 0:
        tx["tx_fee"] = sum_vout
        return tx

    # Fetch the txs, look in the vouts, add up (step 1: search in DB)
    where_cond = "(tx_id = '%s' AND vout_n = %s)"
    where_conds = []
    for vin_counter in vout_dict:
        vout_data = vout_dict[vin_counter]
        ref_tx_id = vout_data["ref_tx_id"]
        ref_vout_n = vout_data["ref_vout_n"]
        where_conds.append(where_cond % (ref_tx_id, ref_vout_n))
    logging.debug("where_conds for TX %s are: %s" % (tx["txid"], where_conds))
    where_clause = "WHERE "
    for i in range(0, len(where_conds) - 1):
        where_clause = where_clause + where_conds[i] + " OR "
    where_clause = where_clause + where_conds[-1]
    sql_get_tx_vout = "SELECT SUM(value) FROM " + db_schema + ".vouts " + where_clause
    db_query_execute(sql_get_tx_vout, None)
    if not dry_run:
        db_res = cursor.fetchone()
    else:
        # We don't care about the correct value in a dry-run. It just shouldn't
        # be 0, as there is a final sanity check at the end of this function.
        db_res = -999999

    # Step 2: it may absolutely be the case that we do not find a single
    # referenced TX in the DB. In such a case, all referenced TX are in the
    # same block.
    if not dry_run:
        if db_res[0] is None:
            db_res = 0
            logging.info("We found a SUM of vouts to be NULL - check TX in the following string: " + sql_get_tx_vout)
            logging.info("The referring TX is %s" % tx["txid"])
        else:
            db_res = db_res[0]
        logging.debug("Computed fee found in DB in TX %s: %s" % (tx["txid"], db_res))

    # The block is not stored to DB yet at this time. So look in parsed_txs
    # if there are some TX that our vins are referencing.
    same_block_res = 0
    for vin_counter in vout_dict:
        vout_data = vout_dict[vin_counter]
        ref_tx_id = vout_data["ref_tx_id"]
        ref_vout_n = vout_data["ref_vout_n"]
        for tx_index in parsed_txs:
            if parsed_txs[tx_index]["txid"] == ref_tx_id:
                # We found a referenced TX in the same block
                for vout in parsed_txs[tx_index]["vout"]:
                    if vout["n"] == ref_vout_n:
                        same_block_res = same_block_res + btc_to_peerbits(vout["value"])
                        logging.debug("Computed fees found in current block for TX %s: %s" % (tx["txid"], same_block_res))
    
    sum_vins = db_res + same_block_res
    # Do a sanity check. TX with vin 0 can exist, but are rare. We log them.
    if sum_vins == 0.0:
        logging.info("Sum of all inputs, i.e. referenced vouts in TX %s is 0. That's unusual." % tx["txid"])
  
    logging.debug("Sum of all inputs for TX %s is %s" % (tx["txid"], sum_vins))

    # We add a new field to the TX
    if not dry_run:
        tx["tx_fee"] = sum_vins - sum_vout
    else:
        tx["tx_fee"] = sum_vins - sum_vout

    # It is amazing, but Peercoin does have blocks that include TXs with
    # negative TX fees:
    # http://ppc.blockr.io/tx/info/6476ede8707a8090c6b861c92efedbea4d72763d4e9b01039b2f5de913a85305
    if not dry_run and tx["tx_fee"] < 0:
        logging.warning("Whoa. Negative TX fee in TX %s. Double-check." % tx["txid"])
        logging.warning("In: %s" % sum_vins)
        logging.warning("Out: %s" % sum_vout)

    logging.debug("Overall fee for TX %s: %s" % (tx["txid"], tx["tx_fee"]))
    return tx


def do_compute_tx_fee_volume(parsed_txs):
    # sum over all do_compute_tx_fee(tx) for tx in block
    fees_volume = 0
    for tx_index in parsed_txs:
        try:
            tx = do_compute_tx_fee(tx_index, parsed_txs)
        except ValueError, e:
            print(e)
            logging.error("Invalid fee was found in block " + tx["block_hash"])
            channel.close()
            sys.exit(-1)
        fees_volume = fees_volume + tx["tx_fee"]
    return fees_volume



def do_compute_tx_volume(parsed_txs):
    tx_volume = 0
    for tx_index in parsed_txs:
        tx = parsed_txs[tx_index]
        tx_volume = tx_volume + helper_compute_vout_sum(tx)
    return tx_volume



def do_insert_block(block, tx_volume, tx_fees):
    bits = block["bits"]
    block_hash = block["hash"]
    block_index= block["height"]
    difficulty = block["difficulty"]
    entropy_bit = True if block["entropybit"] == 1 else False
    flags = block["flags"]
    mint = block["mint"]
    modifier = block["modifier"]
    modifier_checksum = block["modifierchecksum"]
    nonce = block["nonce"]
    if block_index == 0:
        prev_block_hash = None
    else:
        prev_block_hash = block["previousblockhash"]
    proof_hash = block["proofhash"]
    size = block["size"]
    timestamp = block["time"]
    version = block["version"]

    # sql_string = "INSERT INTO " + db_schema + ".blocks (bits, block_hash, block_index, difficulty, entropy_bit, flags, mint, modifier, modifier_checksum, nonce, prev_block_hash, proof_hash, size, timestamp, tx_fees, tx_volume, version) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (to_timestamp(%s) AT TIME ZONE 'UTC'), %s, %s, %s)"
    sql_string = "INSERT INTO " + db_schema + ".blocks (bits, block_hash, block_index, difficulty, entropy_bit, flags, mint, modifier, modifier_checksum, nonce, prev_block_hash, proof_hash, size, timestamp, tx_fees, tx_volume, version) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    db_query_execute(sql_string, (bits, block_hash, block_index, difficulty, entropy_bit, flags, mint, modifier, modifier_checksum, nonce, prev_block_hash, proof_hash, size, timestamp, tx_fees, tx_volume, version))



def helper_compute_vout_sum(tx):
    vout_sum = 0
    for vout in tx["vout"]:
        vout_sum = vout_sum + btc_to_peerbits(vout["value"])
    return vout_sum


# These follow the BTC rules to avoid FP errors.
# Note that the subdivision of Peercoin differs from BTC!
# I.e. the smallest unit, Peerbits, is 0.000001 Peercoin.
def btc_to_peerbits(btc_val):
    return long(round(btc_val * 1e6))

def peerbits_to_btc(peerbits_val):
    return float(peerbits_val / 1e6)


print(' [*] Waiting for logs. To exit press CTRL+C')
channel.basic_consume(amqp_callback, queue=amqp_queue, no_ack=True)

channel.start_consuming()
