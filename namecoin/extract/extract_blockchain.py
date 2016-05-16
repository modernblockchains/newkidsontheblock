from jsonrpc import ServiceProxy
import sys
import csv
import argparse
import ConfigParser
import logging
import os
import pika
import json
import pickle
from collections import OrderedDict

# Initialize argument parser
parser = argparse.ArgumentParser(description="Extract data from the Namecoin blockchain.")

# A config file is needed.
parser.add_argument("-c", "--config", action="store", help="config file name",
        default="namecoin_extractor.conf")

# Debug output - this will be written to log file
parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output")

# Enable AMQP dry-run -- do not send to AMQP, but print out to stdout
parser.add_argument("--amqpdry", action="store_true", help="Do dry run for AMQP, i.e. print to stdout instead of sending to AMQP.")

# Enable storing to pickle -- implies amqpdry
parser.add_argument("--pickle", action="store_true", help="Store to pickle. Implies --amqpdry.")

# Start from given block
parser.add_argument("--startfrom", action="store", help="Start from block with given index")

# Stop at given block
parser.add_argument("--stopat", action="store", help="Stop at block with given index")


# Go through options passed.
args = parser.parse_args()

# Fetch a config file name, if given
config_fn = args.config if args.config else "namecoin_extractor.conf"

# Now parse config file options
scp = ConfigParser.SafeConfigParser()
scp.read(config_fn)

config_read_fail = False
for section in ["namecoind", "logging", "pickle"]:
    if not scp.has_section(section):
        print("Missing section %s in config file." % section)
        config_read_fail = True

# Configure namecoind access
rpc_protocol = scp.get("namecoind", "rpc_protocol") if scp.has_option("namecoind", "rpc_protocol") and scp.get("namecoind", "rpc_protocol") != "" else "http"
if not scp.has_option("namecoind", "rpc_host"):
    print("Missing 'rpc_host' in config file.")
    config_read_fail = True
else:
    rpc_host = scp.get("namecoind", "rpc_host")

rpc_port = scp.get("namecoind", "rpc_port") if scp.has_option("namecoind", "rpc_protocol") and scp.get("namecoind", "rpc_port") != "" else 8336

if not scp.has_option("namecoind", "rpc_user") or not scp.has_option("namecoind", "rpc_password"):
    print("Missing user and/or password in config file or environment.")
    config_read_fail = True
elif scp.get("namecoind", "rpc_user") == "" or scp.has_option("namecoind", "rpc_password") == "":
    print("Missing user and/or password in config file or environment.")
    config_read_fail = True
else:
    rpc_user = scp.get("namecoind", "rpc_user")
    rpc_password = scp.get("namecoind", "rpc_password")

# set up dry run
dry_run = True if args.amqpdry or args.pickle else False
pickle_enabled = True if args.pickle else False

# Check pickle configuration
if pickle_enabled:
    if not scp.has_option("pickle", "working_dir"):
        print("Missing option working_dir in pickle configuration.")
        config_read_fail = True
    else:
        pickle_path = scp.get("pickle", "working_dir")
        pickle_ext = "." + scp.get("pickle", "result_extension") if scp.has_option("pickle", "result_extension") else ".pickle"

# Set up log
log_file = scp.get("logging", "log_file") if scp.has_option("logging", "log_file") and scp.get("logging", "log_file") != "" else "namecoin_extract.log"
level = logging.DEBUG if args.debug else logging.INFO
logging.basicConfig(filename=log_file, filemode="w", level=level, format='%(asctime)s:%(levelname)s:%(threadName)s: %(message)s') 

last_known_block = -1
if not scp.has_section("state") or (scp.has_section("state") and not scp.has_option("state", "last_known_block")):
    if not scp.has_section:
        scp.add_section("state")
    scp.set("state", "last_known_block", "0")
else:
    last_known_block = scp.getint("state", "last_known_block")

if config_read_fail:
    sys.exit(-1)






def do_amqp(service_proxy, scp):
    global last_known_block

    """
    Tries to get new blocks from the blockchain. Sends to AMQP if successful
    and returns the index of the last freshly found block that has been 
    confirmed a certain number of times. Returns last known block if no new 
    blocks or not enough confirmations yet.
    """

    if dry_run:
        print("Dry-run selected.")

    # Go through configuration
    for option in ["amqp_host", "amqp_port", "amqp_exchange", "amqp_queue", "amqp_user", "amqp_password", "amqp_routing_key"]:
        if option not in scp.options("amqp"):
            print("Missing option %s in AMQP confguration." % option)
            return -1


    # set up AMQP
    if not dry_run:
        amqp_host = scp.get("amqp", "amqp_host") if not scp.get("amqp", "amqp_host") == "" else "localhost"
        amqp_port = scp.getint("amqp", "amqp_port") if not scp.get("amqp", "amqp_port") == "" else 8336
        amqp_exchange = scp.get("amqp", "amqp_exchange") if not scp.get("amqp", "amqp_exchange") == "" else "namecoin"
        amqp_queue = scp.get("amqp", "amqp_queue") if not scp.get("amqp", "amqp_queue") == "" else "namecoin"
        amqp_user = scp.get("amqp", "amqp_user") if not scp.get("amqp", "amqp_user") == "" else "guest"
        amqp_password = scp.get("amqp", "amqp_password") if not scp.get("amqp", "amqp_password") == "" else "guest"
        credentials = pika.PlainCredentials(amqp_user, amqp_password)
        parameters = pika.ConnectionParameters(host=amqp_host, port=amqp_port, virtual_host="/", credentials=credentials)
        connection = pika.BlockingConnection(parameters=parameters)
        channel = connection.channel()
        channel.exchange_declare(amqp_exchange, type="fanout")
        channel.queue_declare(queue=amqp_queue)
        channel.queue_bind(exchange=amqp_exchange, queue=amqp_queue)


    cur_block = int(args.startfrom) if args.startfrom else last_known_block + 1
    res_blocks_in_chain = service_proxy.getblockchaininfo()

    # We wait for 12 confirmations before accepting a block as incorporated
    last_block = int(args.stopat) if args.stopat else res_blocks_in_chain["blocks"] - 12

    print("Starting at block %s" % str(cur_block))
    print("Stopping at block %s" % str(last_block))

    if cur_block <= last_block:
        next_block_hash = service_proxy.getblockhash(cur_block)
    else:
        logging.info("No new blocks.")
        return (last_known_block, -1)

    while cur_block <= last_block:
        print("Going for block %s of %s" % (str(cur_block), str(last_block)))
        # Treat genesis block differently
        if cur_block == 0:
            with open("genesis_block.json", "r") as fh:
                block = json.load(fh, object_pairs_hook=OrderedDict)
        else:
            block = OrderedDict(service_proxy.getblock(next_block_hash))
        parsed_txs = OrderedDict()
        tx_index = 0
        for tx_id in block["tx"]:
            # Treat genesis TX differently
            if cur_block == 0:
                with open("genesis_tx.json", "r") as fh:
                    tx_dec = json.load(fh)
            else:
                # We should not get any TX errors after the genesis block. If we do, that's a problem and we 
                # exit gracefully!
                try:
                    tx_raw = service_proxy.getrawtransaction(tx_id)
                except Exception, e:
                    logging.info("Tx " + tx_id + "cannot be found. Bad.")
                    # abandon, this TX does not exist
                    print("TX: %s" % tx_id)
                    print("Raw TX not found. This points to an inconsistency in the server's index. Better rollback on receiver's side. Exiting.")
                    if not dry_run: 
                        connection.close()
                    sys.exit(-1)

                # OK, decode and write to TX
                tx_dec = service_proxy.decoderawtransaction(tx_raw)

            # dive into vins to get the scriptSigs (they are not returned as JSON)
            for vin in tx_dec["vin"]:
                if "scriptSig" in vin:
                    script_sig_hex = vin["scriptSig"]["hex"]
                    vin["scriptSig"]["dec"] = service_proxy.decodescript(script_sig_hex)

            # Get these fancy addresses and check their validity
            addresses_valid = dict()
            for vout in tx_dec["vout"]:
                if "addresses" in vout["scriptPubKey"]:
                    for address in vout["scriptPubKey"]["addresses"]:
                        # It seems even an invalid address gets a correct JSON reply
                        address_reply = service_proxy.validateaddress(address)
                        addresses_valid[address] = address_reply["isvalid"]
            tx_dec["addresses_valid"] = addresses_valid

            # add the hash of the block this TX belongs to
            tx_dec["block_hash"] = next_block_hash
            # add the index of the TX in the serialised output
            tx_dec["tx_index"] = tx_index
            # Even though we use OrderedDicts, we need to serialsise them for pika,
            # and that does not seem to support it. So we need to have keys by which
            # we can order.
            parsed_txs[tx_index] = tx_dec
            tx_index = tx_index + 1

        msg = OrderedDict()
        msg["block"] = block
        msg["parsed_txs"] = parsed_txs
        if "auxpow" in block:
            # The TX in the auxpow is a coinbase TX (because it refers to
            # a merge-mined block). It lacks the block hash of the NMC block
            # where it is included, so we add that manually back:
            block["auxpow"]["block_hash"] = next_block_hash
            aux_tx = block["auxpow"]["tx"]
            aux_tx["block_hash"] = next_block_hash
            # The tx_index is always 0 because it is the only TX in the auxpow
            # Note: the NMC docs say there can be more than one. Doesn't matter,
            # this is not part of the analysis anyway.
            aux_tx["tx_index"] = 0
            msg["auxpow"] = block["auxpow"]

        if not dry_run:
            channel.basic_publish(exchange=amqp_exchange, routing_key=amqp_queue, body=json.dumps(msg))
        else:
            if pickle_enabled:
                pickle_file_name = str(msg["block"]["height"]) + pickle_ext 
                with open(pickle_path + "/" + pickle_file_name, "w+") as out_fh:
                    pickle.dump(msg, out_fh)
            else:
                print(msg)

        next_block_hash = block["nextblockhash"]
        last_known_block = cur_block
        cur_block = cur_block + 1
   
    if not dry_run:
        connection.close()



# Set up JSON-RPC connection
rpc_url = rpc_protocol + "://" + rpc_user + ":" + rpc_password + "@" + rpc_host + ":" + rpc_port
service_proxy = ServiceProxy(rpc_url)

# TODO sleep & wait if there are no new blocks (in continous operation)
do_amqp(service_proxy, scp)

with open(config_fn, "w") as config_fh:
    scp.set("state", "last_known_block", str(last_known_block))
    scp.write(config_fh)
