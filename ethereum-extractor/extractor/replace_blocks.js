var Web3 = require("web3");
var BigNumber = require("bignumber");
var winston = require("winston");
var amqp = require('amqp');

var pg = require("pg"),
	conn_string = "postgres://postgres@localhost:5432/blockchain_ethereum4",
	db = new pg.Client(conn_string);
db.connect();

var connection = amqp.createConnection({ 'host': 'localhost' });

var RPC_SERVER = "http://localhost:8545";

var BLOCK_REWARD = 5,
	BLOCKS_TO_REPLACE = [1022629];

var web3 = new Web3(new Web3.providers.HttpProvider(RPC_SERVER));

if(!web3.isConnected()){
	winston.log("error", "web3 is not connected to the RPC");
	process.exit(1);
}

var delete_block_and_transactions = function (block_number) {
		web3.eth.getBlock(block_number, true, function (error, result) {
			if(error) {
				winston.log("error", "Error getting block number: " + block_number + ". '" + error + "'");
			} else {
				winston.log("info", "Deleting Block #" + block_number + " (with " + result.transactions.length + " transactions).");
				winston.log("info", "Block hash: " + result.hash);

				result.transactions.forEach(function(tx){
					winston.log("info", "    deleting transaction " + tx.transactionIndex + ": " + tx.hash);
					db.query("DELETE FROM TxFromAddress WHERE tx_hash = $1", [tx.hash]);
					db.query("DELETE FROM TxToAddress WHERE tx_hash = $1", [tx.hash]);
					db.query("DELETE FROM Transactions_Blocks WHERE tx_hash = $1", [tx.hash]);
					db.query("DELETE FROM Transactions WHERE tx_hash = $1", [tx.hash]);
				});
				db.query("DELETE FROM Addresses_Blocks WHERE block_hash = $1", [result.hash]);
				db.query("DELETE FROM Blocks WHERE block_hash = $1", [result.hash]);
				winston.log("info", "Queueing Block #" + block_number + " (with " + result.transactions.length + " transactions).");
				connection.publish("ethblocks", result);
			}
		});
	};


BLOCKS_TO_REPLACE.forEach(function(block_num) {
	delete_block_and_transactions(block_num);
});


winston.log("info", "Done!");
