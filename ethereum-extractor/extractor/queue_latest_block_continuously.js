var amqp = require('amqp');
var Web3 = require("web3");
//var BigNumber = require("bignumber.js");
var winston = require("winston");


var RPC_SERVER = "http://localhost:8545";

var web3 = new Web3(new Web3.providers.HttpProvider(RPC_SERVER));

var BLOCK_REWARD = 5,
	FIRST_BLOCK = 1292677,
	MAXIMUM_BLOCK = web3.eth.blockNumber;


if(!web3.isConnected()){
	winston.log("error", "web3 is not connected to the RPC");
	process.exit(1);
}

var connection = amqp.createConnection({ 'host': 'localhost' });

var process_new_block = function(error, block_hash) {
	if(error) {
		winston.log("error", "Error processing new block.");
	} else {
		web3.eth.getBlock(block_hash, true, function(error, result) {
			if(error) {
				winston.log("error", "Error getting block hash: " + block_hash + ". '" + error + "'");
			} else {
				if(result){
					winston.log("info", "Queueing Block #" + result.number + " (with " + result.transactions.length + " transactions).");
					connection.publish("ethblocks", result);
				} else {
					winston.log("warn", "No block seen for #" + block_hash);
				}
			}
		});
	}
};

var queue_blocks_from = function (block_number) {
	web3.eth.getBlock(block_number, true, function(error, result) {
		if(error) {
			winston.log("error", "Error getting block number: " + block_number + ". '" + error + "'");
		} else {
			if(result){
				winston.log("info", "Queueing Block #" + block_number + " (with " + result.transactions.length + " transactions).");
				connection.publish("ethblocks", result);
			} else {
				winston.log("warn", "No block seen for #" + block_number);
			}
		}
		MAXIMUM_BLOCK = web3.eth.blockNumber;
		if(block_number < MAXIMUM_BLOCK){
			queue_blocks_from(block_number + 1)
		} else {
			winston.log("info", "We have caught up to the latest block #" + block_number + ", now listening...");
			web3.eth.filter("latest").watch(process_new_block);
		}
	});
	};



connection.on('ready', function () {
	// Queue blocks first, which will then call process_new_block once it gets to the latest
	queue_blocks_from(FIRST_BLOCK);
});
