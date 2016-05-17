var amqp = require('amqp');
var Web3 = require("web3");
//var BigNumber = require("bignumber.js");
var winston = require("winston");


var RPC_SERVER = "http://localhost:8545";

var web3 = new Web3(new Web3.providers.HttpProvider(RPC_SERVER));

var BLOCK_REWARD = 5,
	FIRST_BLOCK = 0,
	MAXIMUM_BLOCK = web3.eth.blockNumber;


if(!web3.isConnected()){
	winston.log("error", "web3 is not connected to the RPC");
	process.exit(1);
}


var connection = amqp.createConnection({ 'host': 'localhost' });


var queue_blocks_from = function (block_number) {
	web3.eth.getBlock(block_number, true, function(error, result) {
		if(error) {
			winston.log("error", "Error getting block number: " + block_number + ". '" + error + "'");
		} else {
			if(result){
				if(block_number % REPORT_FREQUENCY_BLOCKS == 0){ 
					winston.log("info", "Queueing Block #" + block_number + " (with " + result.transactions.length + " transactions).");
				}
				connection.publish("ethblocks", result);
			} else {
				winston.log("warn", "No block seen for #" + block_number);
			}
		}
		if(block_number < MAXIMUM_BLOCK){
			queue_blocks_from(block_number + 1)
		}
	});
	};



connection.on('ready', function () {
	queue_blocks_from(FIRST_BLOCK);
});
