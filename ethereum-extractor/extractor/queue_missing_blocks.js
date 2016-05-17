var Web3 = require("web3");
var BigNumber = require("bignumber.js");
var winston = require("winston");
var amqp = require('amqp');

var pg = require("pg"),
	conn_string = "postgres://postgres@localhost:5432/blockchain_ethereum3",
	db = new pg.Client(conn_string);
db.connect();

var RPC_SERVER = "http://localhost:8545";

var web3 = new Web3(new Web3.providers.HttpProvider(RPC_SERVER));

var MAXIMUM_BLOCK = web3.eth.blockNumber;

var BLOCK_REWARD = 5;


if(!web3.isConnected()){
	winston.log("error", "web3 is not connected to the RPC");
	process.exit(1);
}



var connection = amqp.createConnection({ 'host': 'localhost' });


var queue_block = function (block_number) {
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
	});
	},
	queue_n_blocks = function(starting_block, n) {
		winston.log("info", "Queueing " + n + " blocks, from " + starting_block);
		for(var i = starting_block; i < (starting_block + n); i++){
			if(i <= MAXIMUM_BLOCK){
				queue_block(i);
			} else {
				winston.log("info", "skipped #" + i);
			}
		}
	};



connection.on('ready', function () {
	winston.log("info", "Rabbit Connection ready, starting to queue...");
	missing = [];
	var query = db.query("select num as missing from   generate_series(1, " + MAXIMUM_BLOCK + ") t(num) left join blocks  on (t.num = blocks.block_number) where blocks.block_number is null",
			function(err, result){
				result.rows.forEach(function(row) {
					missing.push(row.missing);
				});
				var repeat_queue_blocks = function(){
					winston.log("info", missing.length + " remaining...");

					if(missing.length){
						for(var i = 0; i < 500; i++){
							queue_block(missing.pop());
						}
						setTimeout(repeat_queue_blocks, 1100);
					}
				};
				winston.log("info", "Starting to queue blocks in 6 seconds");
				winston.log("info", missing.length + " remaining...");
				setTimeout(repeat_queue_blocks.bind(this, 0), 6000);
			});

});


winston.log("info", "Done!");
