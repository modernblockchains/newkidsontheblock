var Web3 = require("web3");
var BigNumber = require("bignumber");
var winston = require("winston");

var pg = require("pg"),
	conn_string = "postgres://postgres@localhost:5432/blockchain_ethereum",
	db = new pg.Client(conn_string);
db.connect();

var RPC_SERVER = "http://localhost:8545";

var BLOCK_REWARD = 5,
	LATEST_BLOCK = 1128107,
	FIRST_BLOCK = 46149;

var web3 = new Web3(new Web3.providers.HttpProvider(RPC_SERVER));

if(!web3.isConnected()){
	winston.log("error", "web3 is not connected to the RPC");
	process.exit(1);
}

var add_address = function (address, is_contract) {
		if(is_contract){
			is_contract = 1;
		} else {
			is_contract = 0;
		}
		db.query("INSERT INTO Addresses (address, address_type) VALUES ($1, $2) ON CONFLICT DO NOTHING", [address, is_contract]);
	},

	add_blocks_from = function (block_number) {
		web3.eth.getBlock(block_number, true, function (error, result) {
			if(error) {
				winston.log("error", "Error getting block number: " + block_number + ". '" + error + "'");
			} else {
				winston.log("info", "Adding Block #" + block_number + " (with " + result.transactions.length + " transactions).");
				// Insert miner into Addresses
				add_address(result.miner);

				db.query("INSERT INTO Blocks (block_number, block_hash, timestamp_utc, parent_hash, nonce, miner_addr, difficulty, size_bytes, block_reward) VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7, $8, $9)",
						[result.number, result.hash, result.timestamp, result.parentHash, result.nonce, result.miner, parseInt(result.difficulty), result.size, BLOCK_REWARD]);

				var cur_tx_type, cur_tx;
				for(var i = 0; i < result.transactions.length; i++){
					cur_tx = result.transactions[i];
					if(cur_tx.to == null){
						cur_tx_type = 2; // Contract Creation
					} else {
						if(web3.eth.getCode(cur_tx.to) == "0x"){
							cur_tx_type = 0; // Person to Person
						} else {
							cur_tx_type = 1; // Person to Contract
						}
						add_address(cur_tx.to, cur_tx_type);
					}

					add_address(cur_tx.from);

					db.query("INSERT INTO Transactions (tx_hash, tx_index, extra_data, transaction_type) VALUES ($1, $2, $3, $4)",
							[cur_tx.hash, cur_tx.transactionIndex, cur_tx.input, cur_tx_type]);
					db.query("INSERT INTO TxFromAddress (address, transaction_hash, input_value) VALUES ($1, $2, $3)",
							[cur_tx.from, cur_tx.hash, parseInt(cur_tx.value)]);
					if(cur_tx_type < 2){
						// i.e. the to field exists, not creating a contract
						db.query("INSERT INTO TxToAddress (address, transaction_hash, input_value) VALUES ($1, $2, $3)",
								[cur_tx.to, cur_tx.hash, parseInt(cur_tx.value)]);
					}
					db.query("INSERT INTO Transactions_Blocks (transaction_hash, block_hash) VALUES ($1, $2)",
							[cur_tx.hash, result.hash]);


				}

				if(result.number <= web3.eth.blockNumber && result.number <= LATEST_BLOCK){
					add_blocks_from(result.number + 1);
				}
			}
		});
	};






add_blocks_from(FIRST_BLOCK);


winston.log("info", "Done!");
