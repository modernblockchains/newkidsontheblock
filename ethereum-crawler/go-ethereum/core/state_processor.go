package core

import (
	"math/big"
	"fmt"
	"time"
	"strings"

	"github.com/pnomarev/go-ethereum/common"

	"github.com/pnomarev/go-ethereum/core/state"
	"github.com/pnomarev/go-ethereum/core/types"
	"github.com/pnomarev/go-ethereum/core/vm"
	"github.com/pnomarev/go-ethereum/crypto"
	"github.com/pnomarev/go-ethereum/logger"
	"github.com/pnomarev/go-ethereum/logger/glog"
		
	"database/sql"
    _ "github.com/Go-SQL-Driver/MySQL"
//    "os"
    "bytes"
)

var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

type StateProcessor struct {
	bc *BlockChain

	sqlScripts chan string
	failedSqlScripts chan string
}

func NewStateProcessor(bc *BlockChain) *StateProcessor {
	sp := StateProcessor{
		bc:	bc,
		sqlScripts: make(chan string, 5001),
		failedSqlScripts: make(chan string, 1000),
	}
	
	go sp.sqlLoop(0)
	go sp.sqlLoop(1)
	go sp.sqlLoop(2)
	go sp.sqlLoop(3)
	go sp.sqlLoop(4)
	go sp.sqlLoop(5)
	go sp.sqlLoop(6)
	go sp.sqlLoop(7)

	return &sp
}

var storeToDB = true
var storeToFile = false


const (
    DB_HOST = "tcp(127.0.0.1:3306)"
    DB_NAME = "ethereum"
    DB_USER = "root"
    DB_PASS = "mysql"
)



func (p *StateProcessor) sqlLoop(workerID int) {

	var mysqldb *sql.DB = nil
	
	if storeToDB {
		//if mysqldb == nil {
			dsn := DB_USER + ":" + DB_PASS + "@" + DB_HOST + "/" + DB_NAME + "?charset=utf8"
		    mysqldb, _ = sql.Open("mysql", dsn)
		    
		    mysqldb.SetMaxIdleConns(1000)
		    mysqldb.SetMaxOpenConns(10)
	    //}
	}
	
	select {
		case script := <- p.failedSqlScripts:
			_, err := mysqldb.Exec(script)
				
			if err != nil {
		        fmt.Println(script)
		        fmt.Println(err)
		        
		        time.Sleep(5000)
		        
		        p.failedSqlScripts <- script
		    }
			
		default:
	}
	for script := range p.sqlScripts {
		
		ln := len(p.sqlScripts)
		
		if ln > 0 {
			fmt.Printf("                                                  [%d]  script remaining: %d\n", workerID, ln)
		}
		_, err := mysqldb.Exec(script)
				
		if err != nil {
	        fmt.Println(script)
	        fmt.Println(err)
	        
	        time.Sleep(5000)
	        
	        p.failedSqlScripts <- script
	    }
		
	}
}

func (p *StateProcessor) Stop() {
	close(p.sqlScripts)
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB) (types.Receipts, vm.Logs, *big.Int, error) {
	var (
		receipts     types.Receipts
		totalUsedGas = big.NewInt(0)
		err          error
		header       = block.Header()
		allLogs      vm.Logs
		gp           = new(GasPool).AddGas(block.GasLimit())
	)

	blockProcessed := fmt.Sprintf("Processing block #%s", block.Number().String())
	
	
	if block.Number().Cmp(big.NewInt(1465650)) < 0 && block.Number().Cmp(big.NewInt(0)) > 0 {
		storeToDB = false
	} else {
		storeToDB = true
	}
	
	if !storeToDB {
		blockProcessed = fmt.Sprintf("Processing block #%s : skipping db part", block.Number().String())
	}
	
	fmt.Println(blockProcessed)
	
/*
	if mysqldb == nil {
		dsn := DB_USER + ":" + DB_PASS + "@" + DB_HOST + "/" + DB_NAME + "?charset=utf8"
	    mysqldb, _ = sql.Open("mysql", dsn)
    }
*/
	
	blockHash := block.Hash().Hex()
	
	if (storeToDB) {
		blockInsSql := fmt.Sprintf("INSERT INTO blocks(block_number, processed_time, hash, parentHash,	nonce,sha3Uncles,transactionsRoot,stateRoot,receiptRoot,miner,difficulty,size,gasLimit, gasUsed, timestamp) VALUE(%s, FROM_UNIXTIME(%d), '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %d);\n", 
			block.Number().String(), //block_number
			time.Now().Unix(),  //processed_time
			blockHash,  //hash
			block.ParentHash().Hex(),  //parentHash
			block.Nonce(),  //nonce
			block.UncleHash().Hex(), //sha3Uncles
			block.TxHash().Hex(), //transactionsRoot
			block.Root().Hex(), //stateRoot
			block.ReceiptHash().Hex(), //receiptRoot
			block.Coinbase().Hex(), //miner
			block.Difficulty().Uint64(), //difficulty
			block.Size().Int64(), //size
			block.GasLimit().Uint64(), //gasLimit
			block.GasUsed().Uint64(), //gasUsed
			block.Time().Uint64()) // timestamp
	
	
		p.sqlScripts <- blockInsSql //= append(sqlScripts, blockInsSql)
	}
	
	
	for _, uncle := range block.Uncles() {
		unclesInsSql := fmt.Sprintf("INSERT INTO blocks_uncles(block_number, hash, uncle) VALUES(%s, '%s', '%s');\n", block.Number().String(), blockHash, uncle.Hash().Hex())
		
		if (storeToDB) {
			p.sqlScripts <- unclesInsSql //= append(sqlScripts, unclesInsSql)
		}
	}
	
	var transactionInsSqlBuffer bytes.Buffer
	var internalTransactionInsSqlBuffer bytes.Buffer
	//var internalTransactionDelSqlBuffer bytes.Buffer
	
	firstInternal := true
	firstTransaction := true
	
	transactionInsSqlBuffer.WriteString(fmt.Sprintf("INSERT INTO blocks_transactions(`block_number`,`blockHash`,`transactionIndex`,`hash`,`nonce`,`from`,`to`,`value`,`gas`,`gasPrice`,`input`,`contractAddress`,`cumulativeGasUsed`,`gasUsed`,`vmerr`) VALUES"))
	internalTransactionInsSqlBuffer.WriteString(fmt.Sprintf("INSERT INTO block_transactions_internals(`block_number`,`blockHash`, hash, internalIndex, depth, eventType, `from`, `to`, amount, codeHash, `input`) VALUES"))
	//internalTransactionDelSqlBuffer.WriteString(fmt.Sprintf("DELETE FROM block_transactions_internals WHERE hash in("))
	
	for i, tx := range block.Transactions() {
		statedb.StartRecord(tx.Hash(), block.Hash(), i)
		internals, receipt, logs, _, err, vmerr := ApplyTransaction(p.bc, gp, statedb, header, tx, totalUsedGas)
		if err != nil {
			return nil, nil, totalUsedGas, err
		}
		
		//if storeToDB {
			fromAddress, _ := tx.From()
			toAddress := tx.To()
			
			toAddressStr := "0x0000000000000000000000000000000000000000"
			
			if toAddress != nil {
				toAddressStr = toAddress.Hex()
			}
			if !firstTransaction {
				transactionInsSqlBuffer.WriteString("\n,")
				//internalTransactionDelSqlBuffer.WriteString("\n,")
			}
			firstTransaction = false
			
			vmErrFormatted := "null"
			
			if vmerr != "" {
				
				vmErrFormatted = fmt.Sprintf("'%s'", strings.Replace(vmerr, "'", "''", -1))
			}
			
			transactionInsSqlBuffer.WriteString(fmt.Sprintf("(%s, '%s', %d, '%s', %d, '%s', '%s', %s, %s, %s, '0x%s', '%s', %s, %s, %s)", 
				block.Number().String(),
				blockHash,
				i,
				tx.Hash().Hex(),
				tx.Nonce(),
				fromAddress.Hex(),
				toAddressStr,
				tx.Value().String(),
				tx.Gas().String(),
				tx.GasPrice().String(),
				common.Bytes2Hex(tx.Data()),
				receipt.ContractAddress.Hex(),
				receipt.CumulativeGasUsed.String(),
				receipt.GasUsed.String(),
				vmErrFormatted))
			
			for _, intern := range internals {
				//fmt.Println(intern.EventType)
				//fmt.Println(intern.Depth, "txHash =", tx.Hash().Hex() , ", ", intern.EventType )
				
				if !firstInternal {
					internalTransactionInsSqlBuffer.WriteString("\n,")
				}
				
				codeHash := "null"
				input := "null"
				
				if intern.CodeHash != nil {
					codeHash = fmt.Sprintf("'%s'", intern.CodeHash.Hex())
				}
				if intern.Input != nil {
					input = fmt.Sprintf("'%s'", common.Bytes2Hex(intern.Input))
				}
				
				firstInternal = false
				internalTransactionInsSqlBuffer.WriteString( fmt.Sprintf("(%s, '%s', '%s', %d, %d, '%s', '%s', '%s', %s, %s, %s)",
					block.Number().String(), blockHash, tx.Hash().Hex(), intern.InternalIndex, intern.Depth, intern.EventType, intern.From.Hex(), intern.To.Hex(), intern.Amount.String(),
					codeHash, input))
				
			}
		
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, logs...)
	}
	
	if !firstTransaction {
		transactionInsSqlBuffer.WriteString(";\n")
		if (storeToDB) {
			p.sqlScripts <- transactionInsSqlBuffer.String() //= append(sqlScripts, transactionInsSqlBuffer.String())
		}
	}
	
	if !firstInternal {
		internalTransactionInsSqlBuffer.WriteString(";\n")
		if (storeToDB) {
			p.sqlScripts <-  internalTransactionInsSqlBuffer.String() //sqlScripts = append(sqlScripts, internalTransactionInsSqlBuffer.String())
		}
	}
	
	AccumulateRewards(statedb, header, block.Uncles())

	return receipts, allLogs, totalUsedGas, err
}

// ApplyTransaction attemps to apply a transaction to the given state database
// and uses the input parameters for its environment.
//
// ApplyTransactions returns the generated receipts and vm logs during the
// execution of the state transition phase.
func ApplyTransaction(bc *BlockChain, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *big.Int) ([]vm.Internal, *types.Receipt, vm.Logs, *big.Int, error, string) {
	env := NewEnv(statedb, bc, tx, header)
	_, gas, err, vmerr := ApplyMessage(env, tx, gp)
	if err != nil {
		return nil, nil, nil, nil, err, vmerr
	}

	// Update the state with pending changes
	usedGas.Add(usedGas, gas)
	receipt := types.NewReceipt(statedb.IntermediateRoot().Bytes(), usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = new(big.Int).Set(gas)
	if MessageCreatesContract(tx) {
		from, _ := tx.From()
		receipt.ContractAddress = crypto.CreateAddress(from, tx.Nonce())
	}

	logs := statedb.GetLogs(tx.Hash())
	receipt.Logs = logs
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})

	glog.V(logger.Debug).Infoln(receipt)

	return env.GetInternals(), receipt, logs, gas, err, vmerr
}

// AccumulateRewards credits the coinbase of the given block with the
// mining reward. The total reward consists of the static block reward
// and rewards for included uncles. The coinbase of each uncle block is
// also rewarded.
func AccumulateRewards(statedb *state.StateDB, header *types.Header, uncles []*types.Header) {
	reward := new(big.Int).Set(BlockReward)
	r := new(big.Int)
	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, BlockReward)
		r.Div(r, big8)
		statedb.AddBalance(uncle.Coinbase, r)

		r.Div(BlockReward, big32)
		reward.Add(reward, r)
	}
	statedb.AddBalance(header.Coinbase, reward)
}
