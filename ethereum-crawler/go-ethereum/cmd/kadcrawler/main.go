// Copyright 2016 Data61

// kad crawler
package main

import (
	"fmt"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"regexp"
	"crypto/ecdsa"
	"github.com/pnomarev/go-ethereum/crypto"

	"github.com/pnomarev/go-ethereum/logger"
	"github.com/pnomarev/go-ethereum/logger/glog"
	"github.com/pnomarev/go-ethereum/p2p"
	"github.com/pnomarev/go-ethereum/p2p/nat"
	"github.com/pnomarev/go-ethereum/p2p/discover"
)

const (
	ClientIdentifier = "KadCrawler"
	DataDir = "C:\\temp\\dd"
	
)

var (
	portInUseErrRE     = regexp.MustCompile("address already in use")
	
	
	defaultBootNodes = []*discover.Node{
		// ETH/DEV Go Bootnodes
		discover.MustParseNode("enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e3bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303"), // IE
		discover.MustParseNode("enode://de471bccee3d042261d52e9bff31458daecc406142b401d4cd848f677479f73104b9fdeb090af9583d3391b7f10cb2ba9e26865dd5fca4fcdc0fb1e3b723c786@54.94.239.50:30303"),  // BR
		discover.MustParseNode("enode://1118980bf48b0a3640bdba04e0fe78b1add18e1cd99bf22d53daac1fd9972ad650df52176e7c7d89d1114cfef2bc23a2959aa54998a46afcf7d91809f0855082@52.74.57.123:30303"),  // SG
		// ETH/DEV cpp-ethereum (poc-9.ethdev.com)
		discover.MustParseNode("enode://979b7fa28feeb35a4741660a16076f1943202cb72b6af70d327f053e248bab9ba81760f39d0701ef1d8f89cc1fbd2cacba0710a12cd5314d5e0c9021aa3637f9@5.1.83.226:30303"),
	}
	
	nodes []*discover.Node
)

func nodeKey() (*ecdsa.PrivateKey, error) {
	

	// use persistent key if present
	keyfile := filepath.Join(DataDir, "nodekey")
	key, err := crypto.LoadECDSA(keyfile)
	if err == nil {
		return key, nil
	}
	// no persistent key, generate and store a new one
	if key, err = crypto.GenerateKey(); err != nil {
		return nil, fmt.Errorf("could not generate server key: %v", err)
	}
	if err := crypto.SaveECDSA(keyfile, key); err != nil {
		glog.V(logger.Error).Infoln("could not persist nodekey: ", err)
	}
	return key, nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	defer logger.Flush()
	
	netprv, err := nodeKey()
	if err != nil {
		fmt.Println(err)
		return
	}
	
	glog.SetV(6)
	glog.CopyStandardLogTo("INFO")
	glog.SetToStderr(true)

	protocols := append([]p2p.Protocol{})
	
	nodeDb := filepath.Join(DataDir, "nodes")
	
	Nat, _ := nat.Parse("any")
	
	net := &p2p.Server{
		ListenOnly:      true,
		CrawlOnly:       true,
		PrivateKey:      netprv,
		Name:            "geth/1.3.5",
		MaxPeers:        25,
		MaxPendingPeers: 25,
		Discovery:       true,
		Protocols:       protocols,
		NAT:             Nat,
		NoDial:          false,
		BootstrapNodes:  defaultBootNodes,
		StaticNodes:     nodes,
		TrustedNodes:    nodes,
		NodeDatabase:    nodeDb,
		
	}
	
	err = net.StartCrawler()
	if err != nil {
		if portInUseErrRE.MatchString(err.Error()) {
			err = fmt.Errorf("%v (possibly another instance of geth is using the same port)", err)
		}
		fmt.Println(err)
		return
	}
	
	net.WaitForCrawlCompleted()
	
}
/*
func startEth(eth *eth.Ethereum) {
	
	// Start Ethereum itself
	utils.StartEthereum(eth)

}*/
