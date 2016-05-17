// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package discover implements the Node Discovery Protocol.
//
// The Node Discovery protocol provides a way to find RLPx nodes that
// can be connected to. It uses a Kademlia-like protocol to maintain a
// distributed database of the IDs and endpoints of all listening
// nodes.
package discover

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"sort"
	"sync"
	"time"
	"fmt"

	"github.com/pnomarev/go-ethereum/common"
	"github.com/pnomarev/go-ethereum/crypto"
	"github.com/pnomarev/go-ethereum/logger"
	"github.com/pnomarev/go-ethereum/logger/glog"
	
	"database/sql"
    _ "github.com/lib/pq"
	"bytes"
	
)

const (
	alpha      = 500; //3  // Kademlia concurrency factor
	bucketSize = 16 // Kademlia bucket size
	hashBits   = len(common.Hash{}) * 8
	nBuckets   = hashBits + 1 // Number of buckets

	maxBondingPingPongs = 16
	maxFindnodeFailures = 5

	autoRefreshInterval = 1 * time.Hour
	seedCount           = 30
	seedMaxAge          = 5 * 24 * time.Hour
)

type Table struct {
	mutex   sync.Mutex        // protects buckets, their content, and nursery
	buckets [nBuckets]*bucket // index of known nodes by distance
	nursery []*Node           // bootstrap nodes
	db      *nodeDB           // database of known nodes

	refreshReq chan struct{}
	closeReq   chan struct{}
	closed     chan struct{}

	bondmu    sync.Mutex
	bonding   map[NodeID]*bondproc
	bondslots chan struct{} // limits total number of active bonding processes

	nodeAddedHook func(*Node) // for testing

	net  transport
	self *Node // metadata of the local node
	
	sqlScripts chan string
	failedSqlScripts chan string

}

type bondproc struct {
	err  error
	n    *Node
	done chan struct{}
}

// transport is implemented by the UDP transport.
// it is an interface so we can test without opening lots of UDP
// sockets and without generating a private key.
type transport interface {
	ping(NodeID, *net.UDPAddr) error
	waitping(NodeID) error
	findnode(toid NodeID, addr *net.UDPAddr, target NodeID) ([]*Node, error)
	close()
}

// bucket contains nodes, ordered by their last activity. the entry
// that was most recently active is the first element in entries.
type bucket struct{ entries []*Node }

func newTable(t transport, ourID NodeID, ourAddr *net.UDPAddr, nodeDBPath string, crawling bool) *Table {
	// If no node database was given, use an in-memory one
	db, err := newNodeDB(nodeDBPath, Version, ourID)
	if err != nil {
		glog.V(logger.Warn).Infoln("Failed to open node database:", err)
		db, _ = newNodeDB("", Version, ourID)
	}
	tab := &Table{
		net:        t,
		db:         db,
		self:       newNode(ourID, ourAddr.IP, uint16(ourAddr.Port), uint16(ourAddr.Port)),
		bonding:    make(map[NodeID]*bondproc),
		bondslots:  make(chan struct{}, maxBondingPingPongs),
		refreshReq: make(chan struct{}),
		closeReq:   make(chan struct{}),
		closed:     make(chan struct{}),
	}
	for i := 0; i < cap(tab.bondslots); i++ {
		tab.bondslots <- struct{}{}
	}
	for i := range tab.buckets {
		tab.buckets[i] = new(bucket)
	}
	go tab.refreshLoop(crawling)
	return tab
}

// Self returns the local node.
// The returned node should not be modified by the caller.
func (tab *Table) Self() *Node {
	return tab.self
}

// ReadRandomNodes fills the given slice with random nodes from the
// table. It will not write the same node more than once. The nodes in
// the slice are copies and can be modified by the caller.
func (tab *Table) ReadRandomNodes(buf []*Node) (n int) {
	tab.mutex.Lock()
	defer tab.mutex.Unlock()
	// TODO: tree-based buckets would help here
	// Find all non-empty buckets and get a fresh slice of their entries.
	var buckets [][]*Node
	for _, b := range tab.buckets {
		if len(b.entries) > 0 {
			buckets = append(buckets, b.entries[:])
		}
	}
	if len(buckets) == 0 {
		return 0
	}
	// Shuffle the buckets.
	for i := uint32(len(buckets)) - 1; i > 0; i-- {
		j := randUint(i)
		buckets[i], buckets[j] = buckets[j], buckets[i]
	}
	// Move head of each bucket into buf, removing buckets that become empty.
	var i, j int
	for ; i < len(buf); i, j = i+1, (j+1)%len(buckets) {
		b := buckets[j]
		buf[i] = &(*b[0])
		buckets[j] = b[1:]
		if len(b) == 1 {
			buckets = append(buckets[:j], buckets[j+1:]...)
		}
		if len(buckets) == 0 {
			break
		}
	}
	return i + 1
}

func randUint(max uint32) uint32 {
	if max == 0 {
		return 0
	}
	var b [4]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:]) % max
}

// Close terminates the network listener and flushes the node database.
func (tab *Table) Close() {
	select {
	case <-tab.closed:
		// already closed.
	case tab.closeReq <- struct{}{}:
		<-tab.closed // wait for refreshLoop to end.
	}
}

// Bootstrap sets the bootstrap nodes. These nodes are used to connect
// to the network if the table is empty. Bootstrap will also attempt to
// fill the table by performing random lookup operations on the
// network.
func (tab *Table) Bootstrap(nodes []*Node) {
	tab.mutex.Lock()
	// TODO: maybe filter nodes with bad fields (nil, etc.) to avoid strange crashes
	tab.nursery = make([]*Node, 0, len(nodes))
	for _, n := range nodes {
		cpy := *n
		cpy.sha = crypto.Sha3Hash(n.ID[:])
		tab.nursery = append(tab.nursery, &cpy)
	}
	tab.mutex.Unlock()
	tab.requestRefresh()
}

/*
if mysqldb == nil {
	dsn := DB_USER + ":" + DB_PASS + "@" + DB_HOST + "/" + DB_NAME + "?charset=utf8"
    mysqldb, _ = sql.Open("mysql", dsn)
 	
    mysqldb.SetMaxIdleConns(1000)
    mysqldb.SetMaxOpenConns(10)
    
 	//mysqldb.Exec("ALTER TABLE `ethereum`.`ethereum_peers` ADD COLUMN `peer_id` VARCHAR(240) NULL AFTER `sending_txs`;")
 
}
*/

func (tab *Table) sqlLoop(workerID int) {

	var psqldb *sql.DB = nil
	
	//dsn := DB_USER + ":" + DB_PASS + "@" + DB_HOST + "/" + DB_NAME + "?charset=utf8"
    //mysqldb, _ = sql.Open("mysql", dsn)
    
    psqldb, _ = sql.Open("postgres", "postgres://ethereum:ethereum@localhost/db_ethereum?sslmode=disable") //?sslmode=verify-full
     
	select {
		case script := <- tab.failedSqlScripts:
			_, err := psqldb.Exec(script)
				
			if err != nil {
		        fmt.Println(script)
		        fmt.Println(err)
		        
		        time.Sleep(5000)
		        
		        tab.failedSqlScripts <- script
		    }
			
		default:
	}
	
	for script := range tab.sqlScripts {
		
		/*
		ln := len(tab.sqlScripts)
		
		if ln > 0 {
			fmt.Printf("                                                  [%d]  script remaining: %d\n", workerID, ln)
		}
		*/
		
		_, err := psqldb.Exec(script)
				
		if err != nil {
	        fmt.Println(script)
	        fmt.Println(err)
	        
	        time.Sleep(5000)
	        
	        tab.failedSqlScripts <- script
	    }
		
	}
}
// Lookup performs a network search for nodes close
// to the given target. It approaches the target by querying
// nodes that are closer to it on each iteration.
// The given target does not need to be an actual node
// identifier.
func (tab *Table) ScanKadNetwork(targetID NodeID) []*Node {
	tab.sqlScripts = make(chan string, 1000)
	tab.failedSqlScripts = make(chan string, 100)
	
	go tab.sqlLoop(0)
	go tab.sqlLoop(1)
	go tab.sqlLoop(2)
	
	/*
	
	go tab.sqlLoop(3)
	go tab.sqlLoop(4)
	go tab.sqlLoop(5)
	go tab.sqlLoop(6)
	go tab.sqlLoop(7)
	go tab.sqlLoop(8)
	go tab.sqlLoop(9)
	go tab.sqlLoop(10)
	go tab.sqlLoop(11)
	go tab.sqlLoop(12)
	go tab.sqlLoop(13)
	go tab.sqlLoop(14)
	go tab.sqlLoop(15)
	go tab.sqlLoop(16)
	go tab.sqlLoop(17)
	go tab.sqlLoop(18)
	go tab.sqlLoop(19)
	*/
	
	var (
		target         = crypto.Sha3Hash(targetID[:])
		asked          = make(map[NodeID]bool)
		reply          = make(chan []*Node, alpha)
		pendingQueries = 0
	)
	// don't query further if we hit ourself.
	// unlikely to happen often in practice.
	asked[tab.self.ID] = true

	tab.mutex.Lock()
	// generate initial result set
	result := tab.closest(target, bucketSize)
	tab.mutex.Unlock()

	// If the result set is empty, all nodes were dropped, refresh.
	if len(result.entries) == 0 {
		tab.requestRefresh()
		return nil
	}

	var (
		un uniqueNodes
	)
	
	un.entries = make([]*Node, len(result.entries))
	
	copy(un.entries, result.entries[:])
	
	un.entriesUnique = make(map[NodeID]bool)
	un.entriesUniqueDB = make(map[NodeID]bool)
	
	pos := 0
	
	var randomTarget NodeID
	found := 0
	
	powTargets := make(map[int]NodeID)
	
	
	bits := uint(8 + 5)
	
	secondbits := uint(bits - 8)
	
	rightShift := uint(8-secondbits)
	
	//m2 := 1 << secondbits // 2 ^ (8 - bits)
	//m1 := 256 / m2
	//and2 := (255 << secondbits) & 255 // 0b11111000
	
	totalBuckets := 1 << bits // 2^bits
	
	//0..8191
	//1000000 iterations to find good targets
	for i := 0; i < 50000000; i++{
		rand.Read(randomTarget[:])
	
		temp := crypto.Sha3Hash(randomTarget[:])
		
		key := ( (int(temp[0])*256 + int(temp[1])) >> rightShift )
		
		h, ok := powTargets[key]
	
		if !ok {
			powTargets[key] = randomTarget
			found++
			fmt.Println("found good targets:", found, "need:", totalBuckets)
			
		} else {
			temp_now := crypto.Sha3Hash(h[:])
			
			for j := 0; ; j++ {
				if temp[j] < temp_now[j] {
					//replace
					powTargets[key] = randomTarget
					
				} else if temp[j] > temp_now[j] {
					break
				}
			}
		}
	}
	/*
	for k, value := range powTargets {
		fmt.Println(k, crypto.Sha3Hash(value[:]).Hex())
		
	}*/
	round := 0
	roundNewNodes := 0
	prevRound := -1
	
	for {
		//fmt.Println("entries length: ", len(un.entries), " currently at:", pos, " pendingQueries:", pendingQueries, "            nodes found:", len(un.entriesUnique))
		
		targetID = powTargets[int(round % totalBuckets)]
		targetSha := crypto.Sha3Hash(targetID[:]).Hex()
		
		if round > prevRound {
			fmt.Println("Starting round", round, " target SHA:",  targetSha)
			prevRound = round
		}
	
		// ask the alpha closest nodes that we haven't asked yet
		for i := pos; i < len(un.entries) && pendingQueries < alpha; i++ {
			n := un.entries[i]
			if !asked[n.ID] {
				pos = i
				asked[n.ID] = true
				pendingQueries++
				go func() {
					
					// Find potential neighbors to bond with
					r, _ := tab.net.findnode(n.ID, n.addr(), targetID)
					
					var findnodesInsSqlBuffer bytes.Buffer
					firstFindNode := true
					
					findnodesInsSqlBuffer.WriteString(fmt.Sprintf("INSERT INTO ethereum_peers_rounds_details(round_id, asked_peer_ip, reply_ts, peer_ip, peer_id, sha, targetSha) VALUES"))
	
					ts := time.Now().Unix()
					
					for _, repl := range r {
						
						if !firstFindNode {
							findnodesInsSqlBuffer.WriteString("\n,")
						}
						firstFindNode = false
									
						peerIP := repl.addr().String()
						peerID := repl.ID.String()
						sha := crypto.Sha3Hash(repl.ID[:]).Hex()
						askedPeerID := n.addr().String()
						
						findnodesInsSqlBuffer.WriteString(fmt.Sprintf("(%d, '%s', to_timestamp(%d), '%s', '%s', '%s', '%s')",
							round, askedPeerID, ts, peerIP, peerID, sha, targetSha))
						
					}
					
					if !firstFindNode {
						findnodesInsSqlBuffer.WriteString(";\n")
						tab.sqlScripts <- findnodesInsSqlBuffer.String()
					}
						
					reply <- tab.bondall(r)
				}()
			}
		}
		if pendingQueries == 0 {
			
			//if round == 10 {
			//	// we have asked all closest nodes, stop the search
//				break
			
			//} else {
				// reset pos, and start over
				fmt.Println("round", round, " finished. new nodes found:", roundNewNodes)
				
				pos = 0
				round++
				asked          = make(map[NodeID]bool)
				un.entriesUniqueDB = make(map[NodeID]bool)
				
				roundNewNodes = 0
				//sleep for 5 minutes
				//time.Sleep(5 * time.Minute)
				
				continue
				
			//}
			
		}
		// wait for the next reply
		for _, n := range <-reply {
			if n != nil { //&& !seen[n.ID] {
				newCnt := un.push(n)
				
				roundNewNodes += newCnt
				peerIP := n.addr().String()
				peerID := n.ID.String()
				sha := crypto.Sha3Hash(n.ID[:]).Hex()
				
				if !un.entriesUniqueDB[n.ID] {
					
					un.entriesUniqueDB[n.ID] = true
				
					//sqlScript := fmt.Sprintf("INSERT INTO ethereum_peers_rounds(round_id, peer_ip, first_seen_ts, last_seen_ts, sending_txs, peer_id, sha, targetSha) VALUES(%d, '%s', now(), now(), 0, '%s', '%s', '%s') ON DUPLICATE KEY UPDATE last_seen_ts=now(), peer_id='%s';\n",
					//	round, peerIP, peerID, sha, targetSha, peerID);
					
					sqlScript := fmt.Sprintf("INSERT INTO ethereum_peers_rounds(round_id, peer_ip, first_seen_ts, last_seen_ts, sending_txs, peer_id, sha, targetSha) VALUES(%d, '%s', now(), now(), 0, '%s', '%s', '%s') ON CONFLICT(round_id, peer_ip) DO UPDATE SET last_seen_ts=now(), peer_id='%s';\n",
						round, peerIP, peerID, sha, targetSha, peerID);
					
					tab.sqlScripts <- sqlScript
				}
				
			}
		}
		//fmt.Println("found", cnt, "new nodes")
		
		pendingQueries--
	}
	
	
	close(tab.sqlScripts)
	
	return result.entries
}


// Lookup performs a network search for nodes close
// to the given target. It approaches the target by querying
// nodes that are closer to it on each iteration.
// The given target does not need to be an actual node
// identifier.
func (tab *Table) Lookup(targetID NodeID) []*Node {
	
	var (
		target         = crypto.Sha3Hash(targetID[:])
		asked          = make(map[NodeID]bool)
		seen           = make(map[NodeID]bool)
		reply          = make(chan []*Node, alpha)
		pendingQueries = 0
	)
	// don't query further if we hit ourself.
	// unlikely to happen often in practice.
	asked[tab.self.ID] = true

	//fmt.Printf("(%v) Acquiring lock on tab.mutex\n", time.Now())
	tab.mutex.Lock()
	// generate initial result set
	//fmt.Printf("(%v) Geting closest targets..\n", time.Now())
	result := tab.closest(target, bucketSize)
	//fmt.Printf("(%v) Unlocking lock on tab.mutex\n", time.Now())
	tab.mutex.Unlock()
	
	
	// If the result set is empty, all nodes were dropped, refresh.
	if len(result.entries) == 0 {
		tab.requestRefresh()
		return nil
	}

	var (
		un uniqueNodes
	)
	
	un.entries = make([]*Node, len(result.entries))
	
	copy(un.entries, result.entries[:])
	
	un.entriesUnique = make(map[NodeID]bool)
	un.entriesUniqueDB = make(map[NodeID]bool)
	
	pos := 0
	
	sha := crypto.Sha3Hash(targetID[:])
	
	fmt.Println(time.Now(), "Lookup started. Target:", sha.Hex())
		
	for {
		
		//fmt.Println("Lookup at", pos, "out of", len(un.entries))
		
		// ask the alpha closest nodes that we haven't asked yet
		for i := pos; i < len(un.entries) && pendingQueries < alpha; i++ {
			n := un.entries[i]
			if !asked[n.ID] {
				pos = i
				asked[n.ID] = true
				pendingQueries++
				go func() {
					//var randomTarget NodeID

					//rand.Read(randomTarget[:])
		
					// Find potential neighbors to bond with
					r, err := tab.net.findnode(n.ID, n.addr(), targetID)
					if err != nil {
						// Bump the failure counter to detect and evacuate non-bonded entries
						fails := tab.db.findFails(n.ID) + 1
						tab.db.updateFindFails(n.ID, fails)
						glog.V(logger.Detail).Infof("Bumping failures for %x: %d", n.ID[:8], fails)

						if fails >= maxFindnodeFailures {
							glog.V(logger.Detail).Infof("Evacuating node %x: %d findnode failures", n.ID[:8], fails)
							tab.delete(n)
						}
					}
					reply <- tab.bondall(r)
				}()
			}
		}
		if pendingQueries == 0 {
			// we have asked all closest nodes, stop the search
			break
		}
		// wait for the next reply
		for _, n := range <-reply {
			if n != nil && !seen[n.ID] {
				un.push(n)
				seen[n.ID] = true
				//result.push(n, bucketSize)
			}
		}
		pendingQueries--
	}
	
	fmt.Println("Lookup finished. found ", len(un.entries), "nodes")
	
	return un.entries
}

func (tab *Table) requestRefresh() {
	select {
	case tab.refreshReq <- struct{}{}:
	case <-tab.closed:
	}
}

func (tab *Table) refreshLoop(crawling bool) {
	defer func() {
		tab.db.close()
		if tab.net != nil {
			tab.net.close()
		}
		close(tab.closed)
	}()

	timer := time.NewTicker(autoRefreshInterval)
	var done chan struct{}
	
	for {
		select {
		case <-timer.C:
			if done == nil {
				done = make(chan struct{})
				go tab.doRefresh(done, crawling)
			}
		case <-tab.refreshReq:
			if done == nil {
				done = make(chan struct{})
				go tab.doRefresh(done, crawling)
			}
		case <-done:
			done = nil
		case <-tab.closeReq:
			if done != nil {
				<-done
			}
			return
		}
	}
}

// doRefresh performs a lookup for a random target to keep buckets
// full. seed nodes are inserted if the table is empty (initial
// bootstrap or discarded faulty peers).
func (tab *Table) doRefresh(done chan struct{}, crawling bool) {
	
	defer close(done)

	if crawling {
		fmt.Println("crawling doRefresh")
		
		
		seeds := tab.db.querySeeds(seedCount, seedMaxAge)
		seeds = tab.bondall(append(seeds, tab.nursery...))
		if glog.V(logger.Debug) {
			if len(seeds) == 0 {
				glog.Infof("no seed nodes found")
			}
			for _, n := range seeds {
				age := time.Since(tab.db.lastPong(n.ID))
				glog.Infof("seed node (age %v): %v", age, n)
			}
		}
		tab.mutex.Lock()
		tab.stuff(seeds)
		tab.mutex.Unlock()
		
		return
	}
	fmt.Println("doRefresh")
	
	// The Kademlia paper specifies that the bucket refresh should
	// perform a lookup in the least recently used bucket. We cannot
	// adhere to this because the findnode target is a 512bit value
	// (not hash-sized) and it is not easily possible to generate a
	// sha3 preimage that falls into a chosen bucket.
	// We perform a lookup with a random target instead.
	var target NodeID
	rand.Read(target[:])
	result := tab.Lookup(target)
	
	fmt.Println("refresh completed")
	
	if len(result) > 0 {
		return
	}

	// The table is empty. Load nodes from the database and insert
	// them. This should yield a few previously seen nodes that are
	// (hopefully) still alive.
	seeds := tab.db.querySeeds(seedCount, seedMaxAge)
	seeds = tab.bondall(append(seeds, tab.nursery...))
	if glog.V(logger.Debug) {
		if len(seeds) == 0 {
			glog.Infof("no seed nodes found")
		}
		for _, n := range seeds {
			age := time.Since(tab.db.lastPong(n.ID))
			glog.Infof("seed node (age %v): %v", age, n)
		}
	}
	tab.mutex.Lock()
	tab.stuff(seeds)
	tab.mutex.Unlock()

	// Finally, do a self lookup to fill up the buckets.
	
	fmt.Println("starting filling buckets")
	
	tab.Lookup(tab.self.ID)
	
	fmt.Println("fill buckets completed")
}

// closest returns the n nodes in the table that are closest to the
// given id. The caller must hold tab.mutex.
func (tab *Table) closest(target common.Hash, nresults int) *nodesByDistance {
	// This is a very wasteful way to find the closest nodes but
	// obviously correct. I believe that tree-based buckets would make
	// this easier to implement efficiently.
	close := &nodesByDistance{target: target}
	for _, b := range tab.buckets {
		for _, n := range b.entries {
			close.push(n, nresults)
		}
	}
	return close
}

func (tab *Table) len() (n int) {
	for _, b := range tab.buckets {
		n += len(b.entries)
	}
	return n
}

// bondall bonds with all given nodes concurrently and returns
// those nodes for which bonding has probably succeeded.
func (tab *Table) bondall(nodes []*Node) (result []*Node) {
	rc := make(chan *Node, len(nodes))
	for i := range nodes {
		go func(n *Node) {
			nn, _ := tab.bond(false, n.ID, n.addr(), uint16(n.TCP))
			rc <- nn
		}(nodes[i])
	}
	for _ = range nodes {
		if n := <-rc; n != nil {
			result = append(result, n)
		}
	}
	return result
}

// bond ensures the local node has a bond with the given remote node.
// It also attempts to insert the node into the table if bonding succeeds.
// The caller must not hold tab.mutex.
//
// A bond is must be established before sending findnode requests.
// Both sides must have completed a ping/pong exchange for a bond to
// exist. The total number of active bonding processes is limited in
// order to restrain network use.
//
// bond is meant to operate idempotently in that bonding with a remote
// node which still remembers a previously established bond will work.
// The remote node will simply not send a ping back, causing waitping
// to time out.
//
// If pinged is true, the remote node has just pinged us and one half
// of the process can be skipped.
func (tab *Table) bond(pinged bool, id NodeID, addr *net.UDPAddr, tcpPort uint16) (*Node, error) {
	// Retrieve a previously known node and any recent findnode failures
	node, fails := tab.db.node(id), 0
	if node != nil {
		fails = tab.db.findFails(id)
	}
	// If the node is unknown (non-bonded) or failed (remotely unknown), bond from scratch
	var result error
	age := time.Since(tab.db.lastPong(id))
	if node == nil || fails > 0 || age > nodeDBNodeExpiration {
		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//glog.V(logger.Detail).Infof("Bonding %x: known=%t, fails=%d age=%v", id[:8], node != nil, fails, age)

		tab.bondmu.Lock()
		w := tab.bonding[id]
		if w != nil {
			// Wait for an existing bonding process to complete.
			tab.bondmu.Unlock()
			<-w.done
		} else {
			// Register a new bonding process.
			w = &bondproc{done: make(chan struct{})}
			tab.bonding[id] = w
			tab.bondmu.Unlock()
			// Do the ping/pong. The result goes into w.
			tab.pingpong(w, pinged, id, addr, tcpPort)
			// Unregister the process after it's done.
			tab.bondmu.Lock()
			delete(tab.bonding, id)
			tab.bondmu.Unlock()
		}
		// Retrieve the bonding results
		result = w.err
		if result == nil {
			node = w.n
		}
	}
	if node != nil {
		// Add the node to the table even if the bonding ping/pong
		// fails. It will be relaced quickly if it continues to be
		// unresponsive.
		tab.add(node)
		tab.db.updateFindFails(id, 0)
	}
	return node, result
}

func (tab *Table) pingpong(w *bondproc, pinged bool, id NodeID, addr *net.UDPAddr, tcpPort uint16) {
	// Request a bonding slot to limit network usage
	<-tab.bondslots
	defer func() { tab.bondslots <- struct{}{} }()

	// Ping the remote side and wait for a pong.
	if w.err = tab.ping(id, addr); w.err != nil {
		close(w.done)
		return
	}
	if !pinged {
		// Give the remote node a chance to ping us before we start
		// sending findnode requests. If they still remember us,
		// waitping will simply time out.
		tab.net.waitping(id)
	}
	// Bonding succeeded, update the node database.
	w.n = newNode(id, addr.IP, uint16(addr.Port), tcpPort)
	tab.db.updateNode(w.n)
	close(w.done)
}

// ping a remote endpoint and wait for a reply, also updating the node
// database accordingly.
func (tab *Table) ping(id NodeID, addr *net.UDPAddr) error {
	tab.db.updateLastPing(id, time.Now())
	if err := tab.net.ping(id, addr); err != nil {
		return err
	}
	tab.db.updateLastPong(id, time.Now())

	// Start the background expiration goroutine after the first
	// successful communication. Subsequent calls have no effect if it
	// is already running. We do this here instead of somewhere else
	// so that the search for seed nodes also considers older nodes
	// that would otherwise be removed by the expiration.
	tab.db.ensureExpirer()
	return nil
}

// add attempts to add the given node its corresponding bucket. If the
// bucket has space available, adding the node succeeds immediately.
// Otherwise, the node is added if the least recently active node in
// the bucket does not respond to a ping packet.
//
// The caller must not hold tab.mutex.
func (tab *Table) add(new *Node) {
	b := tab.buckets[logdist(tab.self.sha, new.sha)]
	tab.mutex.Lock()
	defer tab.mutex.Unlock()
	if b.bump(new) {
		return
	}
	var oldest *Node
	if len(b.entries) == bucketSize {
		oldest = b.entries[bucketSize-1]
		if oldest.contested {
			// The node is already being replaced, don't attempt
			// to replace it.
			return
		}
		oldest.contested = true
		// Let go of the mutex so other goroutines can access
		// the table while we ping the least recently active node.
		tab.mutex.Unlock()
		err := tab.ping(oldest.ID, oldest.addr())
		tab.mutex.Lock()
		oldest.contested = false
		if err == nil {
			// The node responded, don't replace it.
			return
		}
	}
	added := b.replace(new, oldest)
	if added && tab.nodeAddedHook != nil {
		tab.nodeAddedHook(new)
	}
}

// stuff adds nodes the table to the end of their corresponding bucket
// if the bucket is not full. The caller must hold tab.mutex.
func (tab *Table) stuff(nodes []*Node) {
outer:
	for _, n := range nodes {
		if n.ID == tab.self.ID {
			continue // don't add self
		}
		bucket := tab.buckets[logdist(tab.self.sha, n.sha)]
		for i := range bucket.entries {
			if bucket.entries[i].ID == n.ID {
				continue outer // already in bucket
			}
		}
		if len(bucket.entries) < bucketSize {
			bucket.entries = append(bucket.entries, n)
			if tab.nodeAddedHook != nil {
				tab.nodeAddedHook(n)
			}
		}
	}
}

// delete removes an entry from the node table (used to evacuate
// failed/non-bonded discovery peers).
func (tab *Table) delete(node *Node) {
	tab.mutex.Lock()
	defer tab.mutex.Unlock()
	bucket := tab.buckets[logdist(tab.self.sha, node.sha)]
	for i := range bucket.entries {
		if bucket.entries[i].ID == node.ID {
			bucket.entries = append(bucket.entries[:i], bucket.entries[i+1:]...)
			return
		}
	}
}

func (b *bucket) replace(n *Node, last *Node) bool {
	// Don't add if b already contains n.
	for i := range b.entries {
		if b.entries[i].ID == n.ID {
			return false
		}
	}
	// Replace last if it is still the last entry or just add n if b
	// isn't full. If is no longer the last entry, it has either been
	// replaced with someone else or became active.
	if len(b.entries) == bucketSize && (last == nil || b.entries[bucketSize-1].ID != last.ID) {
		return false
	}
	if len(b.entries) < bucketSize {
		b.entries = append(b.entries, nil)
	}
	copy(b.entries[1:], b.entries)
	b.entries[0] = n
	return true
}

func (b *bucket) bump(n *Node) bool {
	for i := range b.entries {
		if b.entries[i].ID == n.ID {
			// move it to the front
			copy(b.entries[1:], b.entries[:i])
			b.entries[0] = n
			return true
		}
	}
	return false
}

// nodesByDistance is a list of nodes, ordered by
// distance to target.
type nodesByDistance struct {
	entries []*Node
	target  common.Hash
}

// uniqueNodes is a list of nodes, ordered by
// distance to target.
type uniqueNodes struct {
	entries []*Node
	entriesUnique map[NodeID]bool
	target  common.Hash
	
	entriesUniqueDB map[NodeID]bool
}

// push adds the given node to the list, keeping the total size below maxElems.
func (h *nodesByDistance) push(n *Node, maxElems int) {
	//fmt.Println("target:", h.target.Hex())
	//fmt.Println("node:",n.sha.Hex())
	ix := sort.Search(len(h.entries), func(i int) bool {
		return distcmp(h.target, h.entries[i].sha, n.sha) > 0
	})
	if len(h.entries) < maxElems {
		h.entries = append(h.entries, n)
	}
	if ix == len(h.entries) {
		// farther away than all nodes we already have.
		// if there was room for it, the node is now the last element.
	} else {
		// slide existing entries down to make room
		// this will overwrite the entry we just appended.
		copy(h.entries[ix+1:], h.entries[ix:])
		h.entries[ix] = n
	}
	//fmt.Println("----")
	//for _, entry := range h.entries {
	//	fmt.Println(entry.sha.Hex())
	//}
}


// push adds the given node to the list, keeping the total size below maxElems.
func (h *uniqueNodes) push(n *Node) int {
	if _, ok := h.entriesUnique[n.ID]; !ok {
		h.entriesUnique[n.ID] = true
		h.entries = append(h.entries, n)
		
		return 1
	}
	return 0
}