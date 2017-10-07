package libkademlia

// Contains definitions mirroring the Kademlia spec. You will need to stick
// strictly to these to be compatible with the reference implementation and
// other groups' code.

import (
	"net"
	"fmt"
)

type KademliaRPC struct {
	kademlia *Kademlia
}

// Host identification.
type Contact struct {
	NodeID ID
	Host   net.IP
	Port   uint16
}

///////////////////////////////////////////////////////////////////////////////
// PING
///////////////////////////////////////////////////////////////////////////////
type PingMessage struct {
	Sender Contact
	MsgID  ID
}

type PongMessage struct {
	MsgID  ID
	Sender Contact
}

func (k *KademliaRPC) Ping(ping PingMessage, pong *PongMessage) error {
	// TODO: Finish implementation
	pong.MsgID = CopyID(ping.MsgID)
	// Specify the sender
	pong.Sender = k.kademlia.SelfContact
	// Update contact, etc
	k.kademlia.Senders <- ping.Sender
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// STORE
///////////////////////////////////////////////////////////////////////////////
type StoreRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
	Value  []byte
}

type StoreResult struct {
	MsgID ID
	Err   error
}

func (k *KademliaRPC) Store(req StoreRequest, res *StoreResult) error {
	// TODO: Implement.
	// fmt.Println("k.NodeID, req.Key, , req.Value", k.kademlia.NodeID, req.Key , req.Value)
	k.kademlia.kvPairs <- kv_pair{req.Key, req.Value}
	k.kademlia.Senders <- req.Sender
	res.MsgID = CopyID(req.MsgID)
	res.Err = nil
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// FIND_NODE
///////////////////////////////////////////////////////////////////////////////
type FindNodeRequest struct {
	Sender Contact
	MsgID  ID
	NodeID ID
}

type FindNodeResult struct {
	MsgID ID
	Nodes []Contact
	Err   error
}


func (k *KademliaRPC) FindNode(req FindNodeRequest, res *FindNodeResult) error {
	// TODO: Implement.
  // bids := k.kademlia.SortBuckets(req.NodeID)
	// var nodes []Contact
	// i := 0
	// for len(nodes) < K && i < len(bids){
	// 	diff = K - len(nodes)
	// 	bucket := k.buckets[bids[i]]
	// 	i += 1
	// 	if len(bucket) <= diff {
	// 		nodes = append(nodes, bucket...)
	// 	} else {
	// 		sortedBucket := SortInBucket(req.NodeID, bucket)
	// 		nodes = append(nodes, sortedBucket[:diff])
	// 	}
	// }
	k.kademlia.Senders <- req.Sender
	nodesChan := make(chan []Contact)
	k.kademlia.FindNodeChan <- FindNodeReq{req.NodeID, nodesChan}
	// res.Nodes = k.kademlia.FindClosestKNodes(req.NodeID)
	res.Nodes = <- nodesChan
	res.MsgID = CopyID(req.MsgID)
	res.Err = nil
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// FIND_VALUE
///////////////////////////////////////////////////////////////////////////////
type FindValueRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
}

// If Value is nil, it should be ignored, and Nodes means the same as in a
// FindNodeResult.
type FindValueResult struct {
	MsgID ID
	Value []byte
	Nodes []Contact
	Err   error
}

func (k *KademliaRPC) FindValue(req FindValueRequest, res *FindValueResult) error {
	// TODO: Implement.
	k.kademlia.Senders <- req.Sender
	// value, ok := k.kademlia.DHT[req.Key]
	// if ok {
	// 	res.Value = value
	// } else {
	// 	res.Nodes = k.kademlia.FindClosestKNodes(req.Key)
	// }
	vrplChan := make(chan ValueRpl)
  	k.kademlia.FindValueChan <- FindValueReq{req.Key, vrplChan}
	vrpl := <- vrplChan
	if vrpl.ok {
		res.Value = vrpl.value
		fmt.Println("ok is ", res.Value)
	} else {
		// fmt.Println("failed *****")
		nodesChan := make(chan []Contact)
		k.kademlia.FindNodeChan <- FindNodeReq{req.Key, nodesChan}
		res.Nodes = <- nodesChan
	}
	res.MsgID = CopyID(req.MsgID)
	res.Err = nil
	return nil
}

// For Project 3

type GetVDORequest struct {
	Sender Contact
	VdoID  ID
	MsgID  ID
}

type GetVDOResult struct {
	MsgID ID
	VDO   VanashingDataObject
}

type GetVDOResultHelper struct {
	Err   error
	VDO   VanashingDataObject
}

func (k *KademliaRPC) GetVDO(req GetVDORequest, res *GetVDOResult) error {

  	k.kademlia.VDOReqChan <- req
	VDOHelper := <- k.kademlia.VDOChan
	res.VDO = VDOHelper.VDO
	if VDOHelper.Err == nil{
		res.MsgID = CopyID(req.MsgID)
	}else{
		// ERROR
	}
	return nil
}
