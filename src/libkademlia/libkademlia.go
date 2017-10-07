package libkademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"
	// "encoding/binary"
)

const (
	alpha = 3
	b     = 8 * IDBytes
	K     = 20
)

type KBucket []Contact

type kv_pair struct {
	key ID
	value []byte
}

type FindNodeReq struct {
	NodeID ID
	NodesChan chan []Contact
}

type ValueRpl struct {
	value []byte
	ok bool
}
type FindValueReq struct {
	Key ID
	ValueChan chan ValueRpl
}
type FindContactRes struct{
	ContactAddress *Contact
	err error
}

type rpcResult struct {
	contact Contact
	Nodes []Contact
	err error
}

type rpcResultValue struct {
	contact Contact
	value []byte
	Nodes []Contact
	err error
}

// Kademlia type. You can put whatever state you need in this.
type Kademlia struct {
	NodeID      ID
	SelfContact Contact
	buckets [b]KBucket
	DHT map[ID][]byte
	VDO map[ID] VanashingDataObject
	Senders chan Contact
 
	NewContacts chan []Contact

	kvPairs chan kv_pair
	FindNodeChan chan FindNodeReq
	FindValueChan chan FindValueReq
	FindContactChan chan ID
	FindContactResChan chan FindContactRes
	VDOReqChan chan GetVDORequest
	VDOChan chan GetVDOResultHelper

	VDOStoreChan chan VanashingDataObject
	VDOStoreEndChan chan ID
	VDOIDChan chan ID
}

func NewKademliaWithId(laddr string, nodeID ID) *Kademlia {

	k := new(Kademlia)
	k.NodeID = nodeID
	k.DHT = make(map[ID][]byte)
	k.VDO = make(map[ID] VanashingDataObject)
	k.Senders = make(chan Contact, 2000)
	k.NewContacts = make(chan []Contact)
	k.kvPairs = make(chan kv_pair)
	// k.UpdateCH = make(chan bool, 100)
	k.FindNodeChan = make(chan FindNodeReq)
	k.FindValueChan = make(chan FindValueReq)

	k.FindContactChan = make(chan ID)
	k.FindContactResChan =make (chan FindContactRes)

	k.VDOReqChan  = make(chan GetVDORequest)
	k.VDOChan =make ( chan GetVDOResultHelper)

	k.VDOStoreChan = make(chan VanashingDataObject)
	k.VDOStoreEndChan = make(chan ID)
	k.VDOIDChan = make(chan ID)

	// TODO: Initialize other state here as you add functionality.

	// Set up RPC server
	// NOTE: KademliaRPC is just a wrapper around Kademlia. This type includes
	// the RPC functions.

	hostname, port, err := net.SplitHostPort(laddr)
	if err != nil {
		return nil
	}
 
	port_int, _ := strconv.Atoi(port)
	ipAddrStrings, err := net.LookupHost(hostname)
	var host net.IP
	for i := 0; i < len(ipAddrStrings); i++ {
 
		host = net.ParseIP(ipAddrStrings[i])
		if host.To4() != nil {
			break
		}
	}

	s := rpc.NewServer()
	s.Register(&KademliaRPC{k})

 	s.HandleHTTP(rpc.DefaultRPCPath+host.String()+port,
  			rpc.DefaultDebugPath+host.String()+port)
 

	l, err := net.Listen("tcp", laddr)
	if err != nil {
		fmt.Println("Kademlia server failed")
		log.Fatal("Listen: ", err)
	}

	// Run RPC server forever.
	go http.Serve(l, nil)

	// Add self contact
	hostname, port, _ = net.SplitHostPort(l.Addr().String())
	port_int, _ = strconv.Atoi(port)
	ipAddrStrings, err = net.LookupHost(hostname)
 
	for i := 0; i < len(ipAddrStrings); i++ {
		 
		host = net.ParseIP(ipAddrStrings[i])
		if host.To4() != nil {
			break
		}
	}
	k.SelfContact = Contact{k.NodeID, host, uint16(port_int)}
	go k.Update()

	return k
}

func NewKademlia(laddr string) *Kademlia {
	return NewKademliaWithId(laddr, NewRandomID())
}


func inBucket(bucket KBucket, nodeId ID) int {
	for idx, e := range bucket {
		if e.NodeID == nodeId {
			return idx
		}
	}
	return -1
}


func (k *Kademlia) UpdateBuckets(Sender Contact) {
 
	distance := k.NodeID.Xor(Sender.NodeID)
	i := distance.PrefixLen()
 
	idx := inBucket(k.buckets[i], Sender.NodeID)
 
	if  idx != -1 {

		c := k.buckets[i][idx]
		k.buckets[i] = append(k.buckets[i][:idx], k.buckets[i][idx+1:]...)
		k.buckets[i] = append(k.buckets[i], c)
	} else {
		if len(k.buckets[i]) < K {
			k.buckets[i] = append(k.buckets[i], Sender)
		} else {

			least_recent_contact, err := k.DoPing(k.buckets[i][0].Host, k.buckets[i][0].Port)
			if err != nil {
				k.buckets[i] = append(k.buckets[i][1:], Sender)
			} else {
				k.buckets[i] = append(k.buckets[i][1:], *least_recent_contact)
			}
		}
	}
}

func (k *Kademlia) addNewContacts(contacts []Contact) {
	for _, contact := range contacts {
		bucket_id := contact.NodeID.Xor(k.NodeID).PrefixLen()
		idx := inBucket(k.buckets[bucket_id], contact.NodeID)
		if idx == -1 {
			k.buckets[bucket_id] = append(k.buckets[bucket_id],contact)
		}
	}
}

func (k *Kademlia) Update() {
	for {
		select {
		case Sender := <- k.Senders:
			k.UpdateBuckets(Sender)
		case new_contacts := <- k.NewContacts:
			k.addNewContacts(new_contacts)
		case kv := <-k.kvPairs:
			k.DHT[kv.key] = kv.value
		case nodereq := <- k.FindNodeChan:
			nodereq.NodesChan <- k.FindClosestKNodes(nodereq.NodeID)
 
		case valuereq := <- k.FindValueChan:
			v,ok := k.DHT[valuereq.Key]
			valuereq.ValueChan <- ValueRpl{v, ok}
 
		case nodeID := <- k.FindContactChan:
			ContactAddress, err := k.FindContactHelper(nodeID);
			k.FindContactResChan <- FindContactRes{ContactAddress, err}
		case vdo := <- k.VDOStoreChan:
			id := <- k.VDOIDChan
			k.VDO[id] = vdo 
			k.VDOStoreEndChan <- id
		case req := <- k.VDOReqChan:
			flag := true
			for k1, v := range k.VDO{
				if k1 == req.VdoID{
					k.VDOChan <- GetVDOResultHelper{nil, v}
					flag = false
					break
				}
			}
			if flag{
				k.VDOChan <-  GetVDOResultHelper{ &CommandFailed{"NOT FIND"}, *new(VanashingDataObject)}
			}
		}
	}
}



type ContactNotFoundError struct {
	id  ID
	msg string
}

func (e *ContactNotFoundError) Error() string {
	return fmt.Sprintf("%x %s", e.id, e.msg)
}

func (k *Kademlia)FindContactHelper(nodeId ID)(*Contact, error) {
	if nodeId == k.SelfContact.NodeID {
		return &k.SelfContact, nil
	}
	distance := k.NodeID.Xor(nodeId)
	i := distance.PrefixLen()
	for _,contact := range k.buckets[i]{
		if contact.NodeID == nodeId{
			return &contact, nil
		}
	}
	return nil, &ContactNotFoundError{nodeId, "Not found"}
}

func (k *Kademlia) FindContact(nodeId ID) (*Contact, error) {
     k.FindContactChan <- nodeId;
     res := <- k.FindContactResChan;
     return res.ContactAddress, res.err;
}

type CommandFailed struct {
	msg string
}

func (e *CommandFailed) Error() string {
	return fmt.Sprintf("%s", e.msg)
}

func min(x, y int) int {
	if x<y {
		return x
	}
	return y
}

func SortKNodes(nodeID ID, nodes []Contact) (ret []Contact){
 
	for i:=0; i< min(len(nodes),K); i++ {
		for j:=len(nodes)-1; j>i; j-- {
 
			if nodeID.Xor(nodes[j].NodeID).Less(nodeID.Xor(nodes[j-1].NodeID)) {
				temp := nodes[j]
				nodes[j] = nodes[j-1]
				nodes[j-1] = temp
			}
		}
	}
	if len(nodes) > K {
		ret = nodes[:K]
	} else {
		ret = nodes
	}
	return
}

 
func (k *Kademlia) FindClosestKNodes(nodeID ID) (ret []Contact){

  var nodes []Contact
	for _, bucket := range k.buckets {
		if len(bucket) > 0 {
			nodes = append(nodes, bucket...)
		}
	}
	ret = SortKNodes(nodeID, nodes)
 
	return
}

func (k *Kademlia) DoPing(host net.IP, port uint16) (*Contact, error) {


    client, err := rpc.DialHTTPPath("tcp", host.String() + ":" + strconv.Itoa(int(port)),
		rpc.DefaultRPCPath+host.String()+strconv.Itoa(int(port)))
	defer func() {
		client.Close()
	}()
    if err != nil {
    	fmt.Println(err)
        return nil, &CommandFailed{"DOPING: Client setup failed!"}
    }

    ping := new(PingMessage)

    ping.Sender.NodeID = k.NodeID
    ping.Sender.Host = k.SelfContact.Host
    ping.Sender.Port = k.SelfContact.Port
    ping.MsgID = NewRandomID()
    var pong PongMessage
    err = client.Call("KademliaRPC.Ping", ping, &pong)



    if err != nil {
    	fmt.Println(err)
      return nil, &CommandFailed{"DOPING: Client call failed!"}
    }

    if pong.MsgID.Equals(ping.MsgID){
			k.Senders <- pong.Sender
			return &(pong.Sender), nil
    }
	return nil, &CommandFailed{
		"Unable to ping " + fmt.Sprintf("%s:%v", host.String(), port)}
}

func (k *Kademlia) DoStore(contact *Contact, key ID, value []byte) error {
 
	client, err := rpc.DialHTTPPath("tcp", contact.Host.String() + ":" + strconv.Itoa(int(contact.Port)),
				rpc.DefaultRPCPath+contact.Host.String()+strconv.Itoa(int(contact.Port)))
	if err != nil{
		log.Fatal("DialHTTP: ", err)
	}

  	defer func() {
  		client.Close()
	}()

  storeRequest := new (StoreRequest)
  storeResult := new (StoreResult)
  storeRequest.MsgID = NewRandomID()
  storeRequest.Sender = k.SelfContact
  storeRequest.Key = key
  storeRequest.Value = value

  err = client.Call("KademliaRPC.Store", storeRequest, &storeResult)
  if err != nil{
   log.Fatal("Call: ", err)
   return &CommandFailed{"Unable to store {"+key.AsString()+"}"}
  }

  if(storeResult.MsgID.Compare(storeRequest.MsgID) == 0){
  	k.Senders <- *contact
   	return nil
  }
	return &CommandFailed{"Not implemented"}
}

func (k *Kademlia) DoFindNode(contact *Contact, searchKey ID) ([]Contact, error) {
 
	hostname := contact.Host.String()
	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", hostname+":"+port_str,rpc.DefaultRPCPath+contact.Host.String()+port_str)
	if err != nil {
		fmt.Println(err)
		return nil, &CommandFailed{"DoFindNode failed!"}
	}

	msgId := NewRandomID()
	req := FindNodeRequest{k.SelfContact, msgId, searchKey}
	res := new(FindNodeResult)
	err = client.Call("KademliaRPC.FindNode", req, &res)

	defer func() {
		client.Close()
	}()

	if err != nil {
		return nil, &CommandFailed{"RPC Error"}
	}

	if !res.MsgID.Equals(msgId) {
		return nil, &CommandFailed{"MsgID Not Match"}
	}

	k.Senders <- *contact
	if res.Err != nil {
		return nil, &CommandFailed{"FindNodeError"}
	}
 
	k.NewContacts <- res.Nodes
	return res.Nodes, nil
}

func (k *Kademlia) DoFindValue(contact *Contact,
	searchKey ID) (value []byte, contacts []Contact, err error) {
 
 	client, err := rpc.DialHTTPPath("tcp", contact.Host.String() + ":" + strconv.Itoa(int(contact.Port)),
			rpc.DefaultRPCPath+contact.Host.String()+strconv.Itoa(int(contact.Port)))
	if err != nil {
		fmt.Println(err)
 
			return nil,nil, &CommandFailed{"DOFINDVALUE: Client setup failed!"}
	}
 
	defer func() {
		client.Close()
	}()

	fvr := new(FindValueRequest)

	fvr.Sender.NodeID = k.NodeID
	fvr.Sender.Host = k.SelfContact.Host
	fvr.Sender.Port = k.SelfContact.Port
	fvr.MsgID = NewRandomID()
	fvr.Key = searchKey

	var fvrl FindValueResult

	err = client.Call("KademliaRPC.FindValue", fvr, &fvrl)
	if err != nil {
		fmt.Println(err)
			return nil,nil, &CommandFailed{"DOFINDVALUE: Client call failed!"}
	}
 
	if fvrl.MsgID.Equals(fvr.MsgID){
 
		if (fvrl.Nodes!= nil) {
			k.NewContacts <- fvrl.Nodes
		}
		return fvrl.Value, fvrl.Nodes, nil
	}
	return nil, nil, &CommandFailed{"Not implemented"}
}

func (k *Kademlia) LocalFindValue(searchKey ID) ([]byte, error) {

	vrplChan := make(chan ValueRpl)
	k.FindValueChan <- FindValueReq{searchKey, vrplChan}
	vrpl := <- vrplChan
	if vrpl.ok {
		return vrpl.value, nil
	}

	return []byte(""), &CommandFailed{"Not implemented"}
}

// For project 2!
func Min(x int, y int) (int){
	if x<y {
		return x
	}
	return y
}

func (k *Kademlia) RPC_Search(contact Contact, searchKey ID, results chan rpcResult) {
 
	hostname := contact.Host.String()
	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", hostname+":"+port_str,rpc.DefaultRPCPath+contact.Host.String()+port_str)
	if err != nil {
		results <- rpcResult{contact, nil, &CommandFailed{"DialHTTPPath failed!"}}
		return
	}

	msgId := NewRandomID()
	req := FindNodeRequest{k.SelfContact, msgId, searchKey}
	res := new(FindNodeResult)

	defer func() {
		client.Close()
	}()

	callChan := make(chan bool)

	go func() {
		err = client.Call("KademliaRPC.FindNode", req, &res)
 
		if err == nil{
 
			callChan <- true
		}else{
 
			callChan <- false
		}
	}()


	select {
		case err := <- callChan:
			if (!err && msgId.Equals(res.MsgID)) {
				results <- rpcResult{contact, nil, &CommandFailed{"RPC Error"}}
				return
			}
			k.Senders <- contact
			if res.Err != nil {
				results <- rpcResult{contact, nil, &CommandFailed{"FindNodeError"}}
				return
			}
			k.NewContacts <- res.Nodes
 
			results <- rpcResult{contact, res.Nodes, nil}
			return
		case <-time.After(time.Second * 1):
			results <- rpcResult{contact, nil, &CommandFailed{"Timeout"}}
			return
	}
}

func (k *Kademlia) RPC_SearchValue(contact Contact, searchKey ID, results chan rpcResultValue) {
 

	hostname := contact.Host.String()
	port_str := strconv.Itoa(int(contact.Port))
 
	client, err := rpc.DialHTTPPath("tcp", hostname+":"+port_str,rpc.DefaultRPCPath+contact.Host.String()+port_str)

	if err != nil {
		fmt.Println("ERROR: ",err)
		results <- rpcResultValue{contact, nil, nil, &CommandFailed{"DialHTTPPath failed!"}}
		return
	}
	defer func() {
		client.Close()
	}()

    msgId := NewRandomID()
	req := FindValueRequest{k.SelfContact, msgId, searchKey}
	res := new(FindValueResult)


	callChan := make(chan bool)

	go func() {
		err = client.Call("KademliaRPC.FindValue", req, &res)
 
		if err == nil{
 
			callChan <- true
		}else{
 
			callChan <- false
		}
	}()


  	select {
		case err := <- callChan:
			if (!err && msgId.Equals(res.MsgID)) {
				results <- rpcResultValue{contact, nil, nil, &CommandFailed{"RPC Error"}}
				return
			}
			k.Senders <- contact
			if res.Err != nil {
				results <- rpcResultValue{contact, nil, nil, &CommandFailed{"FindNodeError"}}
				return
			}
			if res.Nodes != nil {
				k.NewContacts <- res.Nodes
			}
			results <- rpcResultValue{contact, res.Value, res.Nodes, nil}
			return
		case <-time.After(time.Second * 1):
			results <- rpcResultValue{contact, nil, nil, &CommandFailed{"Timeout"}}
			return
    }

}

func clearList(ls []Contact) {
	i:=0
	for i<len(ls)-1{
		if ls[i].NodeID.Equals(ls[i+1].NodeID) {
			ls = append(ls[:i],ls[i+1:]...)
		} else {
			i++
		}
	}
	return
}

func mergeSort(ls1 []Contact, ls2 []Contact, id ID) (ret []Contact){
	clearList(ls1)
	clearList(ls2)
	i := 0
	j := 0
	for len(ret)<K && i<len(ls1) && j<len(ls2){
		if ls1[i].NodeID.Equals(ls2[j].NodeID) {
			i+=1
			continue
		}
		if ls1[i].NodeID.Xor(id).Less(ls2[j].NodeID.Xor(id)) {
			ret = append(ret,ls1[i])
			i+=1
		} else {  
			ret = append(ret,ls2[j])
			j+=1
		}  
 
	}
	if len(ret)==K {
		return
	} else if i<len(ls1) {
		ret = append(ret, ls1[i:Min(len(ls1),i+K-len(ret))]...)
	} else {
		ret = append(ret, ls2[j:Min(len(ls2),j+K-len(ret))]...)
	}
	return
}

func contains (contact Contact, shortList []Contact) (bool){
	for i:=0;i<len(shortList);i++ {
		if contact.NodeID.Equals(shortList[i].NodeID) {
			return true
		}
	}
	return false
}

func removeDuplicates (tList []Contact, shortList []Contact) (ret []Contact){
	for i:=0;i<len(tList);i++{
		if !contains(tList[i], shortList) {
			ret = append(ret, tList[i])
		}
	}
	return
}

func differ (ls1 []Contact, ls2 []Contact) (bool) {
	if len(ls1) != len(ls2) {
		return true
	}
	for i:=0;i<len(ls1);i++ {
		if !ls1[i].NodeID.Equals(ls2[i].NodeID) {
			return true
		}
	}
	return false
}

func PrintList(ls []Contact){
	for i:=0;i<len(ls);i++ {
		fmt.Println(ls[i].NodeID)
	}
}

func (k *Kademlia) DoIterativeFindNode(id ID) ([]Contact, error) {
	var shortList []Contact
	var closestNode Contact
	stopIter := false
	nodesChan := make(chan []Contact)
	k.FindNodeChan <- FindNodeReq{id, nodesChan}
	tempList := <-nodesChan
	results := make(chan rpcResult, alpha)

	if len(tempList)>0 {
		closestNode = tempList[0]
	}
	for len(shortList)<K && !stopIter {
 
		stopIter = true
		minLen := Min(alpha, len(tempList))
 
		for i:=0;i<minLen;i++ {
 
			go k.RPC_Search(tempList[i], id, results)
		}
		beforeList := tempList
		for i:=0;i<minLen;i++ {
			result := <- results
			if(result.err == nil) {
				shortList = append(shortList, result.contact)
 
				for j:=0;j<len(tempList);j++ {
					if tempList[j].NodeID.Equals(result.contact.NodeID) {
						tempList = append(tempList[:j], tempList[j+1:]...)
						break
					}
				}
				ret := removeDuplicates(result.Nodes, shortList)
 
				tempList = mergeSort(tempList, ret, id)
 
					if len(tempList)>0 && tempList[0].NodeID.Xor(id).Less(closestNode.NodeID.Xor(id)) {
						closestNode = tempList[0]
					}
 
			} else {  
				fmt.Println("timeout")
				for j:=0;j<minLen && j< len(tempList);j++ {
					fmt.Println("&&&&&&&&&&&&",minLen > len(tempList))
					if tempList[j].NodeID.Equals(result.contact.NodeID) {
						tempList = append(tempList[:j], tempList[j+1:]...)
					}
				}
			}
		}
 
		stopIter = !differ(beforeList, tempList)
	}
 
	return SortKNodes(id, shortList), nil
 
}

func (k *Kademlia) DoIterativeStore(key ID, value []byte) ([]Contact, error) {
	
	var ret []Contact
	contacts, err := k.DoIterativeFindNode(key)
	if err != nil {
		return nil, &CommandFailed{"Not implemented"}
	}
	for i:=0;i<len(contacts);i++ {
		err = k.DoStore(&contacts[i], key, value)
		if err == nil {
			ret = append(ret, contacts[i])
		}else{
			return nil, &CommandFailed{"???"}
		}
	}
	return ret, nil
}

func (k *Kademlia) DoIterativeFindValue(key ID) (value []byte, err error) {
	
	var shortList []Contact
	var closestNode Contact
	stopIter := false
	valueChan := make(chan ValueRpl)
	k.FindValueChan <- FindValueReq{key, valueChan}
	vrpl := <- valueChan
	if vrpl.ok {

		return vrpl.value, nil
	}

	nodesChan := make(chan []Contact)
	k.FindNodeChan <- FindNodeReq{key, nodesChan}
	tempList := <-nodesChan
	results := make(chan rpcResultValue, 1000)

	if len(tempList)>0 {
		closestNode = tempList[0]
	}
 
	for len(shortList)<K && !stopIter {
 
		stopIter = true

		minLen := Min(alpha, len(tempList))
 
		for i:=0;i<minLen;i++ {
			go k.RPC_SearchValue(tempList[i], key, results)
		}
		beforeList := tempList
		for i:=0;i<minLen;i++ {
			result := <- results
			if(result.err == nil) {
				if result.value != nil {
					err := k.DoStore(&closestNode, key, result.value)
					return result.value, err
				}
				shortList = append(shortList, result.contact)
	
				for j:=0;j<len(tempList);j++ {
					
					if tempList[j].NodeID.Equals(result.contact.NodeID) {
						tempList = append(tempList[:j], tempList[j+1:]...)
						break
					}
				}
				ret := removeDuplicates(result.Nodes, shortList)
				tempList = mergeSort(tempList, ret, key)

				if len(tempList)>0 && tempList[0].NodeID.Xor(key).Less(closestNode.NodeID.Xor(key)) {
					closestNode = tempList[0]
				}

			} else {  

				fmt.Println("timeout")
				for j:=0;j<minLen;j++ {
					if j < len(tempList) && tempList[j].NodeID.Equals(result.contact.NodeID) {
						if j+1 == len(tempList){
							break
						}
						tempList = append(tempList[:j], tempList[j+1:]...)
					}
				}
			}
		}
		stopIter = !differ(beforeList, tempList)
	}
	return nil, &CommandFailed{"Value Not Found, closestNode:"+closestNode.NodeID.AsString()}

 
}

// For project 3!
func (k *Kademlia) Vanish(vdoID ID, data []byte, numberKeys byte,
	threshold byte, timeoutSeconds int) (vdo VanashingDataObject) {
	fmt.Println("the command is :" ,vdoID, data, numberKeys, threshold, timeoutSeconds)
	vdo = k.VanishData( data, numberKeys, threshold, timeoutSeconds)
	k.VDOStoreChan <- vdo
	k.VDOIDChan <- vdoID
	_ = <- k.VDOStoreEndChan
	return
}

func (k *Kademlia) Unvanish(nodeID ID, searchKey ID) (data []byte) {

	k.FindContactChan <- nodeID
	s := <-k.FindContactResChan 
	if s.err == nil{
		contact := *s.ContactAddress
		hostname := contact.Host.String()
		port_str := strconv.Itoa(int(contact.Port))
		client, err := rpc.DialHTTPPath("tcp", hostname+":"+port_str,rpc.DefaultRPCPath+contact.Host.String()+port_str)
		if err != nil {
			fmt.Println(err)
		}
		msgId := NewRandomID()
		req := GetVDORequest{contact, searchKey, msgId}
		res := new(GetVDOResult)
		client.Call("KademliaRPC.GetVDO", req, &res)
 
		if(res.MsgID.Equals(msgId)){
 
			data = k.UnvanishData(res.VDO)
			return
		}
	}

	var contacts []Contact
	contacts, _ = k.DoIterativeFindNode(nodeID)

	for _, contact := range contacts {
		if contact.NodeID != nodeID{
			break
		}
		hostname := contact.Host.String()
		port_str := strconv.Itoa(int(contact.Port))
		client, err := rpc.DialHTTPPath("tcp", hostname+":"+port_str,rpc.DefaultRPCPath+contact.Host.String()+port_str)
		if err != nil {
			fmt.Println(err)
		}
		msgId := NewRandomID()
		req := GetVDORequest{contact, searchKey, msgId}
		res := new(GetVDOResult)
		client.Call("KademliaRPC.GetVDO", req, &res)
 
		if(res.MsgID.Equals(msgId)){
 
			data = k.UnvanishData(res.VDO)
			return
		}
	}
	
	return 
}
