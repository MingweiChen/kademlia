package libkademlia

import (
	"bytes"
	"net"
	"strconv"
	"testing"
	// "time"
	"fmt"
)

func StringToIpPort(laddr string) (ip net.IP, port uint16, err error) {
	hostString, portString, err := net.SplitHostPort(laddr)
	if err != nil {
		return
	}
	ipStr, err := net.LookupHost(hostString)
	if err != nil {
		return
	}
	for i := 0; i < len(ipStr); i++ {
		ip = net.ParseIP(ipStr[i])
		if ip.To4() != nil {
			break
		}
	}
	portInt, err := strconv.Atoi(portString)
	port = uint16(portInt)
	return
}

// extra credit
func Test1(t *testing.T) {
	//tree structure
	//node1 --- treenode[50]

	instance1 := NewKademlia("localhost:4000")
	treenode := make([]*Kademlia, 50)
	//ping
	for i := 0; i < 50; i++ {
		address := "localhost:" + strconv.Itoa(4001+i)
		treenode[i] = NewKademlia(address)
		host1, port1, _ := StringToIpPort(address)
		instance1.DoPing(host1, port1)
	}
	//vanish by instance1
	nodeid := instance1.SelfContact.NodeID
	// fmt.Println(nodeid)
	VdoID := NewRandomID()
	instance1.Vanish(VdoID, []byte("abcdef"), 8, 4, 3000)
	origin_data := treenode[2].Unvanish(nodeid, VdoID)
 	fmt.Println(origin_data)
	if !bytes.Equal(origin_data, []byte("abcdef")) {
		t.Error("do not pair")
	}
	return
}

// extra credit
func Test2(t *testing.T) {
	//tree structure
	//node1 --- treenode[20]
	//vanish by node1, node1.Unvanish

	instance1 := NewKademlia("localhost:5000")
	treenode := make([]*Kademlia, 20)

	for i := 0; i < 20; i++ {
		address := "localhost:" + strconv.Itoa(5001+i)
		treenode[i] = NewKademlia(address)
		host1, port1, _ := StringToIpPort(address)
		instance1.DoPing(host1, port1)
	}

	nodeid := instance1.SelfContact.NodeID
	fmt.Println(nodeid)
	VdoID := NewRandomID()
	instance1.Vanish(VdoID, []byte("abcdef"), 20, 15, 3000000000000)
	origin_data := instance1.Unvanish(nodeid, VdoID)
	//fmt.Println(string(ciphertext) + "is result")
	if !bytes.Equal(origin_data, []byte("abcdef")) {
		t.Error("no not pair")
	}
	//t.Error("Finish")
	return
}

// extra credit
func Test3(t *testing.T) {
	//tree structure
	//node1 --- treenode[40] --- treenode1[30]
	//vanish by node1, treenode1[5] Unvanish

	instance1 := NewKademlia("localhost:6080")
	treenode := make([]*Kademlia, 50)
	treenode1 := make([]*Kademlia, 30)

	for i := 0; i < 50; i++ {
		address := "localhost:" + strconv.Itoa(7581+i)
		treenode[i] = NewKademlia(address)
		host1, port1, _ := StringToIpPort(address)
		instance1.DoPing(host1, port1)
	}

	for j := 0; j < 30; j++ {
		address := "localhost:" + strconv.Itoa(7881+j)
		treenode1[j] = NewKademlia(address)
		host2, port2, _ := StringToIpPort(address)
		treenode[2].DoPing(host2, port2)
		treenode[5].DoPing(host2, port2)
	}

	nodeid := instance1.SelfContact.NodeID
	fmt.Println("NodeID")
	fmt.Println(instance1.SelfContact.NodeID)
	VdoID := NewRandomID()
	//fmt.Println("test2 ciphertext")
	instance1.Vanish(VdoID, []byte("abcdef"), 50, 45, 3000000000000)
	fmt.Println("test2 ciphertext")
	origin_data := treenode1[5].Unvanish(nodeid, VdoID)
	fmt.Println("test2 original")
	//	fmt.Println(string(ciphertext) + "is result")
	if !bytes.Equal(origin_data, []byte("abcdef")) {
		t.Error("do not pair")
	}
	//t.Error("Finish")
	return
}