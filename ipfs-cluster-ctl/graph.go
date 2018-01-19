package main

import (
	"errors"
	"fmt"
	"io"
	"sort"

	peer "github.com/libp2p/go-libp2p-peer"

	"github.com/ipfs/ipfs-cluster/api"
)

/*
   These functions are used to write an IPFS Cluster connectivity graph to a
   graphviz-style dot file.  Input an api.ConnectGraphSerial object, makeDot
   does some preprocessing and then passes all 3 link maps to a
   cluster-dotWriter which handles iterating over the link maps and writing
   dot file node and edge statements to make a dot-file graph.  Nodes are
   labeled with the go-libp2p-peer shortened peer id.  IPFS nodes are rendered
   with gold boundaries, Cluster nodes with blue.  Currently preprocessing
   consists of moving IPFS swarm peers not connected to any cluster peer to
   the IPFSLinks map in the event that the function was invoked with the
   allIpfs flag.  This allows all IPFS peers connected to the cluster to be
   rendered as nodes in the final graph.
*/

// nodeType specifies the type of node being represented in the dot file:
// either IPFS or Cluster
type nodeType int

const (
	tCluster nodeType = iota // The cluster node type
	tIpfs                    // The IPFS node type
)

var errUnfinishedWrite = errors.New("could not complete write of line to output")
var errUnknownNodeType = errors.New("unsupported node type. Expected cluster or ipfs")
var errCorruptOrdering = errors.New("expected pid to have an ordering within dot writer")

func makeDot(cg api.ConnectGraphSerial, w io.Writer, allIpfs bool) error {
	ipfsEdges := make(map[string][]string)
	for k, v := range cg.IPFSLinks {
		ipfsEdges[k] = make([]string, 0)
		for _, id := range v {
			if _, ok := cg.IPFSLinks[id]; ok || allIpfs {
				ipfsEdges[k] = append(ipfsEdges[k], id)
			}
			if allIpfs { // include all swarm peers in the graph
				if _, ok := ipfsEdges[id]; !ok {
					// if id in IPFSLinks this will be overwritten
					// if id not in IPFSLinks this will stay blank
					ipfsEdges[id] = make([]string, 0)
				}
			}
		}
	}

	dW := dotWriter{
		w:                w,
		ipfsEdges:        ipfsEdges,
		clusterEdges:     cg.ClusterLinks,
		clusterIpfsEdges: cg.ClustertoIPFS,
		clusterOrder:     make(map[string]int, 0),
		ipfsOrder:        make(map[string]int, 0),
	}
	return dW.print()
}

type dotWriter struct {
	clusterOrder map[string]int
	ipfsOrder    map[string]int

	w io.Writer

	ipfsEdges        map[string][]string
	clusterEdges     map[string][]string
	clusterIpfsEdges map[string]string
}

func (dW dotWriter) writeComment(comment string) error {
	final := fmt.Sprintf("/* %s */\n", comment)
	n, err := io.WriteString(dW.w, final)
	if err == nil && n != len([]byte(final)) {
		err = errUnfinishedWrite
	}
	return err
}

// precondition: id has already been processed and id's ordering
// has been recorded by dW
func (dW dotWriter) getString(id string, idT nodeType) (string, error) {
	switch idT {
	case tCluster:
		number, ok := dW.clusterOrder[id]
		if !ok {
			return "", errCorruptOrdering
		}
		return fmt.Sprintf("C%d", number), nil

	case tIpfs:
		number, ok := dW.ipfsOrder[id]
		if !ok {
			return "", errCorruptOrdering
		}
		return fmt.Sprintf("I%d", number), nil
	default:
		return "", errUnknownNodeType
	}
}

func (dW dotWriter) writeEdge(from string, fT nodeType, to string, tT nodeType) error {
	fromStr, err := dW.getString(from, fT)
	if err != nil {
		return err
	}
	toStr, err := dW.getString(to, tT)
	if err != nil {
		return err
	}
	edgeStr := fmt.Sprintf("%s -> %s\n", fromStr, toStr)
	n, err := io.WriteString(dW.w, edgeStr)
	if err == nil && n != len([]byte(edgeStr)) {
		err = errUnfinishedWrite
	}
	return err
}

// writes nodes to dot file output and creates and stores an ordering over nodes
func (dW *dotWriter) writeNode(id string, nT nodeType) error {
	var nodeStr string
	switch nT {
	case tCluster:
		nodeStr = fmt.Sprintf("C%d [label=\"%s\" color=\"blue2\"]\n", len(dW.clusterOrder), shortID(id))
		dW.clusterOrder[id] = len(dW.clusterOrder)
	case tIpfs:
		nodeStr = fmt.Sprintf("I%d [label=\"%s\" color=\"goldenrod\"]\n", len(dW.ipfsOrder), shortID(id))
		dW.ipfsOrder[id] = len(dW.ipfsOrder)
	default:
		return errUnknownNodeType
	}
	n, err := io.WriteString(dW.w, nodeStr)
	if err == nil && n != len([]byte(nodeStr)) {
		err = errUnfinishedWrite
	}
	return err
}

func (dW *dotWriter) print() error {
	_, err := io.WriteString(dW.w, "digraph cluster {\n\n")
	err = dW.writeComment("The nodes of the connectivity graph")
	if err != nil {
		return err
	}
	err = dW.writeComment("The cluster-service peers")
	if err != nil {
		return err
	}
	// Write cluster nodes, use sorted order for consistent labels
	for _, k := range sortedKeys(dW.clusterEdges) {
		err = dW.writeNode(k, tCluster)
		if err != nil {
			return err
		}
	}
	_, err = io.WriteString(dW.w, "\n")
	if err != nil {
		return err
	}

	err = dW.writeComment("The ipfs peers")
	if err != nil {
		return err
	}
	// Write ipfs nodes, use sorted order for consistent labels
	for _, k := range sortedKeys(dW.ipfsEdges) {
		err = dW.writeNode(k, tIpfs)
		if err != nil {
			return err
		}
	}
	_, err = io.WriteString(dW.w, "\n")
	if err != nil {
		return err
	}

	err = dW.writeComment("Edges representing active connections in the cluster")
	if err != nil {
		return err
	}
	err = dW.writeComment("The connections among cluster-service peers")
	// Write cluster edges
	for k, v := range dW.clusterEdges {
		for _, id := range v {
			err = dW.writeEdge(k, tCluster, id, tCluster)
			if err != nil {
				return err
			}
		}
	}
	_, err = io.WriteString(dW.w, "\n")
	if err != nil {
		return err
	}

	err = dW.writeComment("The connections between cluster peers and their ipfs daemons")
	if err != nil {
		return err
	}
	// Write cluster to ipfs edges
	for k, id := range dW.clusterIpfsEdges {
		err = dW.writeEdge(k, tCluster, id, tIpfs)
		if err != nil {
			return err
		}
	}
	_, err = io.WriteString(dW.w, "\n")
	if err != nil {
		return err
	}

	err = dW.writeComment("The swarm peer connections among ipfs daemons in the cluster")
	if err != nil {
		return err
	}
	// Write ipfs edges
	for k, v := range dW.ipfsEdges {
		for _, id := range v {
			err = dW.writeEdge(k, tIpfs, id, tIpfs)
			if err != nil {
				return err
			}
		}
	}
	_, err = io.WriteString(dW.w, "\n }")
	if err != nil {
		return err
	}

	return nil
}

func sortedKeys(dict map[string][]string) []string {
	keys := make([]string, len(dict), len(dict))
	i := 0
	for k := range dict {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

// truncate the provided peer id string to the 3 last characters.  Odds of
// pairwise collisions are less than 1 in 200,000 so with 70 cluster peers
// the chances of a collision are still less than 1 in 100 (birthday paradox).
// As clusters grow bigger than this we can provide a flag for including
// more characters.
func shortID(peerString string) string {
	pid, err := peer.IDB58Decode(peerString)
	if err != nil {
		// We'll truncate ourselves
		// Should never get here but try to match with peer:String()
		if len(peerString) < 6 {

			return fmt.Sprintf("<peer.ID %s>", peerString)
		}
		return fmt.Sprintf("<peer.ID %s>", peerString[:6])
	}
	return pid.String()
}
