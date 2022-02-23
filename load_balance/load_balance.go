package load_balance

import (
	"math/rand"
	"strconv"
	"strings"
)

type LoadBalance interface {
	Add(string)
	Get()string
}

type RandomLb struct{
	curIdx int
	nodes []string
}

func (r *RandomLb)Add(param string){
	r.nodes = append(r.nodes,param)
}

func (r *RandomLb)Get()string{
	r.curIdx = rand.Intn(len(r.nodes))
	return r.nodes[r.curIdx]
}

type RoundRobinLb struct {
	curIdx int
	nodes []string
}

func (r *RoundRobinLb) Add(params string) {
	r.nodes = append(r.nodes, params)
}

//get node
func (r *RoundRobinLb) Get() string{
	lens := len(r.nodes)
	if r.curIdx >= lens {
		r.curIdx = 0
	}
	curNode := r.nodes[r.curIdx]
	r.curIdx = (r.curIdx + 1) % lens
	return curNode
}


type WeightRoundRobinLb struct {
	curIdx   int
	nodes []*WeightNode
}


type WeightNode struct {
	node          string
	weight        int
	currentWeight int
}

//add node
func (r *WeightRoundRobinLb) Add(param string) {
	params:=strings.Split(param,",")
	parInt, _ := strconv.ParseInt(params[1], 10, 64)
	node := &WeightNode{node: params[0], weight: int(parInt)}
	r.nodes = append(r.nodes, node)
}

//get node
func (r *WeightRoundRobinLb) Get()string{
	totalWeight := 0
	var bestNode *WeightNode
	for i := 0; i < len(r.nodes); i++ {
		curNode := r.nodes[i]
		totalWeight += curNode.weight
		curNode.currentWeight += curNode.weight

		//choose the largest weight
		if bestNode == nil || curNode.currentWeight > bestNode.currentWeight {
			bestNode = curNode
		}
	}
	if bestNode == nil {
		return ""
	}
	bestNode.currentWeight -= totalWeight
	return bestNode.node
}