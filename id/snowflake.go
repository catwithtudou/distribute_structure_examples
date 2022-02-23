package id

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

/**
 *@Author tudou
 *@Date 2020/12/30
 **/


var sequence int = 0
var lastTime int = -1

var workerIdBits = 5
var datacenterIdBits = 5
var sequenceBits = 12


var maxWorkerId int = -1 ^ (-1 << workerIdBits) //31
var maxDatacenterId int = - 1 ^ (-1<<datacenterIdBits) //31
var maxSequence int = -1 ^ (-1<<sequenceBits) //4095

var workerIdShift = sequenceBits
var datacenterShift = workerIdBits + sequenceBits
var timestampShift = datacenterIdBits + workerIdBits + sequenceBits

type Snowflake struct{
	datacenterId int
	workerId int
	epoch int
	mu *sync.Mutex
}


func NewSnowflake(datacenterId int, workerId int, epoch int) (*Snowflake, error) {
	if datacenterId > maxDatacenterId || datacenterId < 0 {
		return nil, errors.New(fmt.Sprintf("datacenterId cant be greater than %d or less than 0", maxDatacenterId))
	}
	if workerId > maxWorkerId || workerId < 0 {
		return nil, errors.New(fmt.Sprintf("workerId cant be greater than %d or less than 0", maxWorkerId))
	}
	if epoch > getCurrentTime() {
		return nil, errors.New(fmt.Sprintf("epoch time cant be after now"))
	}
	sf := Snowflake{datacenterId, workerId, epoch, new(sync.Mutex)}
	return &sf, nil
}


func getCurrentTime() int {
	return int(time.Now().UnixNano() / 1e6) //micro second
}

func (s *Snowflake)getId()int{
	s.mu.Lock()
	defer s.mu.Unlock()
	currentTime := getCurrentTime()
	if currentTime < lastTime{
		currentTime = waitUntilNextTime(lastTime)
		sequence = 0
	}else if currentTime == lastTime{
		sequence = (sequence + 1) & maxSequence
		if sequence == 0{
			currentTime = waitUntilNextTime(lastTime)
		}
	}else if currentTime > lastTime{
		sequence = 0
		lastTime = currentTime
	}

	return (currentTime-s.epoch)<<timestampShift | s.datacenterId<<datacenterShift |
		s.workerId<<workerIdShift | sequence
}

func waitUntilNextTime(lastTime int) int {
	currentTime := getCurrentTime()
	for currentTime <= lastTime {
		time.Sleep(1 * time.Second / 1000)
		currentTime = getCurrentTime()
	}
	return currentTime
}