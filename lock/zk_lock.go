package lock

import (
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"time"
)

/**
 *@Author tudou
 *@Date 2020/12/30
 **/



var conn *zk.Conn

func NewZkClient(){
	var err error
	conn,_,err=zk.Connect([]string{"localhost:2181"},time.Second)
	if err!=nil{
		panic(err)
	}
}

func ZkClose(){
	conn.Close()
}



func zkLock(handleFunc func()){
	defer wg.Done()
	lock := zk.NewLock(conn,"/lock",zk.WorldACL(zk.PermAll))
	err:=lock.Lock()
	if err!=nil{
		log.Println("failed to lock")
		panic(err)
		return
	}
	log.Println("Successfully lock")

	handleFunc()

	err = lock.Unlock()
	if err!=nil{
		log.Println("failed to unlock")
		return
	}
	log.Println("Successfully unlock")
}

