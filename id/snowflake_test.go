package id

import (
	"log"
	"sync"
	"testing"
)

/**
 *@Author tudou
 *@Date 2020/12/30
 **/


func TestSnowFlake(t *testing.T){
	s,err:=NewSnowflake(0,0,1596850974656)
	if err!=nil{
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i:=0;i<1000;i++{
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Println(s.getId())
		}()
	}

	wg.Wait()
}