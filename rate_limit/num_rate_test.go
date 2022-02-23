package rate_limit

import (
	"fmt"
	"testing"
	"time"
)

func TestNumRateLimit(t *testing.T) {
	fillInterval:=30*time.Second
	segment:=int64(6)
	limitNum:=int64(4)
	waitTime:=5
	fmt.Printf("[time]0:%d\n",waitTime)
	time.Sleep(time.Duration(waitTime)*time.Second)
	for i:=0;i<10;i++{
		fmt.Printf("[time]%d:%d\n",i*5+waitTime,(i+1)*5+waitTime)
		rs:=NumRateLimit("test",fillInterval,segment,limitNum)
		fmt.Println("result:",rs)
		time.Sleep(5 * time.Second)
	}
}
