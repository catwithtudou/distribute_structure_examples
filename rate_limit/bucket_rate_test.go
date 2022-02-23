package rate_limit

import (
	"fmt"
	"testing"
	"time"
)

func TestBucketRate(t *testing.T){

	rl:=New(1)
	prev:=time.Now()
	for i:=0;i<10;i++{
			now:=rl.Take()
			fmt.Println(i,now.Sub(prev))
			prev=now
	}
}
