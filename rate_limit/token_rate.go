package rate_limit

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

var (
	numKey = "num"
	lastTimeKey = "lastTime"
)

func tokenRateLimit(key string,fillInterval time.Duration,limitNum,capacity int64)bool{
	curKey:=fmt.Sprintf("%s_%d_%d_%d",key,fillInterval,limitNum,capacity)
	curTime:=time.Now().Unix()
	client.HSetNX(curKey,numKey,capacity).Result()
	client.HSetNX(curKey,lastTimeKey,curTime).Result()

	result,_:=client.HMGet(curKey,numKey,lastTimeKey).Result()
	lastNum,_:=strconv.ParseInt(result[0].(string),0,64)
	lastTime,_:=strconv.ParseInt(result[1].(string),0,64)
	rate:=float64(limitNum)/float64(fillInterval.Seconds())
	incrNum:=int64(math.Ceil(float64(curTime-lastTime)*rate))
	curNum:=lastNum + incrNum
	if capacity<curTime{
		curTime = capacity
	}

	if curNum>0{
		var fields = map[string]interface{}{lastTimeKey:curTime,numKey:curNum-1}
		client.HMSet(curKey,fields)
		return true
	}
	return false
}