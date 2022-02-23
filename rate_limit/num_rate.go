package rate_limit

import (
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"time"
)

var client *redis.Client

func init(){
	client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
	})
	_,err:=client.Ping().Result()
	if err!=nil{
		panic(err)
	}
}


// 通过redis计数进行计时器限流
func NumRateLimit(key string,fillInterval  time.Duration, segmentNum,limitNum int64)bool{
	segmentInterval := fillInterval.Seconds() / float64(segmentNum)
	tick:=float64(time.Now().Unix()) / segmentInterval 	// 按时间段进行切割即时间段内tick值相同
	curKey:= fmt.Sprintf("%s_%d_%d_%d_%f",key,fillInterval,segmentNum,limitNum,tick)

	sCount:=0
	_,err:=client.SetNX(curKey,sCount,fillInterval).Result()
	if err!=nil{
		panic(err)
	}
	tickNum,err:=client.Incr(curKey).Result()
	if err!=nil{
		panic(err)
	}
	for tStart:=segmentInterval;tStart<fillInterval.Seconds();tStart+=segmentInterval{
		tick = tick-1
		preKey:=fmt.Sprintf("%s_%d_%d_%d_%f",key,fillInterval,segmentNum,limitNum,tick)
		val,err:=client.Get(preKey).Result()
		if err!=nil{
			val="0" //若值为空则默认为0
		}
		num,err:=strconv.ParseInt(val,0,64)
		tickNum+= num
		if tickNum>limitNum{
			client.Decr(curKey).Result()
			return false
		}
	}
	return true
}