package lock

/**
 *@Author tudou
 *@Date 2020/12/30
 **/


import (
	"github.com/go-redis/redis"
	"log"
	"os/exec"
	"sync"
	"time"
)



var redisClient *redis.Client

func NewRedisClient(){
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	})
}

var counter int64
var wg sync.WaitGroup
var lockKey = "lock"

func incr(){
	counter++
	log.Printf("incring is %d\n",counter)
}

func lock(handleFunc func()){
	defer wg.Done()

	uuid := getUuid()

	lockFlag,err:=redisClient.SetNX(lockKey,uuid,5*time.Second).Result()
	if err!=nil || !lockFlag{
		log.Println("failed to lock")
		return
	}else{
		log.Println("successfully lock")
	}

	handleFunc()

	script:=`
	if redis.call("get", KEYS[1]) == ARGV[1]
		then
				return redis.call("del",KEYS[1])
		else
				return 0
		end
	`

	var luaScript = redis.NewScript(script)
	result,_:=luaScript.Run(redisClient,[]string{lockKey},uuid).Result()
	if result == 0{
		log.Println("failed to unlock")
	}else{
		log.Println("successfully unlock")
	}
}

func getUuid() string {
	out, err := exec.Command("uuidgen").Output()
	if err != nil {
		panic(err)
	}
	return string(out)
}
