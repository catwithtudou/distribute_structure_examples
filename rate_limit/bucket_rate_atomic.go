package rate_limit

import (
	"log"
	"sync/atomic"
	"time"
	"unsafe"
)

type state struct{
	last time.Time
	sleepFor time.Duration
}

type atomicLimiter struct{
	state unsafe.Pointer
	padding [56]byte // 本地内存
	perRequest time.Duration
	maxSlack time.Duration
	clock Clock
}

func newAtomicBased(rate int,opts ...Option)*atomicLimiter{
	config:=buildConfig(opts)
	l:=&atomicLimiter{
		perRequest: config.per / time.Duration(rate),
		maxSlack:   -1 * config.maxSlack * time.Second / time.Duration(rate),
		clock:      config.clock,
	}
	initState:=state{
		last:     time.Time{},
		sleepFor: 0,
	}
	atomic.StorePointer(&l.state,unsafe.Pointer(&initState))
	return l
}

func (t *atomicLimiter)Take()time.Time{
	var(
		newState state
		taken bool
		interval time.Duration
	)

	for !taken{
		now := t.clock.Now()

		previousStatePointer:=atomic.LoadPointer(&t.state) //原子性获取状态
		oldState:=(*state)(previousStatePointer)

		newState = state{
			last:     now,
			sleepFor: oldState.sleepFor,
		}


		// 若为第一次请求则允许
		if oldState.last.IsZero(){
			// 乐观锁进行交换
			taken = atomic.CompareAndSwapPointer(&t.state,previousStatePointer,unsafe.Pointer(&newState))
			continue
		}

		// 在该时间段内获取需要等待的时间
		newState.sleepFor += t.perRequest - now.Sub(oldState.last)
		log.Println("[sleepFor:",newState.sleepFor,"]")

		if newState.sleepFor < t.maxSlack{
			newState.sleepFor = t.maxSlack
		}

		if newState.sleepFor > 0{
			newState.last = newState.last.Add(newState.sleepFor)
			interval,newState.sleepFor = newState.sleepFor,0
		}
		taken = atomic.CompareAndSwapPointer(&t.state,previousStatePointer,unsafe.Pointer(&newState))
	}
	log.Println("[interval:",interval,"]")
	t.clock.Sleep(interval)
	return newState.last
}