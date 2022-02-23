package rate_limit

import (
	"github.com/andres-erbsen/clock"
	"time"
)

type Limiter interface {
	Take() time.Time
}

type Clock interface {
	Now() time.Time
	Sleep(duration time.Duration)
}

type config struct{
	clock Clock //间隔时间段
	maxSlack time.Duration
	per time.Duration //时间滑动窗口区间
}

func New(rate int,opts ...Option)Limiter{
	return newAtomicBased(rate,opts...)
}



func buildConfig(opts []Option)config{
	c:=config{
		clock:    clock.New(),
		maxSlack: 10,
		per:      time.Second,
	}
	for _,opt:=range opts{
		opt.apply(&c)
	}
	return c
}

type Option interface {
	apply(*config)
}

type clockOption struct{
	clock Clock
}

func (o clockOption)apply(c *config){
	c.clock = o.clock
}

func WithClock(clock Clock)Option{
	return clockOption{clock: clock}
}

type slackOption struct{
	slack time.Duration
}

func (o slackOption)apply(c *config){
	c.maxSlack = o.slack
}

func WithSlack(slack time.Duration)Option{
	return slackOption{slack: slack}
}

type perOption time.Duration

func (p perOption)apply(c *config){
	c.per = time.Duration(p)
}

func WithPer(per time.Duration)Option{
	return perOption(per)
}

