package main

import (
	"distributed-kv-benchmark/client"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

type statistic struct {
	count int
	time  time.Duration
}

type result struct {
	getCount    int
	missCount   int
	setCount    int
	statBuckets []statistic
}

func (r *result) addStatistic(bucket int, stat statistic) {
	if bucket > len(r.statBuckets)-1 {
		newStatBuckets := make([]statistic, bucket+1)
		copy(newStatBuckets, r.statBuckets)
		r.statBuckets = newStatBuckets
	}
	r.statBuckets[bucket].count = stat.count
	r.statBuckets[bucket].time = stat.time
}

func (r *result) addDuration(d time.Duration, typ string) {
	bucket := int(d / time.Millisecond)
	r.addStatistic(bucket, statistic{1, d})
	switch typ {
	case client.GET:
		r.getCount++
	case client.SET:
		r.setCount++
	default:
		r.missCount++
	}
}

func (r *result) addResult(src *result) {
	for b, s := range src.statBuckets {
		r.addStatistic(b, s)
	}
	r.getCount += src.getCount
	r.missCount += src.missCount
	r.setCount += src.setCount
}

var (
	typ, server, operation                          string
	total, valueSize, threads, keySpaceLen, pipeLen int
)

func init() {
	flag.StringVar(&typ, "type", "tcp", "cache server type")
	flag.StringVar(&server, "h", "localhost", "cache server address")
	flag.IntVar(&total, "n", 1000, "total number of requests")
	flag.IntVar(&valueSize, "d", 1000, "data size of SET/GET value in bytes")
	flag.IntVar(&threads, "c", 1, "number of parallel conn")
	flag.StringVar(&operation, "t", "set", "test set, could be get/set/mixed")
	flag.IntVar(&keySpaceLen, "r", 0, "key space len, use random keys from 0 to (key-space-len - 1)")
	flag.IntVar(&pipeLen, "p", 1, "pipeline length")
	flag.Parse()
	println("cache-cli info:")
	println("[cli-type]: ", typ)
	println("[target-server]: ", server)
	println("[total-req]: ", total)
	println("[data-size]: ", valueSize)
	println("[conn-threads]: ", threads)
	println("[operation]: ", operation)
	println("[key-space-len]: ", keySpaceLen)
	println("[pipeline]: ", pipeLen)

	rand.Seed(time.Now().UnixNano())
}

func pipeline(cli client.Client, cmds []*client.Cmd, r *result) {
	expect := make([]string, len(cmds))
	for i, c := range cmds {
		if c.Name == client.GET {
			expect[i] = c.Value
		}
	}

	start := time.Now()
	cli.PipelineRun(cmds)
	d := time.Now().Sub(start)
	for i, c := range cmds {
		resType := c.Name
		if resType == client.GET {
			if c.Value == "" {
				resType = "miss"
			} else if c.Value != expect[i] {
				panic(fmt.Sprintf("kv not match: key=%s, gotVal=%s, expectVal=%s", c.Key, c.Value, expect))
			}
		}
		r.addDuration(d, resType)
	}
}

func run(cli client.Client, c *client.Cmd, r *result) {
	expect := c.Value
	start := time.Now()
	cli.Run(c)
	d := time.Now().Sub(start)
	resType := c.Name
	if resType == client.GET {
		if c.Value == "" {
			resType = "miss"
		} else if c.Value != expect {
			panic(fmt.Sprintf("kv not match: key=%s, gotVal=%s, expectVal=%s", c.Key, c.Value, expect))
		}
	}
	r.addDuration(d, resType)
}

func operate(id, count int, ch chan *result) {
	cli := client.New(typ, server)
	cmds := make([]*client.Cmd, 0)
	valuePrefix := strings.Repeat("v", valueSize)
	res := &result{statBuckets: make([]statistic, 0)}
	for i := 0; i < count; i++ {
		var tmpKey int
		if keySpaceLen > 0 {
			tmpKey = rand.Intn(keySpaceLen)
		} else {
			tmpKey = id*count + i
		}
		key := fmt.Sprintf("%d", tmpKey)
		value := fmt.Sprintf("%s%d", valuePrefix, tmpKey)
		name := operation
		if operation == "mixed" {
			rand.Seed(time.Now().UnixNano())
			if rand.Intn(2) == 1 {
				name = client.GET
			} else {
				name = client.SET
			}
		}
		c := &client.Cmd{Name: name, Key: key, Value: value}
		if pipeLen > 1 {
			cmds = append(cmds, c)
			if len(cmds) == pipeLen {
				log.Println("process pipeline")
				pipeline(cli, cmds, res)
				cmds = make([]*client.Cmd, 0)
			}
		} else {
			run(cli, c, res)
			log.Printf("recv-resp: value=%s, err=%+v", c.Value, c.Error)
		}
	}
	if len(cmds) != 0 {
		pipeline(cli, cmds, res)
	}
	ch <- res
}

func main() {
	ch := make(chan *result, threads)
	res := &result{statBuckets: make([]statistic, 0)}
	start := time.Now()
	for i := 0; i < threads; i++ {
		// id, 每个线程分配的请求数，请求结果
		go operate(i, total/threads, ch)
	}
	for i := 0; i < threads; i++ {
		res.addResult(<-ch)
	}
	d := time.Now().Sub(start)
	totalCount := res.getCount + res.missCount + res.setCount
	fmt.Printf("%d records get\n", res.getCount)
	fmt.Printf("%d records missing\n", res.missCount)
	fmt.Printf("%d records were set\n", res.setCount)
	fmt.Printf("%d records total\n", totalCount)

	statCountSum := 0
	statTimeSum := time.Duration(0)
	for b, s := range res.statBuckets {
		if s.count == 0 {
			continue
		}
		statCountSum += s.count
		statTimeSum += s.time
		fmt.Printf("%d%% requests < %d ms\n", statCountSum*100/totalCount, b+1)
	}
	fmt.Printf("%d usec average for each request\n", int64(statTimeSum/time.Microsecond)/int64(statCountSum))
	fmt.Printf("throughput is %f MB/s\n", float64((res.getCount+res.setCount)*valueSize)/1e6/d.Seconds())
	fmt.Printf("rps is %f\n", float64(totalCount)/d.Seconds())
}
