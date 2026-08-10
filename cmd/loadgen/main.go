// loadgen 是一个轻量 HTTP 负载生成器。
// 用于对网关进行压测，输出 QPS、P50/P95/P99 延迟、错误率。
//
// 用法:
//
//	loadgen -url http://localhost:8080/health -concurrency 50 -duration 10s
//	loadgen -url http://localhost:8080/health -qps 1000 -duration 30s
//	loadgen -url http://localhost:8080/health -concurrency 100 -duration 5s -method POST -body '{"name":"x"}'
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	latency time.Duration
	status  int
	err     bool
}

func main() {
	var (
		url         string
		method      string
		body        string
		concurrency int
		duration    time.Duration
		qps         int
		timeout     time.Duration
	)

	flag.StringVar(&url, "url", "http://localhost:8080/health", "target URL")
	flag.StringVar(&method, "method", "GET", "HTTP method")
	flag.StringVar(&body, "body", "", "request body")
	flag.IntVar(&concurrency, "concurrency", 50, "number of concurrent workers")
	flag.DurationVar(&duration, "duration", 10*time.Second, "test duration")
	flag.IntVar(&qps, "qps", 0, "limit QPS (0 = unlimited)")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "per-request timeout")
	flag.Parse()

	if url == "" {
		fmt.Println("error: -url is required")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("loadgen 开始压测: %s\n", url)
	fmt.Printf("  method=%s concurrency=%d duration=%s", method, concurrency, duration)
	if qps > 0 {
		fmt.Printf(" qps_limit=%d", qps)
	}
	fmt.Println()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	var (
		total     atomic.Int64
		errCount  atomic.Int64
		status5xx atomic.Int64
		results   = make(chan result, 1024)
		done      = make(chan struct{})
		wg        sync.WaitGroup
		all       []result
		mu        sync.Mutex
	)

	// 收集结果 goroutine
	go func() {
		for r := range results {
			mu.Lock()
			all = append(all, r)
			mu.Unlock()
		}
		close(done)
	}()

	// 限速器
	var limiter <-chan time.Time
	if qps > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(qps))
		defer ticker.Stop()
		limiter = ticker.C
	}

	// 工作 goroutine
	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if limiter != nil {
				select {
				case <-limiter:
				case <-ctx.Done():
					return
				}
			}

			reqStart := time.Now()
			resp, err := doRequest(client, method, url, body)
			var st int
			var failed bool
			if err != nil {
				st = 0
				failed = true
				errCount.Add(1)
			} else {
				st = resp.StatusCode
				_ = resp.Body.Close()
				if st >= 500 {
					status5xx.Add(1)
				}
			}
			total.Add(1)
			results <- result{latency: time.Since(reqStart), status: st, err: failed}
		}
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	wg.Wait()
	close(results)
	<-done

	elapsed := time.Since(start)

	// 汇总
	mu.Lock()
	defer mu.Unlock()
	success := 0
	for _, r := range all {
		if !r.err && r.status < 500 {
			success++
		}
	}

	totalN := len(all)
	if totalN == 0 {
		fmt.Println("无请求完成")
		return
	}

	sort.Slice(all, func(i, j int) bool { return all[i].latency < all[j].latency })

	// 延迟分位数
	latencies := make([]float64, totalN)
	for i, r := range all {
		latencies[i] = float64(r.latency) / 1e6 // ms
	}
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	fmt.Println()
	fmt.Println("══════════════ 压测结果 ══════════════")
	fmt.Printf("总请求数:    %d\n", totalN)
	fmt.Printf("成功请求:    %d (%.2f%%)\n", success, float64(success)/float64(totalN)*100)
	fmt.Printf("5xx 错误:    %d\n", status5xx.Load())
	fmt.Printf("连接错误:    %d\n", errCount.Load())
	fmt.Printf("QPS:         %.0f req/s\n", float64(totalN)/elapsed.Seconds())
	fmt.Printf("平均延迟:    %.2f ms\n", avg(latencies))
	fmt.Printf("P50 延迟:    %.2f ms\n", p50)
	fmt.Printf("P95 延迟:    %.2f ms\n", p95)
	fmt.Printf("P99 延迟:    %.2f ms\n", p99)
	fmt.Printf("最大延迟:    %.2f ms\n", latencies[len(latencies)-1])
	fmt.Println("══════════════════════════════════════")
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func avg(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var sum float64
	for _, x := range v {
		sum += x
	}
	return sum / float64(len(v))
}

// doRequest 发送 HTTP 请求，支持任意方法和 body。
func doRequest(client *http.Client, method, url, body string) (*http.Response, error) {
	var reqBody io.Reader
	if body != "" {
		reqBody = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return client.Do(req)
}
