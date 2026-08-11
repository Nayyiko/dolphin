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
	"regexp"
	"sort"
	"strconv"
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
		histogram   string // -histogram <metric>: 从 /metrics 读直方图算 P50/P95/P99
		labelFilter string // -label 'k=v': 直方图 label 过滤
	)

	flag.StringVar(&url, "url", "http://localhost:8080/health", "target URL")
	flag.StringVar(&method, "method", "GET", "HTTP method")
	flag.StringVar(&body, "body", "", "request body")
	flag.IntVar(&concurrency, "concurrency", 50, "number of concurrent workers")
	flag.DurationVar(&duration, "duration", 10*time.Second, "test duration")
	flag.IntVar(&qps, "qps", 0, "limit QPS (0 = unlimited)")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "per-request timeout")
	flag.StringVar(&histogram, "histogram", "", "read histogram metric from /metrics and compute percentiles")
	flag.StringVar(&labelFilter, "label", "", "label filter for histogram, e.g. handler_type=http")
	flag.Parse()

	if histogram != "" {
		// 直方图模式：url 是 metrics 端点
		if err := readHistogram(url, histogram, labelFilter); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}

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

// readHistogram 从 Prometheus /metrics 端点读取直方图指标，计算 P50/P95/P99/均值。
// url 是 metrics 端点，metric 是直方图指标名（不含 _bucket/_count/_sum）。
// label 是可选的过滤条件，形如 "handler_type=http"。
func readHistogram(url, metric, label string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("fetch metrics: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	text := string(body)
	lines := strings.Split(text, "\n")

	// 解析 buckets: metric_bucket{...} value
	type bucket struct {
		le    float64
		count float64
	}
	var buckets []bucket
	var totalCount float64
	var totalSum float64

	for _, line := range lines {
		if label != "" && !labelMatch(line, label) {
			continue
		}
		if strings.HasPrefix(line, metric+"_bucket") {
			// 提取 le="..." 和值
			if leMatch := regexp.MustCompile(`le="([^"]+)"`).FindStringSubmatch(line); leMatch != nil {
				var le float64
				if leMatch[1] == "+Inf" {
					le = math.Inf(1)
				} else if v, err := strconv.ParseFloat(leMatch[1], 64); err == nil {
					le = v
				} else {
					continue
				}
				val := parseLastFloat(line)
				buckets = append(buckets, bucket{le: le, count: val})
			}
		} else if strings.HasPrefix(line, metric+"_count") && !strings.HasPrefix(line, metric+"_count_") {
			totalCount = parseLastFloat(line)
		} else if strings.HasPrefix(line, metric+"_sum") {
			totalSum = parseLastFloat(line)
		}
	}

	if len(buckets) == 0 {
		return fmt.Errorf("no buckets found for metric %s (label=%s), check /metrics endpoint", metric, label)
	}

	// 排序 buckets by le
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].le < buckets[j].le })

	// 百分位估算（线性插值）
	pct := func(p float64) float64 {
		target := totalCount * p
		prevLE, prevCum := 0.0, 0.0
		for _, b := range buckets {
			if math.IsInf(b.le, 1) {
				return prevLE
			}
			if b.count >= target {
				if b.count <= prevCum {
					return b.le
				}
				ratio := (target - prevCum) / (b.count - prevCum)
				return prevLE + (b.le - prevLE) * ratio
			}
			prevLE, prevCum = b.le, b.count
		}
		return prevLE
	}

	avg := 0.0
	if totalCount > 0 {
		avg = totalSum / totalCount
	}

	fmt.Println()
	fmt.Println("══════════════ 直方图统计 ══════════════")
	fmt.Printf("指标:       %s\n", metric)
	if label != "" {
		fmt.Printf("标签过滤:   %s\n", label)
	}
	fmt.Printf("样本数:     %d\n", int(totalCount))
	if totalCount >= 10 {
		fmt.Printf("均值:       %.3f s (%.1f ms)\n", avg, avg*1000)
		fmt.Printf("P50:        %.3f s (%.1f ms)\n", pct(0.50), pct(0.50)*1000)
		fmt.Printf("P95:        %.3f s (%.1f ms)\n", pct(0.95), pct(0.95)*1000)
		fmt.Printf("P99:        %.3f s (%.1f ms)\n", pct(0.99), pct(0.99)*1000)
	} else {
		fmt.Printf("样本数不足 (<10)，无法给出可信百分位\n")
	}
	fmt.Println("══════════════════════════════════════")
	return nil
}

// parseLastFloat 提取行末尾的数值。
func parseLastFloat(line string) float64 {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return 0
	}
	v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
	if err != nil {
		return 0
	}
	return v
}

// labelMatch 检查行是否匹配 label 条件。
// 支持 "key=value" 和 "key=\"value\"" 两种形式。
func labelMatch(line, label string) bool {
	key, val, found := strings.Cut(label, "=")
	if !found {
		return strings.Contains(line, label)
	}
	// 匹配 key="value" 或 key=value
	if strings.Contains(line, key+"=\""+val+"\"") {
		return true
	}
	return strings.Contains(line, key+"="+val)
}
