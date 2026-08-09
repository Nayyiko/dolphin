package proxy

import (
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/yourname/dolphin/internal/pkg/tracing"
)

// ReverseProxy 反向代理。
// 基于 httputil.ReverseProxy，自定义 Director（改目标地址）和 Transport（连接池）。
type ReverseProxy struct {
	proxy *httputil.ReverseProxy
	lb    *LoadBalancer
}

// NewReverseProxy 创建反向代理。
// targets 是上游地址列表，如 ["http://10.0.0.1:8080", ...]。
func NewReverseProxy(targets []string, algo string) *ReverseProxy {
	lb := NewLoadBalancer(targets, nil, algo)

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			target := lb.Next()
			if target == "" {
				return
			}
			// 保留原始 scheme/host，仅替换上游地址
			req.URL.Scheme = "http"
			req.URL.Host = target

			// 注入/透传 trace id
			if traceID := req.Header.Get(tracing.TraceIDHeader); traceID == "" {
				req.Header.Set(tracing.TraceIDHeader, tracing.NewTraceID())
			}
			req.Header.Set("X-Forwarded-For", req.RemoteAddr)
			req.Header.Set("X-Forwarded-Host", req.Host)
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "Bad Gateway: "+err.Error(), http.StatusBadGateway)
		},
	}

	return &ReverseProxy{proxy: proxy, lb: lb}
}

// ServeHTTP 实现 http.Handler。
func (p *ReverseProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.proxy.ServeHTTP(w, r)
}
