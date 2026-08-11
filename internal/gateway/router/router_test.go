package router

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouter_StaticRoutes(t *testing.T) {
	r := NewRouter()
	r.GET("/api/tasks", func(c *Context) { c.String(200, "tasks") })
	r.GET("/api/users", func(c *Context) { c.String(200, "users") })
	r.GET("/api/user", func(c *Context) { c.String(200, "user") })

	cases := []struct {
		path string
		want string
	}{
		{"/api/tasks", "tasks"},
		{"/api/users", "users"},
		{"/api/user", "user"},
	}

	for _, tc := range cases {
		h, _ := r.Lookup(http.MethodGet, tc.path)
		if h == nil {
			t.Errorf("path %s: handler not found", tc.path)
			continue
		}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		c := acquireContext(rr, req, []HandlerFunc{h})
		c.Next()
		releaseContext(c)
		if rr.Body.String() != tc.want {
			t.Errorf("path %s: got %q, want %q", tc.path, rr.Body.String(), tc.want)
		}
	}
}

func TestRouter_ParamRoutes(t *testing.T) {
	r := NewRouter()
	r.GET("/api/tasks/:id", func(c *Context) {
		c.String(200, "task:"+c.Param("id"))
	})
	r.GET("/api/users/:uid/tasks/:tid", func(c *Context) {
		c.String(200, "u:"+c.Param("uid")+",t:"+c.Param("tid"))
	})

	cases := []struct {
		path string
		want string
	}{
		{"/api/tasks/123", "task:123"},
		{"/api/tasks/abc", "task:abc"},
		{"/api/users/9/tasks/5", "u:9,t:5"},
	}

	for _, tc := range cases {
		h, params := r.Lookup(http.MethodGet, tc.path)
		if h == nil {
			t.Errorf("path %s: handler not found", tc.path)
			continue
		}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		c := acquireContext(rr, req, []HandlerFunc{h})
		c.Params = params
		c.Next()
		releaseContext(c)
		if rr.Body.String() != tc.want {
			t.Errorf("path %s: got %q, want %q", tc.path, rr.Body.String(), tc.want)
		}
	}
}

func TestRouter_WildcardRoutes(t *testing.T) {
	r := NewRouter()
	r.GET("/admin/*filepath", func(c *Context) {
		c.String(200, "fp:"+c.Param("filepath"))
	})
	r.GET("/static/*filepath", func(c *Context) {
		c.String(200, "static:"+c.Param("filepath"))
	})

	cases := []struct {
		path string
		want string
	}{
		{"/admin/a", "fp:a"},
		{"/admin/a/b/c", "fp:a/b/c"},
		{"/static/style.css", "static:style.css"},
	}

	for _, tc := range cases {
		h, params := r.Lookup(http.MethodGet, tc.path)
		if h == nil {
			t.Errorf("path %s: handler not found", tc.path)
			continue
		}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		c := acquireContext(rr, req, []HandlerFunc{h})
		c.Params = params
		c.Next()
		releaseContext(c)
		if rr.Body.String() != tc.want {
			t.Errorf("path %s: got %q, want %q", tc.path, rr.Body.String(), tc.want)
		}
	}
}

func TestRouter_SplitInsert(t *testing.T) {
	// 测试前缀分裂：
	// 先插入 /api/user，再插入 /api/task
	r := NewRouter()
	r.GET("/api/user", func(c *Context) { c.String(200, "user") })
	r.GET("/api/task", func(c *Context) { c.String(200, "task") })

	// 分裂后两个都应能匹配
	h1, _ := r.Lookup(http.MethodGet, "/api/user")
	h2, _ := r.Lookup(http.MethodGet, "/api/task")
	if h1 == nil || h2 == nil {
		t.Fatalf("split insert failed: h1=%v h2=%v", h1 != nil, h2 != nil)
	}

	// 更深的分裂：/users/p/orders vs /users/p/tickets
	r.GET("/users/p/orders", func(c *Context) { c.String(200, "o") })
	r.GET("/users/p/tickets", func(c *Context) { c.String(200, "t") })
	h3, _ := r.Lookup(http.MethodGet, "/users/p/orders")
	h4, _ := r.Lookup(http.MethodGet, "/users/p/tickets")
	if h3 == nil || h4 == nil {
		t.Fatalf("deep split failed")
	}
}

func TestRouter_NotFound(t *testing.T) {
	r := NewRouter()
	r.GET("/api/tasks", func(c *Context) { c.String(200, "ok") })

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/nope", nil)
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}

	// 方法不存在
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/tasks", nil)
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for wrong method, got %d", rr.Code)
	}
}

func TestRouter_ServeHTTPFull(t *testing.T) {
	r := NewRouter()
	r.GET("/api/tasks/:id", func(c *Context) {
		c.String(200, "task "+c.Param("id"))
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks/42", nil)
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || rr.Body.String() != "task 42" {
		t.Errorf("ServeHTTP full: got %d %q", rr.Code, rr.Body.String())
	}
}

// TestRouter_StatusWriterCapture 验证直接写底层 Writer 的组件（如反向代理）
// 产生的状态码能被 Context.Status 捕获，供 Metrics / 熔断器读取。
// 这是熔断器能正确感知上游 5xx 的前提。
func TestRouter_StatusWriterCapture(t *testing.T) {
	var capturedStatus int

	r := NewRouter()
	// 中间件：在 handler 执行后读取捕获到的状态码（等价于 CircuitBreakerMiddleware 的判定逻辑）
	r.Use(func(next HandlerFunc) HandlerFunc {
		return func(c *Context) {
			next(c)
			capturedStatus = c.Status
		}
	})

	// handler 模拟反向代理：直接写 http.Error 到 c.Writer，不经过 c.JSON/c.String
	r.GET("/proxy/*rest", func(c *Context) {
		http.Error(c.Writer, "Bad Gateway: upstream refused", http.StatusBadGateway)
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/proxy/upstream", nil)
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Errorf("response code: got %d, want %d", rr.Code, http.StatusBadGateway)
	}
	if capturedStatus != http.StatusBadGateway {
		t.Errorf("captured status: got %d, want %d (statusWriter must intercept direct writes)",
			capturedStatus, http.StatusBadGateway)
	}
}

// TestRouter_StatusWriterFlush 验证 statusWriter 透传 Flush，反向代理流式响应可用。
func TestRouter_StatusWriterFlush(t *testing.T) {
	r := NewRouter()
	r.GET("/stream", func(c *Context) {
		// 直接写并 Flush，模拟 SSE / 流式代理
		_, _ = c.Writer.Write([]byte("chunk"))
		if f, ok := c.Writer.(http.Flusher); ok {
			f.Flush()
		}
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("stream: got %d, want %d", rr.Code, http.StatusOK)
	}
	if rr.Body.String() != "chunk" {
		t.Errorf("stream body: got %q, want %q", rr.Body.String(), "chunk")
	}
}

func BenchmarkRouter_StaticMatch(b *testing.B) {
	r := NewRouter()
	r.GET("/api/v1/tasks", func(c *Context) { c.String(200, "ok") })
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = r.Lookup(http.MethodGet, "/api/v1/tasks")
	}
	_ = req
}

func BenchmarkRouter_ParamMatch(b *testing.B) {
	r := NewRouter()
	r.GET("/api/tasks/:id", func(c *Context) { c.String(200, "ok") })

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = r.Lookup(http.MethodGet, "/api/tasks/12345")
	}
}
