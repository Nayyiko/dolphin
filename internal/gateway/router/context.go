package router

import (
	"encoding/json"
	"net/http"
	"sync"
)

// HandlerFunc 统一的 handler 签名。
type HandlerFunc func(*Context)

// Middleware 中间件签名。接收一个 handler，返回包装后的 handler（洋葱模型）。
type Middleware func(HandlerFunc) HandlerFunc

// Context 请求上下文，贯穿整个请求生命周期。
// 采用对象池复用，减少 GC 压力。
type Context struct {
	Request  *http.Request
	Writer   http.ResponseWriter
	Params   map[string]string // URL 路径参数 /api/task/:id → {"id":"123"}
	Keys     map[string]any    // 中间件间传递值（如 trace_id, user_id）

	handlers []HandlerFunc
	index    int
	aborted  bool

	// Status 记录最终响应状态码（供 metrics 使用）。
	Status int
}

var contextPool = sync.Pool{
	New: func() any {
		return &Context{}
	},
}

// acquireContext 从池中获取 Context 并初始化。
func acquireContext(w http.ResponseWriter, r *http.Request, handlers []HandlerFunc) *Context {
	c := contextPool.Get().(*Context)
	c.Writer = w
	c.Request = r
	c.handlers = handlers
	c.index = -1
	c.aborted = false
	c.Status = http.StatusOK
	c.Params = make(map[string]string)
	c.Keys = make(map[string]any)
	return c
}

// releaseContext 归还 Context 到池中并清理引用，防止内存泄漏。
func releaseContext(c *Context) {
	c.Request = nil
	c.Writer = nil
	c.handlers = nil
	c.Params = nil
	c.Keys = nil
	contextPool.Put(c)
}

// Next 执行下一个 handler/middleware。
// 中间件在调用 next(c) 之前/之后的代码分别构成洋葱的外层/内层。
func (c *Context) Next() {
	c.index++
	for c.index < len(c.handlers) && !c.aborted {
		c.handlers[c.index](c)
		c.index++
	}
}

// Abort 中断 handler 链，后续 handler 不再执行。
func (c *Context) Abort() {
	c.aborted = true
}

// Set 在中间件间传递值。
func (c *Context) Set(key string, value any) {
	if c.Keys == nil {
		c.Keys = make(map[string]any)
	}
	c.Keys[key] = value
}

// Get 取出中间件间传递的值。
func (c *Context) Get(key string) (any, bool) {
	v, ok := c.Keys[key]
	return v, ok
}

// Param 获取路径参数。
func (c *Context) Param(key string) string {
	return c.Params[key]
}

// JSON 便捷方法：写 JSON 响应。
func (c *Context) JSON(status int, data any) {
	c.Status = status
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.Writer.WriteHeader(status)
	_ = json.NewEncoder(c.Writer).Encode(data)
}

// String 便捷方法：写文本响应。
func (c *Context) String(status int, s string) {
	c.Status = status
	c.Writer.WriteHeader(status)
	_, _ = c.Writer.Write([]byte(s))
}
