package router

import (
	"net/http"
	"strings"
)

// node Radix Tree 节点。node.path 是单个路径片段（不含斜杠），前缀压缩。
type node struct {
	path      string // 相对父节点的路径片段
	children  []*node
	handler   HandlerFunc
	isWild    bool   // 是否为参数节点（:id 或 *filepath）
	paramName string // 参数名
}

// Router 路由表。按 HTTP method 分树。
type Router struct {
	trees      map[string]*node
	middlewares []Middleware // 全局中间件（在 Handle 注册时包装进 handler）
	notFound   HandlerFunc
}

// NewRouter 创建路由。
func NewRouter() *Router {
	return &Router{
		trees: make(map[string]*node),
		notFound: func(c *Context) {
			c.String(http.StatusNotFound, "404 page not found")
		},
	}
}

// Use 注册全局中间件（作用于之后注册的所有路由）。
func (r *Router) Use(mws ...Middleware) {
	r.middlewares = append(r.middlewares, mws...)
}

// Handle 注册 handler。将全局中间件以洋葱模型包装。
func (r *Router) Handle(method, pattern string, handler HandlerFunc) {
	h := handler
	for i := len(r.middlewares) - 1; i >= 0; i-- {
		h = r.middlewares[i](h)
	}
	root, ok := r.trees[method]
	if !ok {
		root = &node{}
		r.trees[method] = root
	}
	root.insert(splitPath(pattern), h)
}

// GET 注册 GET 路由。
func (r *Router) GET(pattern string, handler HandlerFunc) {
	r.Handle(http.MethodGet, pattern, handler)
}

// POST 注册 POST 路由。
func (r *Router) POST(pattern string, handler HandlerFunc) {
	r.Handle(http.MethodPost, pattern, handler)
}

// PUT 注册 PUT 路由。
func (r *Router) PUT(pattern string, handler HandlerFunc) {
	r.Handle(http.MethodPut, pattern, handler)
}

// DELETE 注册 DELETE 路由。
func (r *Router) DELETE(pattern string, handler HandlerFunc) {
	r.Handle(http.MethodDelete, pattern, handler)
}

// Lookup 查找 handler 和路径参数。
func (r *Router) Lookup(method, path string) (HandlerFunc, map[string]string) {
	root, ok := r.trees[method]
	if !ok {
		return nil, nil
	}
	params := make(map[string]string)
	h := root.search(strings.TrimPrefix(path, "/"), params)
	if h == nil {
		return nil, nil
	}
	return h, params
}

// ServeHTTP 实现 http.Handler。
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	handler, params := r.Lookup(req.Method, req.URL.Path)
	if handler == nil {
		// 未匹配：直接写 404，不走 Context 池。
		c := acquireContext(w, req, nil)
		r.notFound(c)
		releaseContext(c)
		return
	}
	c := acquireContext(w, req, []HandlerFunc{handler})
	c.Params = params
	// 用 statusWriter 包裹原始 writer，反向代理等直接写 writer 的组件
	// 也能被捕获到真实状态码（否则 Metrics/熔断器永远看到 200）。
	if sw, ok := w.(*statusWriter); ok {
		c.Writer = sw
	} else {
		c.Writer = &statusWriter{ResponseWriter: w, ctx: c}
	}
	c.Next()
	releaseContext(c)
}

// insert 将 segments 插入节点树。
// 核心算法：前缀分裂。
func (n *node) insert(segments []string, handler HandlerFunc) {
	if len(segments) == 0 {
		n.handler = handler
		return
	}

	seg := segments[0]
	rest := segments[1:]

	// 通配符 *: 吞掉剩余所有路径，必须是叶子。
	if strings.HasPrefix(seg, "*") {
		w := &node{path: seg, isWild: true, paramName: strings.TrimPrefix(seg, "*")}
		n.children = append(n.children, w)
		w.handler = handler
		return
	}

	// 路径参数 :id
	if strings.HasPrefix(seg, ":") {
		name := strings.TrimPrefix(seg, ":")
		for _, c := range n.children {
			if c.isWild && !strings.HasPrefix(c.path, "*") && c.paramName == name {
				c.insert(rest, handler)
				return
			}
		}
		p := &node{path: seg, isWild: true, paramName: name}
		n.children = append(n.children, p)
		p.insert(rest, handler)
		return
	}

	// 静态片段：前缀压缩/分裂
	for _, c := range n.children {
		if c.isWild {
			continue
		}
		common := longestCommonPrefix(seg, c.path)
		if common == 0 {
			continue
		}

		switch {
		case common == len(seg) && common == len(c.path):
			// 完全一致：直接下钻
			c.insert(rest, handler)
			return
		case common == len(c.path):
			// c.path 是 seg 的前缀：把 seg 剩余部分作为 c 的子节点
			c.insert(append([]string{seg[common:]}, rest...), handler)
			return
		case common == len(seg):
			// seg 是 c.path 的前缀：分裂 c
			// c.path="user" seg="use" → c 变为 "use"，生成子节点 "er" 承载 c 原有内容
			oldChildren := c.children
			oldHandler := c.handler
			sub := &node{
				path:      c.path[common:],
				children:  oldChildren,
				handler:   oldHandler,
				isWild:    c.isWild,
				paramName: c.paramName,
			}
			c.path = seg
			c.children = []*node{sub}
			c.handler = nil
			c.isWild = false
			c.paramName = ""
			if len(rest) == 0 {
				c.handler = handler
			} else {
				c.insert(rest, handler)
			}
			return
		default:
			// common 小于两者：同时分裂两者
			// c.path="user" seg="useA" common="use"
			oldChildren := c.children
			oldHandler := c.handler
			subC := &node{
				path:      c.path[common:],
				children:  oldChildren,
				handler:   oldHandler,
				isWild:    c.isWild,
				paramName: c.paramName,
			}
			c.path = seg[:common]
			c.children = []*node{subC}
			c.handler = nil
			c.isWild = false
			c.paramName = ""

			if len(rest) == 0 {
				subSeg := &node{path: seg[common:], handler: handler}
				c.children = append(c.children, subSeg)
			} else {
				subSeg := &node{path: seg[common:]}
				c.children = append(c.children, subSeg)
				subSeg.insert(rest, handler)
			}
			return
		}
	}

	// 无匹配：新建子节点
	nc := &node{path: seg}
	n.children = append(n.children, nc)
	nc.insert(rest, handler)
}

// search 匹配路径，返回 handler。优先静态，其次参数，最后通配符。
func (n *node) search(path string, params map[string]string) HandlerFunc {
	if path == "" {
		return n.handler
	}

	// 1. 静态子节点
	for _, c := range n.children {
		if c.isWild {
			continue
		}
		if strings.HasPrefix(path, c.path) {
			remaining := path[len(c.path):]
			if strings.HasPrefix(remaining, "/") {
				remaining = remaining[1:]
			}
			if h := c.search(remaining, params); h != nil {
				return h
			}
		}
	}

	// 2. 参数子节点 :id（消费一个 segment）
	for _, c := range n.children {
		if c.isWild && !strings.HasPrefix(c.path, "*") {
			seg, remaining := consumeSegment(path)
			if seg == "" {
				continue
			}
			params[c.paramName] = seg
			if h := c.search(remaining, params); h != nil {
				return h
			}
			delete(params, c.paramName) // 回溯
		}
	}

	// 3. 通配符子节点 *filepath（吞掉剩余全部）
	for _, c := range n.children {
		if c.isWild && strings.HasPrefix(c.path, "*") {
			params[c.paramName] = path
			return c.handler
		}
	}

	return nil
}

// splitPath 将路径拆成非空 segment 数组。
func splitPath(path string) []string {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func longestCommonPrefix(a, b string) int {
	max := len(a)
	if len(b) < max {
		max = len(b)
	}
	i := 0
	for i < max && a[i] == b[i] {
		i++
	}
	return i
}

func consumeSegment(path string) (string, string) {
	if path == "" {
		return "", ""
	}
	if idx := strings.IndexByte(path, '/'); idx >= 0 {
		return path[:idx], path[idx+1:]
	}
	return path, ""
}
