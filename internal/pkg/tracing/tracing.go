package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"google.golang.org/grpc/metadata"
)

// traceIDKey 上下文中的 traceID 键。
type traceIDKey struct{}

// TraceIDHeader 注入到 HTTP/gRPC 请求头中的 trace ID 字段名。
const TraceIDHeader = "X-Dolphin-Trace-ID"

// NewTraceID 生成一个 16 字节的随机 trace ID。
func NewTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// WithTraceID 将 traceID 放入 context。
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// GetTraceID 从 context 中取出 traceID。不存在时生成一个新的。
func GetTraceID(ctx context.Context) string {
	if id, ok := ctx.Value(traceIDKey{}).(string); ok && id != "" {
		return id
	}
	return NewTraceID()
}

// ToOutgoingGRPC 将 traceID 注入 gRPC 出站 metadata（透传给下游）。
func ToOutgoingGRPC(ctx context.Context) context.Context {
	id := GetTraceID(ctx)
	return metadata.AppendToOutgoingContext(ctx, TraceIDHeader, id)
}
