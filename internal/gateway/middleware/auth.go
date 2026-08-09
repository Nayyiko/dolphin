package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/yourname/dolphin/internal/gateway/router"
)

// Claims 自定义 JWT claims。
type Claims struct {
	UserID   string `json:"user_id"`
	ClientID string `json:"client_id"`
	jwt.RegisteredClaims
}

// Auth JWT 鉴权中间件。
// 从 Authorization: Bearer <token> 中解析 JWT，校验签名和过期时间，
// 将 user_id 注入 Context。
func Auth(secret string) router.Middleware {
	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(c *router.Context) {
			auth := c.Request.Header.Get("Authorization")
			if auth == "" {
				c.JSON(http.StatusUnauthorized, map[string]any{
					"code":    "unauthorized",
					"message": "missing Authorization header",
				})
				c.Abort()
				return
			}

			// 支持 "Bearer <token>" 或裸 token
			tokenStr := strings.TrimPrefix(auth, "Bearer ")
			tokenStr = strings.TrimSpace(tokenStr)

			claims, err := ParseToken(tokenStr, secret)
			if err != nil {
				c.JSON(http.StatusUnauthorized, map[string]any{
					"code":    "invalid_token",
					"message": err.Error(),
				})
				c.Abort()
				return
			}

			c.Set("user_id", claims.UserID)
			c.Set("client_id", claims.ClientID)
			next(c)
		}
	}
}

// GenerateToken 生成 JWT。
func GenerateToken(userID, clientID, secret string, expireHours int) (string, error) {
	claims := Claims{
		UserID:   userID,
		ClientID: clientID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(expireHours) * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   userID,
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// ParseToken 解析并校验 JWT。
func ParseToken(tokenStr, secret string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, jwt.ErrTokenInvalidClaims
	}
	return claims, nil
}
