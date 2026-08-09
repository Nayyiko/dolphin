package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/yourname/dolphin/internal/gateway/router"
)

func TestAuth_ValidToken(t *testing.T) {
	secret := "test-secret"
	token, err := GenerateToken("user-1", "client-1", secret, 24)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	r := router.NewRouter()
	r.Use(Auth(secret))
	r.GET("/api/tasks", func(c *router.Context) {
		uid, _ := c.Get("user_id")
		c.JSON(200, map[string]any{"user_id": uid})
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestAuth_MissingToken(t *testing.T) {
	r := router.NewRouter()
	r.Use(Auth("secret"))
	r.GET("/api/tasks", func(c *router.Context) { c.String(200, "ok") })

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAuth_InvalidToken(t *testing.T) {
	r := router.NewRouter()
	r.Use(Auth("secret"))
	r.GET("/api/tasks", func(c *router.Context) { c.String(200, "ok") })

	// 用错误密钥签发的 token
	token, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_id": "x"}).SignedString([]byte("wrong"))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestRecovery_Panic(t *testing.T) {
	r := router.NewRouter()
	r.Use(Recovery())
	r.GET("/api/boom", func(c *router.Context) {
		panic("kaboom")
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/boom", nil)
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestChain_Order(t *testing.T) {
	var order []string

	mw := func(name string) router.Middleware {
		return func(next router.HandlerFunc) router.HandlerFunc {
			return func(c *router.Context) {
				order = append(order, name+"-before")
				next(c)
				order = append(order, name+"-after")
			}
		}
	}

	h := Chain(func(c *router.Context) {
		order = append(order, "handler")
	}, mw("m1"), mw("m2"))

	c := &router.Context{}
	h(c)

	want := []string{"m1-before", "m2-before", "handler", "m2-after", "m1-after"}
	if len(order) != len(want) {
		t.Fatalf("got %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("got %v, want %v", order, want)
		}
	}
}
