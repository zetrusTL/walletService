package middleware

import (
	"context"
	"net/http"
	"strings"
	"wall/internal/auth"
)

type contextKey string

const UserIDKey contextKey = "user_id"

func AuthMiddleware(secret []byte) func(http.Handler) http.Handler { // middleware для аутентификации
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // обработчик запроса
			ah := r.Header.Get("Authorization")
			if ah == "" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			const prefix = "Bearer "
			if !strings.HasPrefix(ah, prefix) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			tokenString := strings.TrimPrefix(ah, prefix)
			claims, err := auth.ValidateToken(secret, tokenString) 
			if err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), UserIDKey, claims.UserID) // добавление user_id в контекст
			next.ServeHTTP(w, r.WithContext(ctx)) // передача запроса дальше
		})
	}
}

func UserIDFromContext(ctx context.Context) (int64, bool) { // извлечение user_id из контекста
	v := ctx.Value(UserIDKey)
	if v == nil {
		return 0, false
	}
	id, ok := v.(int64)
	return id, ok
}
