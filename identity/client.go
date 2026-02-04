package identity

import "net/http"

type ClientIdResolver func(*http.Request) (string, error)

func DefaultClientIdResolver(r *http.Request) (string, error) {
	clientId := r.Header.Get("SSE-Client-ID")
	return clientId, nil
}
