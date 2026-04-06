package iceberg

import "net/http"

// BearerTransport is an http.RoundTripper that adds a Bearer token
// Authorization header to every request.
type BearerTransport struct {
	inner http.RoundTripper
	token string
}

// NewBearerTransport creates a new BearerTransport with the given token.
func NewBearerTransport(token string) *BearerTransport {
	return &BearerTransport{inner: http.DefaultTransport, token: token}
}

func (t *BearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+t.token)
	return t.inner.RoundTrip(req)
}
