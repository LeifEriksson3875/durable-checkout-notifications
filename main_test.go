package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCheckoutQueuesOfflineNotification(t *testing.T) {
	var got string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/queue/publish" {
			t.Fatalf("path %s", r.URL.Path)
		}
		b := make([]byte, r.ContentLength)
		r.Body.Read(b)
		got = string(b)
		w.Write([]byte(`{"ok":true,"data":{},"error":null}`))
	}))
	defer ts.Close()
	c := &InfraiClient{baseURL: ts.URL, key: "test", httpClient: ts.Client()}
	r := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/checkout", strings.NewReader(`{"OrderID":"o-7","CustomerID":"c-2","total_cents":4200}`))
	checkoutHandler(c)(r, req)
	if r.Code != 200 || !strings.Contains(got, "o-7") {
		t.Fatalf("status=%d payload=%s", r.Code, got)
	}
	_ = context.Background()
}
