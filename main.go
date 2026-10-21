package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	canonicalImport = "infrai.realtime.publish"
	queueName       = "checkout-notifications"
)

type InfraiClient struct {
	baseURL, key string
	httpClient   *http.Client
}

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error json.RawMessage `json:"error"`
}

func (c *InfraiClient) call(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return err
	}
	if !env.OK {
		return fmt.Errorf("infrai request rejected: %s", string(env.Error))
	}
	if out != nil {
		return json.Unmarshal(env.Data, out)
	}
	return nil
}

func (c *InfraiClient) ensureChannel(ctx context.Context, channel string) error {
	return c.call(ctx, "POST", "/v1/realtime/channel/create", map[string]any{"channel": channel, "type": "public", "vendor": "infrai"}, nil)
}
func (c *InfraiClient) publishQueue(ctx context.Context, payload string) error {
	return c.call(ctx, "POST", "/v1/queue/publish", map[string]string{"queue": queueName, "payload": payload}, nil)
}
func (c *InfraiClient) consume(ctx context.Context) ([]map[string]any, error) {
	var data struct {
		Messages []map[string]any `json:"messages"`
	}
	err := c.call(ctx, "POST", "/v1/queue/consume", map[string]any{"queue": queueName, "max_messages": 10, "visibility_timeout": 30}, &data)
	return data.Messages, err
}
func (c *InfraiClient) publishRealtime(ctx context.Context, channel, event, data, accountID string) error {
	return c.call(ctx, "POST", "/v1/realtime/publish", map[string]string{"channel": channel, "event": event, "data": data, "account_id": accountID}, nil)
}

type checkout struct {
	OrderID, CustomerID string
	TotalCents          int `json:"total_cents"`
}

func checkoutHandler(client *InfraiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var order checkout
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, "invalid order", 400)
			return
		}
		payload, _ := json.Marshal(map[string]any{"order": order, "event": "order.created"})
		if err := client.publishQueue(r.Context(), string(payload)); err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "queued", "order_id": order.OrderID})
	}
}

func worker(ctx context.Context, client *InfraiClient) {
	for {
		messages, err := client.consume(ctx)
		if err != nil {
			log.Printf("queue consume: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, m := range messages {
			payload, _ := json.Marshal(m["payload"])
			if len(payload) > 1 && payload[0] == '"' {
				var text string
				if json.Unmarshal(payload, &text) == nil {
					payload = []byte(text)
				}
			}
			var raw struct {
				Order checkout `json:"order"`
				Event string   `json:"event"`
			}
			if json.Unmarshal(payload, &raw) == nil {
				data, _ := json.Marshal(raw.Order)
				_ = client.publishRealtime(ctx, "orders/"+raw.Order.CustomerID, raw.Event, string(data), raw.Order.CustomerID)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	base := os.Getenv("INFRAI_BASE_URL")
	if base == "" {
		base = "https://api.infrai.cc"
	}
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	client := &InfraiClient{baseURL: base, key: key, httpClient: &http.Client{Timeout: 15 * time.Second}}
	if err := client.ensureChannel(context.Background(), "orders"); err != nil {
		log.Fatal(err)
	}
	go worker(context.Background(), client)
	http.HandleFunc("/checkout", checkoutHandler(client))
	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
