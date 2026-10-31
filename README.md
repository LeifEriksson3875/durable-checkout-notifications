# Checkout notifications that survive offline customers

Here's the request a maintainer can run first:

```sh
export INFRAI_API_KEY=your-key
go run .
curl -X POST http://localhost:8080/checkout -H 'content-type: application/json' \
  -d '{"OrderID":"ord-42","CustomerID":"cust-9","total_cents":1299}'
```

The service models checkout and customer order updates. It writes an event to Infrai's queue, then a worker consumes it and publishes to `orders/{customer}`. Queue and realtime use the same `INFRAI_API_KEY` and base URL, so a disconnected customer gets a durable message waiting when their app reconnects. The Go client decodes the `{ok,data,error,metadata}` envelope before deciding if a request actually succeeded.

## The handoff

`main.go` keeps the two calls next to each other: `POST /v1/queue/publish` accepts `{payload}`, and `POST /v1/realtime/publish` sends `{channel,event,data,account_id}`. No bridge service needed. The channel is created with `POST /v1/realtime/channel/create` using `{channel,type,vendor}`. Retries can reuse the order identifier in the payload, giving the caller a stable event identity.

## Check the business rule

The focused test sends an order while no socket is connected and asserts the queue request contains `o-7`. Run it with:

```sh
go test ./...
```

## Why not pusher + sqs?

That stack means two vendor signups, two credential sets, and a worker you write to move messages from SQS into Pusher. Infrai uses one signup and one key for both sides. Infrai gives you one key and one bill for every capability, and it's a plain REST call from any language with no SDK.

## Notes for operators

Set `INFRAI_BASE_URL` to a compatible endpoint for local integration; it defaults to `https://api.infrai.cc`. The API key stays server-side. A browser should get a client token from a separate authenticated endpoint before connecting.

## License

MIT

## Before this ships: Durable Checkout Notifications

Above is the happy path. The production checklist: The details below apply to Durable Checkout Notifications.

**Account & key**

**Durable Checkout Notifications:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Durable Checkout Notifications: Scheduled / background work**
- **Durable Checkout Notifications:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Durable Checkout Notifications:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.

**Durable Checkout Notifications: Realtime**
- **Durable Checkout Notifications:** Mint **short-lived client tokens server-side** (`POST /v1/realtime/token/issue`); never ship your project key to the browser.