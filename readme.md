# Redis Streams + Rust — Event-Driven Architecture

A hands-on exploration of building a reliable, event-driven messaging system using **Redis Streams** and **Rust**.

---

## What We Built

```
Rust Producer
     ↓
Redis Stream (XADD)
     ↓
Consumer Group
     ↓
Worker (XREADGROUP)
     ↓
Deserialize Event
     ↓
Process Event
     ↓
XACK
```

This forms a real event-driven architecture foundation — the same pattern used in production exchange and messaging systems.

---

## Core Concepts

### 1. Redis Streams are Append-Only Logs

Redis Streams behave like event logs:

- Messages **persist** in the stream after delivery.
- Consumers **do not** remove messages automatically.
- Great for replay, auditing, and recovery.

---

### 2. Streams Store Field-Value Pairs

Redis Streams do **not** store raw JSON directly. They store `field → value` pairs.

```
1779689554578-0
    data → "{\"user\":111,...}"
```

We use a single `data` field and serialize our struct into it.

---

### 3. Serialization Flow

**Producer side:**
```
Rust Struct → serde_json::to_string() → JSON String → Redis Bytes → Redis Stream
```

**Consumer side:**
```
Redis Bytes → UTF-8 String → serde_json::from_str() → Rust Struct
```

---

### 4. Redis Transport is Binary-Safe

Redis internally transports `Vec<u8>`, not Rust strings. This is why we receive:

```rust
Value::Data(Vec<u8>)
```

And convert it using:

```rust
String::from_utf8(bytes)
```

---

### 5. Consumer Groups

| Concept | Role |
|---|---|
| **Stream** | Append-only log |
| **Group** | Delivery tracking system |
| **Consumer** | Actual worker process |

Our setup:
- Stream: `redis_test`
- Group: `chat_workers`
- Consumer: `worker_1`

---

### 6. XREAD vs XREADGROUP

| Feature | XREAD | XREADGROUP |
|---|---|---|
| Pending tracking | ✗ | ✓ |
| Acknowledgements | ✗ | ✓ |
| Worker coordination | ✗ | ✓ |
| Delivery tracking | ✗ | ✓ |

**Use XREADGROUP for any reliable production system.**

---

### 7. The Meaning of `>`

```
XREADGROUP ... STREAMS redis_test >
```

| ID | Meaning |
|---|---|
| `>` | New, undelivered messages |
| `0` | Pending/recovery messages |
| `$` | Only future messages |

---

### 8. Pending Messages & ACK Lifecycle

When a worker reads a message, it becomes **PENDING** — Redis assumes the worker might crash before finishing.

```
Message → Delivered → Pending → Processed → XACK → Removed from PEL
```

**XACK only removes from the Pending Entries List (PEL) — the stream entry still exists.**

---

### 9. Important Stream Commands

```bash
# Add an event
XADD redis_test * data "hello"

# Create a consumer group
XGROUP CREATE redis_test chat_workers 0

# Read messages as a worker
XREADGROUP GROUP chat_workers worker_1 STREAMS redis_test >

# Acknowledge a message
XACK redis_test chat_workers <message_id>

# View pending messages
XPENDING redis_test chat_workers

# Inspect the stream
XRANGE redis_test - +

# Inspect consumer groups
XINFO GROUPS redis_test
```

---

### 10. Stream Entry IDs

```
1779689554578-0
```

Structure: `timestamp-sequence`

Used for: ordering, replay, acknowledgements, recovery, and checkpointing.

---

## Rust Lessons

### `match event` vs `match &event`

```rust
match event   // moves ownership
match &event  // borrows — prefer this when you still need `event` later
```

### `continue` skips the rest of the loop iteration

Be careful — placing `continue` before your ACK logic will silently skip acknowledgements.

### Redis commands often need explicit types

```rust
let id: String = conn.xadd(...).unwrap();
```

Redis is dynamically typed; Rust needs a hint to deserialize the response.

### Non-Lexical Lifetimes (NLL)

A borrow ends at its **last usage**, not necessarily at the end of scope. This matters when you're mixing mutable and immutable borrows of the same variable.

---

## Architecture Separation

**Always separate transport from business logic.**

| Layer | Contains |
|---|---|
| Transport | Redis bytes, stream IDs, delivery metadata, consumer groups |
| Business | `ChatMessage`, `TradeExecutedEvent`, `CreateOrderEvent` |

---

## Event-Driven Mindset

Instead of shared mutable state, think in **immutable events flowing through services**:

```
CreateOrderEvent → TradeExecutedEvent → BalanceUpdatedEvent
```

---

## Final Architecture Direction

This naturally evolves into a full exchange-style pipeline:

```
API Service
     ↓
orders_stream
     ↓
Matching Engine
     ↓
trades_stream
     ↓
DB Service
     ↓
WebSocket Service
```

Each service is decoupled, independently scalable, and communicates exclusively through streams.

---

## Quick Reference

| Command | Purpose |
|---|---|
| `XADD` | Append event to stream |
| `XGROUP CREATE` | Create a consumer group |
| `XREADGROUP` | Read as a group-aware worker |
| `XACK` | Mark message as processed |
| `XPENDING` | Inspect unacknowledged messages |
| `XRANGE` | Scan stream entries |
| `XINFO GROUPS` | Inspect group state (lag, pending count) |