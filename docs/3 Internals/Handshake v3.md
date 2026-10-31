---
tags: [internals, protocol, diagram]
---

# Handshake v3

## TCP + UDP server

```mermaid
sequenceDiagram
  autonumber
  participant C as NetClient
  participant T as Server TCP
  participant U as Server UDP
  participant S as NetServer
  participant A as ConnectionApprover
  C->>T: TCP connect
  C->>T: Hello [ver 3, token, configHash, mode, suites, ECDH key]
  T->>S: checks version, config hash, mode, suite
  S->>S: create connection (keys, signed Welcome)
  S-->>C: Welcome [id, suite, server key, identity key, signature]
  C->>C: verify signature and pinned key, derive keys
  C->>U: Hello (same token and key) — UDP may only join
  U->>S: bind UDP link to the connection
  S-->>C: Welcome (identical)
  C->>T: Join [encrypted connect data] (inside the envelope)
  S->>S: reserve a MaxConnections slot
  S->>A: ConnectionRequest
  A-->>S: Accept / Reject(reason)
  S->>S: raise ConnectionOpened
  S-->>C: JoinAccept (or JoinReject [reason])
  Note over C,S: from here on every message travels in the authenticated envelope
```

- Hello is resent every 200 ms until Welcome (datagrams get lost; duplicates are idempotent). Join is resent every 300 ms.
- A mismatch at step 3 → `Reject [reason]` → `ConnectionRejectedException` (ProtocolVersion, Configuration, Encryption).
- If UDP stays silent for `TransportFallbackTimeoutMs`, the client sends `Degrade` over TCP instead and the connection opens without UDP ([[Connecting and approval#When UDP is blocked]]).
- Envelopes that fail verification before the connection opens mean different keys (wrong PSK) → `Reject(Authentication)` once, then close.

## UDP-only server: cookie first

```mermaid
sequenceDiagram
  participant C as NetClient
  participant U as Server UDP
  C->>U: Hello (no cookie)
  U-->>C: Retry [cookie = HMAC(key, epoch, token, address, port)]
  C->>U: Hello [cookie]
  U->>U: cookie valid for this address (current or previous 30 s epoch)
  U-->>C: Welcome …
```

No keys are generated and no connection exists until the client has proven it receives at its source address — spoofed Hellos cost the server one HMAC.

## Rules

| Rule | Why |
|---|---|
| Only stream links (TCP, WebSocket) create connections; UDP joins | UDP source addresses are trivially forged |
| ≤ 1024 handshakes pending | flood insurance |
| A bound slot is never re-bound by another address | the token is plaintext; anyone who saw it could hijack the link |
| Later Hellos must carry the same ECDH key | same token from someone else is not the same client |
| Welcome built once, identical on every link | the identity signature is costly |
| `ConnectionOpened` before `JoinAccept` | nothing the client sends can arrive ahead of the event |

Code: [[NetClient]] (`NetClient.Connect.cs`: `ConnectCoreAsync`, `SendHelloLoopAsync`, `HandleWelcome`, `EstablishKeys`, `JoinAsync`), [[NetServer]] (`NetServer.Handshake.cs`: `HandleHello`, `MayCreateConnection`, `ComputeCookie`, `CreateConnection`, `HandleJoin`, `ApproveAsync`, `TryTakeSlot`), [[PeerProtocol]], [[ConnectionApproval]]. Byte layouts: [[Wire protocol]].
