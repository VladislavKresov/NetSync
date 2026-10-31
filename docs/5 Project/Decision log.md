---
tags: [project]
---

# Decision log

| Date | Decision | Context |
|---|---|---|
| 2026-07-05 | Bluetooth dropped; targets `netstandard2.1` + `net8.0`; free API redesign; English docs | 2.0 rewrite |
| 2026-07-05 | Reliability window range-based, not count-based | a loss test showed the sender outrunning the receiver's dedup ring — [[Reliable UDP]] |
| 2026-07-05 | One cipher (AES-CBC+HMAC) on both targets | superseded 2026-10-14 by negotiated suites |
| 2026-07-06 | Benchmarks package-free; MIT; NuGet id `NetSync`; versions synced core ↔ Unity | release prep |
| 2026-10-13 | Phase 2.1 after a code review: limits, protocol v2 (config hash), factories, fallback, congestion control, packet authentication, usability layer | [[Limits and hardening]] |
| 2026-10-13 | UDP→TCP fallback on by default — connectivity first | [[Connecting and approval]] |
| 2026-10-13 | Bound links never re-bound by Hello (no connection migration) | link hijack test |
| 2026-10-13 | No explicit pacing — ACK clocking was enough on the bottleneck simulator | [[Reliable UDP]] |
| 2026-10-13 | Priority on UDP = congestion-control flavour (Low = LEDBAT); queue order on TCP | [[Channels and delivery modes]] |
| 2026-10-14 | NetSync is a universal low-level core; engine adapters live one level up; a JS/TS client is a separate package | phase 2.2 vision — [[What is NetSync]] |
| 2026-10-14 | Zero allocation per message as an enforced property; send buffers reusable when `SendAsync` returns | [[Send path]] |
| 2026-10-14 | Protocol v3: negotiated suites, server identity signatures, Join with encrypted connect data and approval, cookies for UDP-only servers | [[Handshake v3]], [[Keys and envelope]] |
| 2026-10-14 | Stream links create connections, datagram links only join them | spoofing — [[Handshake v3]] |
| 2026-10-14 | Relay with UDP hole punching; the stream slot stays relayed (UDP-only configs go fully direct) | [[Relay and NAT traversal]] |
| 2026-10-16 | UDP batching without delay, corked receive thread, collapsed acks; linger rejected | [[UDP batching]] |
| 2026-10-16 | NetClient/NetServer split into partial files by concern; shared accept loop, cancellable connects, shared delivery helpers | final review — [[Repository and projects]] |
| 2026-10-17 | Documentation vault in `docs/`, code notes and dependency diagrams generated from the source | this vault — [[Home]] |
