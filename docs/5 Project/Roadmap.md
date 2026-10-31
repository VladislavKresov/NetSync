---
tags: [project]
---

# Roadmap

## Done

Phases 2.0 (stages 0–8: core extraction, transports, peer layer, reliability, security, discovery, Unity adapter), 2.1 (stages 9–14: hardening, protocol v2, factories, congestion control, packet authentication, usability) and 2.2 (stages 15–21: zero allocation, protocol v3, session services, WebSocket and listeners, relay, scale, docs), followed by a full review — see [[Decision log]].

## Next

- [ ] Publish: remotes, repository URL, NuGet id check, API key, tag ([[Releasing]]).
- [ ] Verify in real Unity 2021.2+ (Mono, IL2CPP) and on Android/iOS.
- [ ] Update the Unity adapter (below).
- [ ] Reliable-ordered throughput: ack/data piggybacking ([[Known limitations]]).
- [ ] JavaScript/TypeScript browser client over WebSocket + protocol v3 (separate repo).
- Out of scope for the core: codecs, object replication, RPC frameworks — they belong in packages on top.

## Unity adapter

The adapter (`NetSync.Unity`) was deliberately left untouched in 2.2. It compiles against the new DLL as is (the APIs it uses didn't change), but must ship it: protocol v3 does not talk to rc.2.

Required:
1. Refresh the DLL and bump the package to 2.0.0-rc.3.
2. Test on real Unity and devices: `NetIdentity.IsSupported` / ECDH on IL2CPP, batching, WebSocket.

Recommended:
3. Zero-allocation receive: subscribe to the core's `MessageReceived`, create arrays only when the adapter's `DataReceived` has subscribers; expose `MessageReceived`.
4. Zero-allocation send: check `IsCompletedSuccessfully` before awaiting in its `Observe(ValueTask)`.
5. Inspector security: ECDH + server identity (private key from a secure store, not the scene), trusted keys (base64), fingerprint display.
6. Join: `connectData`, `ConnectionApprover` passthrough, `MaxConnections`, a refusal event with `Rejection`/`Reason`.
7. Services: `ServerTimeMs` for interpolation (PositionSync), `NetQuery` for server lists, discovery `Payload` + `ServerDiscovered`.
8. Relay: host-through-relay mode (show the room code) and join by code.
9. `NetworkSimulation` in the inspector for development builds.
10. Samples: Chat on struct messages; document WebGL as unsupported until the JS client.

Relevant notes: [[Security]], [[Connecting and approval]], [[Session services]], [[Relay and NAT traversal]], [[Typed messages and streams]].
