---
tags: [internals, diagram]
---

# Send path

`client.SendAsync(channel, data)` — the server side ([[NetConnection]] → [[NetServer]]) is the same with `SendToLinkAsync`.

```mermaid
flowchart TD
  a["SendAsync(channel, data)"] --> b["NetConfig.ResolveOutgoing<br/>channel exists, size ≤ MaxMessageSize"]
  b --> c{"channel needs<br/>transform?"}
  c -- "compression / encryption" --> d["ChannelTransform.Encode<br/>Deflate → cipher, [flags] prefix<br/>(pooled buffer)"]
  c -- no --> e
  d --> e{"Route by slot"}
  e -- "UDP, mode ≠ Unreliable" --> r["ReliableEndpoint<br/>seq, window, cwnd,<br/>fragments ≥ FragmentSize"]
  e -- "stream slot or UDP Unreliable" --> f["prefix [MsgData][channel]<br/>(cached, no allocation)"]
  r --> g
  f --> g["SendOnLinkAsync / SendToLinkAsync"]
  g --> h{"key exchange?"}
  h -- yes --> i["LinkSecurity.WrapPooled<br/>[MsgSecure][counter][tag][inner]"]
  h -- no --> j
  i --> j{"transport"}
  j -- TCP --> k["frame [Data][len][body]<br/>≥ 64 KB → BulkSender fragments"] --> q["SendQueue<br/>priority, coalesce ≤ 64 KB"] --> sock1["socket write<br/>(drain loop)"]
  j -- UDP --> u["DatagramBatcher.Enqueue<br/>copy into destination's batch"] --> drain["drain loop: one datagram<br/>per batch (≤ 1200 B)"] --> sock2["socket send"]
  j -- "WebSocket / relay stream" --> m["MessageLink → SendQueue<br/>one message per packet"] --> sock3["WebSocket / relay frame"]
```

## Rules this path keeps

- **Caller memory is read only before `SendAsync` returns.** Every copy happens synchronously (into a pooled frame, a batch, a reliable `PendingPacket`); bulk paths copy the rest the moment they would have to wait ([[BulkSender]], [[ReliableEndpoint]]).
- **No allocation per message**: cached prefixes, pooled buffers, struct jobs, `AsyncSignal` wake-ups ([[Pooled]], [[AsyncSignal]], [[SendQueue]], [[DatagramBatcher]]).
- **A lone message is never delayed**: queues wake their drain loop at once; batching only packs what arrives while a socket call is in progress ([[UDP batching]]).
- **Backpressure**: `SendQueue` fails fast for real-time sends when full, waits for bulk; reliable channels wait for window/cwnd; UDP batches are capped at 256 KB per destination (beyond = dropped, like a full socket buffer).

Code: [[NetClient]] (`SendAsync`, `Route`, `SendOnLinkAsync`), [[ChannelTransform]], [[PeerProtocol]] (`DataPrefix`), [[LinkSecurity]], [[PacketProtocol]], [[TcpTransport]], [[UdpTransport]], [[MessageLink]].
