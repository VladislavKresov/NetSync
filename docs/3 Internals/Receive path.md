---
tags: [internals, diagram]
---

# Receive path

```mermaid
flowchart TD
  s["socket read into the transport's<br/>own pooled buffer"] --> t{"transport"}
  t -- TCP --> tf["TcpFrameReader: many frames per read<br/>fragments → FragmentBuffer"]
  t -- UDP --> tu["Data → one packet<br/>Batch → unpack every packet<br/>(cork while handling the datagram)"]
  t -- "WebSocket / relay" --> tm["MessageLink.HandleMessage"]
  tf & tu & tm --> hook["PooledReceive hook<br/>(zero-copy, valid during the call)"]
  hook --> env{"keyed connection?"}
  env -- yes --> un["LinkSecurity.Unwrap<br/>tag, replay window"]
  env -- no --> sw
  un -- "bad tag / replay" --> drop1["drop"]
  un -- ok --> sw{"peer message type"}
  sw -- "Hello / Welcome / Join / Bye / …" --> ctl["handshake and control"]
  sw -- "RelData / RelAck" --> rel["ReliableEndpoint<br/>dedup, order, deliver, ack"]
  sw -- Data --> dec
  rel --> dec{"channel needs<br/>transform?"}
  dec -- yes --> cd["ChannelTransform.Decode<br/>decrypt → inflate (pooled)"]
  dec -- no --> ev
  cd --> ev{"EventDelivery"}
  ev -- Immediate --> h1["MessageReceived (the buffer)<br/>DataReceived (a copy)"]
  ev -- Polled --> pq["pooled copy in the event queue<br/>→ PollEvents"]
  tu -. "end of datagram" .-> fl["DatagramCompleted →<br/>ReliableEndpoint.FlushAcks"]
```

## Rules

- **Exactly one copy for anything kept**: transports deliver their own buffer; the reliability layer copies only out-of-order packets it must hold; Polled delivery copies into a pooled buffer.
- **Anything invalid is dropped**, never delivered as garbage or thrown into network threads ([[Limits and hardening]]).
- **Order of checks** on keyed connections: envelope first — nothing parses an unauthenticated packet.
- **Replies leave together**: while the UDP receive thread handles a datagram, its own sends (acks, echoes) are corked and the collapsed acks are flushed at the end ([[UDP batching]], [[Reliable UDP]]).

Code: [[TcpFrameReader]], [[UdpTransport]], [[UdpServerTransport]], [[DatagramBatcher]] (`Unpack`), [[MessageLink]], [[TransportCapabilities]] (`ReceivedPacket.Deliver`), [[LinkSecurity]], [[ReliableEndpoint]], [[ChannelTransform]], [[EventDispatcher]], [[NetClient]], [[NetServer]].
