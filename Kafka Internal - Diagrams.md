# Kafka Internals — Diagrams

Companion diagrams to `Kafka Internals - Notes.md`, in Mermaid so they render directly in Markdown-aware tools (GitHub, VS Code + Mermaid extension, Obsidian, Claude). If your viewer doesn't render Mermaid, each block below is still readable as plain text.

---

## Fig. 1 — Cluster topology (3 brokers, 3 partitions, replication factor 3)

Leadership is per **partition**, not per broker — each broker leads one partition and follows the other two. KRaft controllers run a separate metadata quorum alongside the data plane.

```mermaid
flowchart TB
    P1(["Producer"]):::client
    P2(["Producer"]):::client

    subgraph BK1["Broker 1"]
        direction TB
        A1["P0 · LEADER"]:::leader
        A2["P1 · follower"]:::follower
        A3["P2 · follower"]:::follower
    end

    subgraph BK2["Broker 2"]
        direction TB
        B1["P0 · follower"]:::follower
        B2["P1 · LEADER"]:::leader
        B3["P2 · follower"]:::follower
    end

    subgraph BK3["Broker 3"]
        direction TB
        C1["P0 · follower"]:::follower
        C2["P1 · follower"]:::follower
        C3["P2 · LEADER"]:::leader
    end

    subgraph KRAFT["KRaft Controller Quorum (@metadata)"]
        direction LR
        K1["Controller"]
        K2["Controller · LEADER"]:::leader
        K3["Controller"]
    end

    CA(["Consumer A"]):::client
    CB(["Consumer B"]):::client
    CC(["Consumer C"]):::client

    P1 -->|writes| BK1
    P2 -->|writes| BK2

    KRAFT -.metadata.-> BK1
    KRAFT -.metadata.-> BK2
    KRAFT -.metadata.-> BK3

    BK1 -->|pull, ≤ HW| CA
    BK2 -->|pull, ≤ HW| CB
    BK3 -->|pull, ≤ HW| CC

    classDef leader fill:#f5a35c,stroke:#b8621f,color:#3a2410,font-weight:bold;
    classDef follower fill:#dce7f2,stroke:#3f5f80,color:#27455f;
    classDef client stroke-dasharray:4 3;
```

---

## Fig. 2 — Partition storage & the sparse index

A segment directory holds `.log`, `.index`, `.timeindex`, and `leader-epoch-checkpoint`. The `.index` file only records a position every 4 KB (`log.index.interval.bytes`), so reading offset **1050** means a binary search to the nearest marker, then a short sequential scan.

| Offset | Byte position |
|---|---|
| 0 | 0 |
| 1000 | 4096 |
| 2000 | 8192 |

```mermaid
flowchart LR
    Q["Target: offset 1050"] --> BS["Binary search .index\n(sparse, one entry / 4KB)"]
    BS --> M["Nearest marker ≤ target:\noffset 1000 → byte 4096"]
    M --> SEEK["Seek to byte 4096 in .log"]
    SEEK --> SCAN["Sequential scan\n1000, 1001, 1002 ... 1050"]
    SCAN --> HIT(["Offset 1050 found"])
```

---

## Fig. 3 — Serving a read: traditional copy vs. zero-copy

Kafka serves consumer fetches with the Linux `sendfile()` syscall, moving bytes from page cache straight to the NIC via DMA — skipping the JVM entirely.

**Traditional file transfer — 3 CPU copies, 4 context switches**

```mermaid
flowchart LR
    D1["Disk"] --> PC1["Page Cache"] --> JB["JVM Buffer"] --> SB["Socket Buffer"] --> NB1["NIC Buffer"] --> N1["Network"]
```

**Kafka `sendfile()` zero-copy — 0 CPU copies, 2 context switches**

```mermaid
flowchart LR
    D2["Disk"] --> PC2["Page Cache"] -->|DMA| NB2["NIC Buffer"] --> N2["Network"]
```

---

## Fig. 4 — Producer pipeline: partition, batch, dispatch

A message is routed, buffered into a per-partition batch, compressed, then flushed to the leader once either threshold trips.

```mermaid
flowchart TD
    MSG["Message arrives"] --> KEYQ{"Has a key?"}
    KEYQ -->|Yes| HASH["murmur2(key) % partitions\n(ordering preserved)"]
    KEYQ -->|No| STICKY["Sticky partitioner\n(fills one batch before moving on)"]
    HASH --> BUF["Per-partition batch buffer\n(buffer.memory)"]
    STICKY --> BUF
    BUF --> COMPRESS["Compress batch — LZ4 / ZSTD / Snappy"]
    COMPRESS --> GATE{"batch.size reached\nOR linger.ms elapsed?"}
    GATE -->|Yes| SEND(["Sender thread → Partition Leader"])
```

---

## Fig. 5 — Replication: the pull-based fetch cycle

Followers pull from the leader like ordinary consumers. The leader learns a follower's progress from the fetch request itself, then advances the High Watermark once the whole ISR has caught up.

```mermaid
sequenceDiagram
    participant Producer
    participant Leader as Node 1 (Leader)
    participant Follower as Node 2 (Follower)

    Producer->>Leader: Write batch
    Leader->>Leader: Append to log, LEO 100 → 105
    Follower->>Leader: FetchRequest (LEO=100)
    Leader->>Leader: Learns Node2_LEO = 100
    Leader-->>Follower: Records 100-105 + current HW
    Follower->>Follower: Append batch, LEO 100 → 105
    Follower->>Leader: FetchRequest (LEO=105)
    Leader->>Leader: Node2_LEO = 105 → advance HW to 105
    Leader-->>Producer: ACK (if acks=all)
```

---

## Fig. 6 — Consumer group: poll loop & partition assignment

Each partition is owned by exactly one consumer in a group. A consumer dropping out or joining triggers a rebalance of ownership across the rest.

```mermaid
flowchart TD
    POLL["consumer.poll(100ms)"] --> PROC["Process records"]
    PROC --> COMMIT["Commit offset to __consumer_offsets"]
    COMMIT --> POLL
```

**Topic: 4 partitions · Group: 4 consumers**

| Partition | Assigned consumer |
|---|---|
| P0 | Consumer A |
| P1 | Consumer B |
| P2 | Consumer C |
| P3 | Consumer D |

If Consumer C drops out, the group coordinator reassigns P2 to a remaining consumer — cooperative incremental rebalancing (`CooperativeStickyAssignor`) revokes only the affected partition, not the whole group.
