# Kafka Internals — Notes

Consolidated from two research chats: "Deep Dive Into Kafka Internals" and "Understanding Kafka Sparse Indexes."

---

## 1. Core Architecture

Kafka is built around one primitive: the **append-only commit log**. A topic is split into **partitions**, and each partition is an ordered, immutable sequence of messages written to disk. Kafka gets its throughput from three techniques layered on top of this log: OS-level page cache storage, zero-copy networking, and sequential disk I/O.

---

## 2. Storage Engine & On-Disk Layout

Each partition maps to a directory on disk (under `log.dirs`), named `<topic>-<partition-id>`:

```
/tmp/kafka-logs/order-events-0/
├── 00000000000000000000.log              # raw message payload (WAL)
├── 00000000000000000000.index            # offset -> file position
├── 00000000000000000000.timeindex        # timestamp -> offset
└── leader-epoch-checkpoint               # tracks leader transitions
```

- **`.log`** — the actual message data in binary format. Kafka's wire protocol is identical to its on-disk format, so no re-serialization is needed between network and disk.
- **`.index`** — a **sparse index**: instead of recording every message's position, Kafka writes an entry every `log.index.interval.bytes` (default 4096 bytes / 4 KB).
- **`.timeindex`** — a sparse index mapping **timestamps to offsets** (see §2.2).

### 2.1 How the sparse index works (reading offset 1050)

Think of a 1,000-page textbook: a dense index lists every sentence's page; a sparse index only marks chapter starts. Kafka's `.index` is the sparse version.

1. **Binary search the index** for the highest offset ≤ the target. Example index entries:
   - Offset 0 → byte 0
   - Offset 1000 → byte 4096
   - Offset 2000 → byte 8192

   For offset 1050, Kafka finds `Offset 1000 → byte 4096`.
2. **Sequential scan** — Kafka jumps to byte 4096 in the `.log` file and reads forward message-by-message (1000, 1001, 1002, …) until it reaches offset 1050.

Why sparse: a dense index over billions of messages would be too large to fit in memory. Keeping checkpoints every 4 KB keeps the index small enough to live entirely in the page cache, so binary search is fast and the sequential scan afterward is cheap.

### 2.2 The `.timeindex` file

Maps **timestamp → offset** the same way `.index` maps offset → byte position, using the same sparse-entry interval.

**Problem it solves:** if you want "all messages since 1:30 PM," you'd otherwise have to scan from offset 0 checking timestamps one by one.

**Lookup process:**
1. Binary search `.timeindex` for the timestamp → get the nearest offset (e.g., 1:30 PM → offset 5000).
2. Hand that offset to the normal `.index` file to get the exact byte position, then start streaming.

**Used for:**
- Time-based consumer resets (`KafkaConsumer.offsetsForTimes()`, or `--to-datetime` in CLI tools).
- Time-based retention (`log.retention.hours`) — Kafka checks a segment's largest timestamp via `.timeindex` without opening the full `.log` file.

### 2.3 Segment rolling

A partition isn't one giant file — it's a series of **segments**. Only one segment is **active** (open for writes) at a time; the rest are closed/read-only.

Kafka rolls (closes the active segment, opens a new one) when **either** threshold hits first:

| Trigger | Config | Default |
|---|---|---|
| Size | `log.segment.bytes` | 1 GB |
| Time | `log.roll.hours` | 168 hours (7 days) |

**Why segments instead of one file:**
- **Retention/cleanup:** Kafka deletes or compacts whole closed segment files in one fast filesystem op. It cannot delete individual messages from the middle of an active file.
- **Faster lookups:** smaller segments mean smaller per-segment index files that stay in RAM. Kafka first identifies which segment holds an offset, then does the sparse-index search only within that segment.
- **Safety:** closed files can't be corrupted by in-flight writes and are easier for the OS page cache to manage.

### 2.4 Segment compaction & cleanup

A background thread (`kafka-log-cleaner-thread`) enforces retention per `cleanup.policy`:
- **`delete`** — purges segments whose max timestamp or size exceeds `log.retention.hours` / `log.retention.bytes`.
- **`compact`** — keeps only the latest record per key within the partition, discarding older versions of that key.

---

## 3. High-Throughput Architecture

### 3.1 Zero-copy data transfer

**The traditional path** for serving a file over the network involves 4 context switches and 3 CPU copies:

```
Disk → Page Cache → JVM Buffer → Socket Buffer → NIC Buffer → Network
```

Each arrow is a copy: disk to kernel memory, kernel to app (user-space) memory, app back to kernel memory, kernel to network card.

**Kafka's zero-copy path** uses the Linux `sendfile()` syscall — 2 context switches, 0 CPU copies:

```
Disk → Page Cache ───────────────────────────► NIC Buffer → Network
                    (DMA, hardware-driven)
```

Kafka tells the OS "send this file straight to the network interface." The data moves from page cache to NIC buffer via **DMA (Direct Memory Access)** — hardware handles the copy, not the CPU, and it never enters application (JVM) memory at all.

### 3.2 Page cache over JVM heap

Kafka deliberately avoids storing message data in the JVM heap.

- **Problem with the heap:** Java's Garbage Collector has to periodically pause the application ("stop-the-world" pauses) to reclaim memory. With gigabytes of message data in the heap, these pauses cause real latency spikes.
- **Kafka's approach:** write directly to the OS **page cache** (kernel memory), which the GC doesn't know exists and never scans. Dirty pages are flushed to disk sequentially by OS background threads. Data also survives a broker process restart because it's cached at the OS level, not inside the JVM process.

### 3.3 Sequential I/O

Disk seeks (jumping to random locations) are slow; sequential writes/reads approach RAM bandwidth. Kafka **only appends** to the end of the active segment — it never does random-access writes — which is why it can sustain very high throughput on ordinary disks.

### 3.4 Why JVM matters here / language alternatives

Kafka's core is written in **Scala and Java**, both of which run on the **JVM (Java Virtual Machine)** — the runtime that executes compiled bytecode and provides automatic memory management (Garbage Collection). This is exactly why Kafka has to work around heap/GC behavior as described above.

If Kafka were written in a different language, the memory story would change:
- **Other GC'd languages** (Go, C#, Node.js, Python) would face the same class of GC-pause problem if they buffered large amounts of streaming data in application memory.
- **Non-GC'd languages** (C, C++, Rust) manage memory manually (`malloc`/`free` in C/C++, ownership rules at compile time in Rust) and have no GC pauses at all.

**Real-world example:** **Redpanda** is a Kafka-API-compatible broker written from scratch in **C++**. It has no JVM and no garbage collector, manages memory directly at the kernel/hardware level, and avoids GC latency spikes entirely — it doesn't need the same page-cache workaround because it never had a JVM heap problem to begin with.

---

## 4. Network Threading Model

Kafka uses Java NIO selectors to handle many concurrent client connections with a small thread pool:

```
Clients (Producers / Consumers)
        │
        ▼
  Acceptor Thread          (accepts new TCP connections)
        │  round-robin
        ▼
  Network Threads (N)      (num.network.threads — read/write raw bytes,
        │                   deserialize protocol frames)
        │  request queue
        ▼
  I/O Threads (M)          (num.io.threads — execute logic: append to
                             log segment, read from page cache, etc.)
```

- **Acceptor Thread:** listens on the socket, hands off new connections to Network Threads round-robin.
- **Network Threads:** read raw network bytes, deserialize into Kafka protocol requests, push onto a shared request queue, and write responses back.
- **I/O Threads:** pull requests off the queue and do the actual work (disk reads/writes, page cache access).

---

## 5. Cluster & Broker-Level Partition Management

### 5.1 What a Kafka cluster stores/manages

A cluster is a group of **brokers** acting as one distributed system. It manages two categories of state:

**A. Payload data**
- Topics & partitions (log segment files spread across brokers)
- Replicas (redundant partition copies for availability)
- Consumer group offsets (stored in the internal `__consumer_offsets` topic)

**B. Control-plane metadata**
- Cluster topology (broker IDs, IPs, ports, rack info)
- Partition map (which broker leads/follows each partition)
- ISR lists
- Topic configs, ACLs, quotas
- Cluster ID (immutable, assigned at cluster init)

### 5.2 Broker-level partition internals

Each partition assigned to a broker is a directory under `log.dirs` containing multiple log segments (one active, rest read-only) — see §2.

In the broker's JVM memory, each partition is represented by a `Partition` object tracking:
- **LEO (Log End Offset):** offset of the next record to be written locally.
- **HW (High Watermark):** highest offset replicated to all ISR members; data below HW is committed and safe to read.
- **ISR set:** which replica broker IDs are currently alive and caught up.
- **Replica state tracking map:** the leader's local view of each follower's LEO.

### 5.3 Operational responsibilities handled automatically

| Task | How it's handled |
|---|---|
| Broker registration | New broker registers `broker.id` + heartbeat with the active controller; missed heartbeats mark it dead |
| Leader election | If a partition-leader broker dies, the controller picks a new leader from that partition's ISR |
| Partition rebalancing | Admins can trigger reassignment to redistribute load across brokers |
| Consumer group coordination | One broker acts as Group Coordinator per group, handling membership/rebalances/offset commits |
| Data retention & expiry | Each broker's background threads purge/compact expired segments independently |

---

## 6. Replication & High Availability

Each partition has exactly **one Leader** (handles all reads/writes) and zero-or-more **Follower** replicas that stay in sync by pulling data.

### 6.1 Key terms

- **LEO (Log End Offset):** offset of the next record to be written to a given replica's local log.
- **HW (High Watermark):** highest offset replicated across the entire ISR set. Only data at/below HW is visible to consumers.
- **ISR (In-Sync Replicas):** replicas currently caught up with the leader within `replica.lag.time.max.ms`.

### 6.2 Producer durability (`acks`)

| Setting | Guarantee |
|---|---|
| `acks=0` | Returns as soon as the packet is sent — no durability guarantee |
| `acks=1` | Waits for the leader to append locally |
| `acks=-1` / `acks=all` | Waits until the leader **and** all current ISR replicas have updated their HW |

### 6.3 How replication actually happens (pull-based)

Kafka does **not** push from leader to followers. Followers pull, exactly like consumers, via a background `ReplicaFetcherThread`.

```
NODE 1 (Leader)                                  NODE 2 (Follower)
1. Producer writes batch → local log             2. Fetcher sends FetchRequest
   appended → LEO 100→105                            "I'm at offset 100, give me more"
4. Leader updates Node2_LEO = 105  ◄────────────  3. Leader returns records 100-105
5. Leader advances HW to 105                          + current HW; follower appends,
   → ACKs producer (if acks=all)                      advances its own LEO to 105
                                                   6. Updates local HW from leader's
                                                      response header
```

Step by step:
1. **Producer append:** producer locates the partition leader, sends the batch; leader appends to its active segment (via page cache) and bumps its LEO.
2. **Follower fetch:** the follower's `ReplicaFetcherThread` requests data starting at its own last-known offset. By receiving this request, the leader learns the follower's LEO.
3. **Leader responds:** leader reads the requested range from page cache, returns it plus its current HW. Follower appends the batch and advances its own LEO.
4. **HW advances:** once all ISR members' LEOs reach a given offset, the leader advances its HW to that offset — the range becomes **committed**. If `acks=all`, the producer is ACKed now.

### 6.4 ISR shrinking/expansion

- **Shrink:** if a follower doesn't send a fetch request within `replica.lag.time.max.ms` (GC pause, network partition, crash), the leader drops it from the ISR.
- **Expand:** once a lagging replica catches up to the leader's LEO, it's added back to the ISR.

### 6.5 Leader epochs (preventing data divergence)

Relying only on the High Watermark for crash recovery could cause a recovering follower to truncate its log incorrectly and diverge from the leader. Modern Kafka uses **Leader Epochs**:

- Every leader change increments an epoch number, recorded in each broker's local `leader-epoch-checkpoint` file.
- On recovery, a follower asks the new leader "what was the end offset of epoch X?" instead of blindly truncating to its old HW — it only truncates to the point where its epoch history matches the leader's, preventing silent data loss.

---

## 7. Consensus & Metadata Management (KRaft vs. ZooKeeper)

### 7.1 Why a separate control plane is needed at all

Brokers handle the **data plane** (storing/serving messages). But some decisions need a single, globally-agreed answer, which brokers cannot safely decide among themselves:

- **Who is the partition leader?** If brokers elected leaders independently, two brokers could each believe they're the leader simultaneously — a **split-brain** scenario causing data corruption.
- **What does the cluster look like?** Which broker owns which partition, which brokers are alive, what are the topic configs — this needs a centralized, consistent registry.

This is why Kafka needs a dedicated **control-plane consensus system** — historically ZooKeeper, now KRaft.

### 7.2 ZooKeeper era (legacy, Kafka < 3.0)

- ZooKeeper ran as a fully separate application/cluster (its own process, config, ports).
- ZooKeeper elected one Kafka broker as "Active Controller"; that broker then pushed metadata updates out to every other broker.
- **Problems:** two systems to operate; slow multi-hop synchronization; in large clusters, a controller crash could require **15–30 minutes** to fully resynchronize metadata from ZooKeeper.

### 7.3 KRaft era (modern, Kafka 3.3+/4.0+)

ZooKeeper is removed entirely. Kafka implements its own control plane using the **Raft consensus algorithm**.

- A subset of brokers are designated **Controllers**, forming a **quorum**.
- All administrative decisions (topic creation, partition reassignment, ACL changes, leader elections) are written as events into an internal, replicated **`@metadata`** log/topic.
- Every broker consumes this `@metadata` stream and keeps an up-to-date **in-memory** view of the cluster — no external watcher round-trips needed.

**Why KRaft is better:** one unified process (no separate ZooKeeper to install/secure), controller failover in milliseconds instead of minutes (state is already replicated and warm in memory), and it scales to millions of partitions.

### 7.4 Controller failover walkthrough

```
[ Active Controller Leader Crashes ]
        │
        ▼
1. Heartbeats stop (timeout detected by standby controllers)
        │
        ▼
2. A standby increments its term/epoch, becomes Candidate,
   sends VoteRequest to the other controllers
        │
        ▼
3. Once it gets a majority (quorum) of votes, it becomes
   the new Controller Leader
        │
        ▼
4. It publishes the new metadata epoch; all brokers redirect
   admin requests to it
```

**What keeps working during this:** producing and consuming data — clients talk to **Partition Leaders** on the data plane, not the Controller, and data brokers already have cluster metadata cached locally.

**What's briefly paused:** creating/deleting/altering topics, ACL/config changes, and electing *new* partition leaders (only if a data broker also happens to crash during the same window).

### 7.5 Where KRaft/ZooKeeper physically run

Neither runs as a single instance on one broker — consensus requires a quorum across multiple nodes.

- **KRaft — combined mode** (typical for small/medium clusters): every broker runs both roles in the same process (`process.roles=broker,controller`). All nodes serve client traffic *and* participate in the metadata quorum.
- **KRaft — isolated mode** (typical for large/enterprise clusters): dedicated controller-only nodes (`process.roles=controller`) run the metadata quorum separately from broker-only nodes (`process.roles=broker`) that just serve data.
- **ZooKeeper (legacy):** a fully separate ZooKeeper ensemble (typically 3 or 5 instances), which can be co-located on the same physical machines as the Kafka brokers or run on entirely separate servers — but it is always a distinct daemon process that Kafka connects to over TCP.

### 7.6 Leadership is per-partition, not per-broker

A common misconception: in a 3-broker cluster, people assume one broker "leads" and the other two just follow. In reality, **leadership is assigned per partition**, and it's spread across brokers to balance load. Example — topic `orders`, 3 partitions, replication factor 3:

| | Partition 0 | Partition 1 | Partition 2 |
|---|---|---|---|
| Broker 1 | **Leader** | Follower | Follower |
| Broker 2 | Follower | **Leader** | Follower |
| Broker 3 | Follower | Follower | **Leader** |

This way all 3 brokers actively handle reads/writes instead of one broker taking 100% of the traffic while the others sit idle. Separately, exactly **one** broker also holds the **Controller Leader** role for metadata coordination — that's a cluster-wide, not per-partition, role.

---

## 8. Message Lifecycle — Producer Pipeline

`producer.send(record)` doesn't hit the network immediately — it goes through three stages:

```
Message Arrives
      │
 Has a key?
   /      \
 YES        NO
  │          │
Key hashing   Sticky / round-robin:
murmur2(key)  fill one partition's batch
% partitions  fully before moving to the next
(ordering
 guaranteed)
```

### 8.1 Partitioning
- **With a key** (e.g. `user_id: 12345`): Kafka hashes it via **Murmur2** — `partition = murmur2(key) % total_partitions`. Same key always → same partition, which is what gives per-key ordering guarantees.
- **Without a key:** ordering doesn't matter, so Kafka uses **Sticky Partitioning** — it fills one partition's batch completely before moving to the next, rather than round-robining message-by-message. This produces larger, more efficient batches with less fragmentation.

### 8.2 Record batching
Messages for the same partition go into an in-memory buffer (`buffer.memory`, default 32 MB), organized as one `RecordBatch` per target partition:

```
Producer buffer (buffer.memory = 32MB)
├── Partition 0 batch [ Msg1 | Msg2 | Msg3 ... ] → compressed (LZ4/ZSTD/Snappy/GZIP)
├── Partition 1 batch [ Msg1 | Msg2 ... ]
└── Partition 2 batch [ Msg1 | Msg2 | Msg3 | Msg4 ... ]
```

**Compression happens on the whole batch, not per message:**
- Repetitive structural data (JSON keys like `"user_id"`, `"timestamp"`) compresses far better in bulk — often 50–80% reduction — than compressing each small message individually.
- The broker writes the compressed bytes straight to disk **without decompressing them**; decompression only happens on the consumer side.

### 8.3 Dispatch
A background **Sender Thread** flushes a partition's batch to its leader broker as soon as **either** threshold is hit:

| Threshold | Config | Default | Effect |
|---|---|---|---|
| Size | `batch.size` | 16 KB | Sends as soon as the buffer fills, protecting memory under load |
| Time | `linger.ms` | 0 ms (send immediately) | Waiting a few ms (e.g. 5–20) lets more messages join the batch, trading a little latency for much higher throughput |

**Producer stage summary**

| Stage | Job | Key config |
|---|---|---|
| Partitioning | Route message to a partition | `murmur2(key)` or sticky partitioner |
| Batching | Buffer + compress in memory | `buffer.memory`, `compression.type` |
| Dispatch | Send over network | `batch.size` (16 KB) vs `linger.ms` (0–20ms) |

---

## 9. Message Lifecycle — Consumer Side

Kafka is **pull-based**: unlike broker-push systems (e.g. RabbitMQ), consumers actively poll for data and track their own position.

### 9.1 The poll loop

```
consumer.poll(Duration.ofMillis(100))
        │ fetch batch
        ▼
Process messages (application logic)
        │ commit offset
        ▼
Update committed offset on broker
```

1. **Fetch request:** consumer asks the leader broker for messages starting at a given offset.
2. **Fetch response:** broker returns a batch of (still-compressed) messages straight from page cache.
3. **Decompression:** happens **client-side** — the broker never spends CPU decompressing.

### 9.2 Consumer groups & partition assignment

Consumers share a `group.id` and belong to a **Consumer Group**.

- Each partition is assigned to **exactly one** consumer instance within a group at a time.
- 4 partitions + 4 consumers in a group → each consumer handles 1 partition in parallel.
- **Rebalancing:** if a consumer joins or dies, a coordinator reassigns partitions among the remaining consumers so processing continues without data loss. (The broker-side `Deep Dive` chat notes modern Kafka uses the **cooperative incremental** protocol — `CooperativeStickyAssignor` — which revokes only the affected partitions rather than pausing every consumer in the group.)

### 9.3 Offset tracking & commits

Each consumer tracks, per partition:
- **Current fetch offset** — what it'll request next.
- **Committed offset** — the last offset it has confirmed processed, saved to the internal `__consumer_offsets` topic.

**Commit modes:**
- **Automatic** (`enable.auto.commit=true`): commits the latest polled offset on a timer (`auto.commit.interval.ms`, e.g. every 5s). Risk: a crash after auto-commit but before finishing processing loses those messages.
- **Manual** (`enable.auto.commit=false`):
  - `commitSync()` — blocks until the broker confirms the commit; safer, slower.
  - `commitAsync()` — fire-and-forget; faster, needs your own failure handling.

**Delivery guarantees**, depending on when you commit relative to processing:

| Guarantee | How | Trade-off |
|---|---|---|
| At-Most-Once | Commit **before** processing | No duplicates, but can lose messages if the consumer crashes mid-processing |
| At-Least-Once (default) | Commit **after** processing | No data loss, but can reprocess (duplicate) if it crashes before committing |
| Exactly-Once (EOS) | Two-phase transactional commit spanning producer + broker + read-process-write loop | No loss, no duplicates, more overhead |

### 9.4 Single-message vs. batch handler mode

Kafka **always** fetches in batches over the wire — but whether your application code sees one message at a time or the whole batch depends on the client framework's listener mode:

```
Broker (sends batch over network)
        │
        ▼
Client library receives a list of messages
        │
        ├── [Default] Library loops internally
        │       └── calls your handler ONCE per message
        │
        └── [Batch mode] Library skips the loop
                └── calls your handler ONCE per entire batch
```

Example (Spring Kafka):
```java
// Default: called once per message, even though 500 arrived in one poll
@KafkaListener(topics = "orders")
public void listen(String record) { processOrder(record); }

// Batch mode: called once for the whole batch
@KafkaListener(topics = "orders")
public void listen(List<ConsumerRecord<String, String>> records) {
    bulkInsertToDatabase(records);
}
```

**Why use batch mode:** lets you do one bulk `INSERT` instead of 500 individual queries, cuts per-record function-call overhead, and improves throughput on high-volume topics.

---

## 10. Quick Reference / Glossary

| Term | Meaning |
|---|---|
| Partition | An ordered, immutable, append-only log; the unit of parallelism in Kafka |
| Segment | A fixed-size (or time-boxed) chunk of a partition's log on disk; only one is active at a time |
| `.log` / `.index` / `.timeindex` | Message data / offset→byte sparse index / timestamp→offset sparse index |
| LEO | Log End Offset — next offset to be written on a given replica |
| HW | High Watermark — highest offset replicated to the whole ISR; the read boundary for consumers |
| ISR | In-Sync Replicas — replicas currently caught up with the leader |
| Leader epoch | Monotonic counter incremented on each leader change; prevents log divergence during failover |
| KRaft | Kafka's built-in Raft-based metadata/control-plane consensus system, replacing ZooKeeper |
| `@metadata` topic | Internal replicated log KRaft controllers use to store cluster metadata as events |
| `__consumer_offsets` | Internal compacted topic storing every consumer group's committed offsets |
| Zero-copy (`sendfile`) | Broker sends page-cache data straight to the NIC via DMA, skipping JVM/user-space copies |
| Sticky partitioning | Fills one partition's batch fully before moving to the next, when messages have no key |
