# KDB
## Design Doc #2 – Thread-Safe Persistent Key Value Store

**Abstract:**
KDB requires a performant, thread-safe persistent key-value store capable of handling arbitrary data using byte-based keys and values. This document proposes implementing a generic `Store` interface backed by a `ConcurrentSkipListMap` to ensure highly scalable read and write operations, further optimized with an LSM tree implementation. By using `ByteBuffer` for keys and returning `Optional<byte[]>` for retrievals, the system guarantees accurate object hashing and clear API contracts.

---

## 1. Objectives
*Note: This section includes goals and non-goals.*

### Goals
* Create a performant, thread-safe persistent key-value store that can handle arbitrary data with byte-based keys and values.
* Create an LSM tree implementation with a WAL log to ensure persistence.
* Ensure p99 read latency is <5ms.
* Ensure p99 write latency is <5ms.

### Non-Goals
* Making this key-value store distributed.
* Opting for multiple implementations using different types for the KV store.

---

## 2. Proposed Design
*Note: This section includes any data structures or APIs created.*

### Main Data Structures Used
* `Store<K, V>` interface
* `MemTable<ByteBuffer, byte[]>` class
* `PersistentStore<ByteBuffer, byte[]>` class
* `SSTable` class
* `CompactionManager` class
* `WriteAheadLog` class

### API
**Contracts:** Key and Value must never be null.

* `Optional<byte[]> get(ByteBuffer key)`
    * Returns a value (or empty byte array) if found at a specific key. This should return an `Optional.ofNullable` object. (Handles null value case)
    * If the key is null, we throw a `NullPointerException`.
* `void put(ByteBuffer key, byte[] value)`
    * Places a value at a specific given key.
    * If the key or value is null, we throw a `NullPointerException`.
* `Optional<byte[]> remove(ByteBuffer key)`
    * Removes the mapping for a specified key (or does nothing if the key doesn't exist).
    * Returns the previous value of the key mapping (or nothing if it doesn't exist or the value is null).

---

## 3. Design Tradeoffs and Alternatives
*Note: Include tradeoffs with pros and cons.*

### Tradeoff #1: Opting for `ConcurrentSkipListMap` vs `ConcurrentHashMap` or `HashMap`
* **Pros:** A sorted data structure, which allows us to perform O(log N) searches and merges for the LSM tree.
* **Cons:** O(logN) is slower than O(1), but the sorted attribute benefit outweighs the CPU cost.
* **Other alternatives:**
    * `ConcurrentHashMap`
    * `HashMap`

### Tradeoff #2: Opting for LSM Tree vs B-Tree
* **Pros:** Optimized for writes. Low write amplification by using sequential writes vs random.
* **Cons:** Reads are slower as we might have to check multiple SSTables on disk (in the worst case). Background compaction is slow (merging files) and can affect R/W performance.
* **Other alternatives:**
    * B-Tree (better for read-heavy workloads)

### Tradeoff #3: Byte-based keys and values as opposed to other data types
* **Reason:** Using byte-based keys and values allows for higher-performance reading/writing as we don’t have the overhead from other boxed data types (`String`, `Integer`, etc.). It also gives us more flexibility in what we can store. `ByteBuffer`s do add overhead, but are useful, providing `get` and `put` operations. Also, it allows for byte array comparisons without creating a subclass.
* **Other alternatives:**
    * String-based keys and values (or any other boxed data type): Utilizing a different data type adds overhead (boxing and unboxing). Also, it removes flexibility, because if we want to store any other types of data, we would need to create another Data Store implementation.

### Tradeoff #4: Opting for Size-Tiered Compaction vs. Leveled Compaction
* **Pros:** Significantly easier to implement. Handles high-throughput write bursts well.
* **Cons:** Higher read amplification. Higher temporary disk usage.
* **Other alternatives:**
    * Leveled Compaction (better for read-heavy workloads, but more complex)

---

## 4. Constraints & Scalability

### Constraints
* **Key and Value constraints:**
    * Key: 50 byte maximum
    * Value: 1 MB maximum
* **MemTable constraints:**
    * 4MB maximum → flush cap

### Scalability
* Limited by I/O, due to WAL and SSTable operations. All operations are O(logN).
* Handles concurrent read and write access.

---

## 5. Testing Capabilities
**Tests:**
* Test read, write, and delete happy path
* Test read, write, and delete null key/value
* Test concurrent read, write, and delete ops
* Test OOM
* Test crash recovery
* Test MemTable Flush
* Test compaction
* Test SSTable operations

---

## 6. Future Considerations
For the next implementation, I want to add a distributed architecture. For KDB, we should add support for usage across multiple machines. This is more flexible and allows KDB to be used for large-scale applications.  
Adding a bloom filter to optimize our nonexistent key reads~~~~.