# KDB (Kelechi’s Database)
## Design Doc #1 – Thread-Safe In-Memory Key Value Store

**Abstract:**
KDB requires a performant, thread-safe in-memory key-value store capable of handling arbitrary data using byte-based keys and values. This document proposes implementing a generic `Store` interface backed by a `ConcurrentHashMap` to ensure highly scalable, O(1) read and write operations. By using `ByteBuffer` for keys and returning `Optional<byte[]>` for retrievals, the system guarantees accurate object hashing and clear API contracts within the boundaries of volatile JVM heap memory.

---

## 1. Objectives
*Note: This section includes goals and non-goals.*

### Goals
* Create a performant, thread-safe in-memory key-value store that handles arbitrary data with byte-based keys and values.
* Ensure p99 read latency is <1ms.
* Ensure p99 write latency is <1ms.

### Non-Goals
* Making this key-value store distributed.
* Making this key-value store persistent.
* Opting for multiple implementations using different types for the KV store.

---

## 2. Proposed Design
*Note: This section includes any data structures or APIs created.*

### Data Structures Used
* `Store<K, V>` interface
* `InMemoryStore<ByteBuffer, byte[]>` class

### API
**Contracts:** Key and Value must never be null.

* `Optional<byte[]> get(ByteBuffer key)`
    * Returns a value (or empty Optional object) if found at a specific key. This should return an `Optional.ofNullable` object. (Handles null value case).
    * If the key is null, we throw an `IllegalArgumentException`.
* `void put(ByteBuffer key, byte[] value)`
    * Places a value at a specific given key.
    * If the key or value is null, we throw an `IllegalArgumentException`.
* `Optional<byte[]> delete(ByteBuffer key)`
    * Removes the mapping for a specified key (or does nothing if key doesn't exist).
    * Returns the previous value of the key mapping (or nothing if doesn't exist or value is null).

---

## 3. Design Tradeoffs and Alternatives
*Note: Include all tradeoffs with pros and cons.*

### Tradeoff #1: Opting for `ConcurrentHashMap` vs `HashMap`
* **Reason:** `ConcurrentHashMap` is a thread-safe version of the `HashMap` collection found in the Java util package. `ConcurrentHashMap` uses locking mechanisms under the hood to provide atomic operations, preventing any `ConcurrentModificationException`s.
* **Other alternatives:**
    * `ConcurrentSkipListMap`: Thread-safe `TreeMap` implementation. This alternative could be considered if we choose to leverage the sorted nature of this data structure. For now, this is not needed.

### Tradeoff #2: Byte-based keys and values as opposed to other data types
* **Reason:** Using byte-based keys and values allows for higher-performance reading/writing as we don’t have the overhead from other boxed data types (`String`, `Integer`, etc.). It also gives us more flexibility in what we can store. `ByteBuffer`s do add overhead, but are useful, providing `get` and `put` operations. Also, it allows for byte array comparisons without creating a subclass.
* **Other alternatives:**
    * String-based keys and values (or any other boxed data type): Utilizing a different data type adds overhead (boxing and unboxing). Also, it removes flexibility, because if we want to store any other types of data, we would need to create another Data Store implementation.

---

## 4. Constraints & Scalability

### Constraints
* **Memory Bound:** This implementation is constrained to the JVM and available heap memory.
* **Volatility:** 100% of data will be lost in the event of a system crash.

### Scalability
* CPU overhead is strictly bound by our 50-byte key limit, ensuring hash computation remains consistently low.
* **Key and Value Constraints:** * Key: 50 byte maximum
    * Value: 1 MB maximum
* Handles concurrent read and write access.

---

## 5. Testing Capabilities
**Tests:**
* Test read for existing key/value
* Test read for non-existing key/value
* Test concurrent read
* Test write for existing key/value
* Test write for a non-existing key/value
* Test read for null key/value
* Test write for null key/value
* Test concurrent write
* Test OOM (Out of Memory)
* Test put for zero-byte array

---

## 6. Future Considerations
For the next implementation, I want to add persistence. For KDB, we shouldn’t allow for all data to be lost due to system crashes. Instead, we should store this information on disk (using Write-Ahead Logging (WAL) or other methods).