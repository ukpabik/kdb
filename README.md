# KDB

KDB is a fast key-value storage library built upon the LSM tree architecture.

## Features
* Keys and values are byte arrays of arbitrary size (utilizing ByteBuffers for manipulation).
* Data is stored in sorted order, by key.
* Basic Operations include `Get(key)`, `Put(key)`, and `Remove(key)`.
* `Put` requests can be batched into single, atomic operations.

## Installation
```bash
git clone https://github.com/ukpabik/kdb.git
```

## Testing
KDB uses Apache Maven as its build system.

```bash
mvn clean
mvn test
```

## Performance
To run benchmarks on your local machine, use this command below:
```bash
mvn exec:java
```


### Workload Parameters
* **Key Size:** 16 bytes each
* **Value Size:** 100 bytes each
* **Total Operations:** 1,000,000 unique records

### p99 Results
| Operation | Latency          | Throughput |
| :--- |:-----------------|:-----------|
| `fillrandom` | 9.000 micros/op  | 27.0 MB/s  |
| `overwrite` | 9.800 micros/op  | 26.2 MB/s  |
| `readrandom` | 25.400 micros/op | 11.4 MB/s  |
