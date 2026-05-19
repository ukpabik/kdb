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

### Throughput Results
| Operation | Latency         | Throughput |
| :--- |:----------------|:-----------|
| `fillrandom` | 3.978 micros/op | 27.8 MB/s  |
| `overwrite` | 3.959 micros/op | 27.9 MB/s  |
| `readrandom` | 3.555 micros/op | 31.1 MB/s  |