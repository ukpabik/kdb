# KDB

KDB is a highly concurrent, persistent, LSM-tree-backed key-value storage engine implemented from scratch in Java.

## Performance Benchmarks


### Workload Parameters
* **Key Size:** 16 bytes each
* **Value Size:** 100 bytes each
* **Total Operations:** 1,000,000 unique records

### Throughput Results
| Operation | Latency per Op | Throughput |
| :--- | :--- | :--- |
| `fillrandom` | 3.848 micros/op | 28.7 MB/s |
| `overwrite` | 4.553 micros/op | 24.3 MB/s |
| `readrandom` | 257.184 micros/op | 0.4 MB/s |