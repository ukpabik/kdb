# KDB

KDB is a highly concurrent, persistent, LSM-tree-backed key-value storage engine implemented from scratch in Java.

## Performance Benchmarks


### Workload Parameters
* **Key Size:** 16 bytes each
* **Value Size:** 100 bytes each
* **Total Operations:** 1,000,000 unique records

### Throughput Results
| Operation | Latency per Op 
| :--- | :--- 
| `fillrandom` | 431.363 micros/op 
| `overwrite` | 584.048 micros/op 
| `readrandom` | 310.002 micros/op 