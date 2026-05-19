package com.kdb.benchmark;

import com.kdb.storage.Store;
import com.kdb.storage.engine.StorageEngines;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Stream;

public class KDBBenchmark {

    private static final int NUM_OPERATIONS = 1_000_000;
    private static final int KEY_SIZE = 16;
    private static final int VALUE_SIZE = 100;

    private static final int BYTES_PER_OP = KEY_SIZE + VALUE_SIZE;

    public static void main(String[] args) throws Exception {
        System.out.println("Benchmark for KDB Persistent Store");
        System.out.println("Keys:       " + KEY_SIZE + " bytes each");
        System.out.println("Values:     " + VALUE_SIZE + " bytes each");
        System.out.println("Entries:    " + NUM_OPERATIONS);
        System.out.println("--------------------------------------------------");

        byte[] payload = new byte[VALUE_SIZE];
        new Random().nextBytes(payload);

        ByteBuffer[] keys = new ByteBuffer[NUM_OPERATIONS];
        for (int i = 0; i < NUM_OPERATIONS; i++) {
            keys[i] = ByteBuffer.wrap(String.format("key_%012d", i).getBytes());
        }

        shuffleArray(keys);

        System.gc();

        Path dbDirectory = Files.createTempDirectory("kdb_bench_rand_");
        try (Store<ByteBuffer, byte[]> store = StorageEngines.createPersistentStore(dbDirectory)) {

            runP99Benchmark("fillrandom", store, keys, payload, NUM_OPERATIONS, true);

            shuffleArray(keys);
            runP99Benchmark("overwrite", store, keys, payload, NUM_OPERATIONS, true);

            shuffleArray(keys);
            runP99Benchmark("readrandom", store, keys, null, NUM_OPERATIONS, false);
        } finally {
            cleanDirectory(dbDirectory);
        }
        System.exit(0);
    }

    /**
     * Core benchmark runner that executes the workload and formats output
     */
    private static void runBenchmark(String name, Store<ByteBuffer, byte[]> store, ByteBuffer[] keys, byte[] payload, int ops, boolean isWrite) {
        long startTime = System.nanoTime();
        int foundCount = 0;

        for (int i = 0; i < ops; i++) {
            ByteBuffer key = keys[i];
            key.clear();

            if (isWrite) {
                store.put(key, payload);
            } else {
                if (store.get(key).isPresent()) {
                    foundCount++;
                }
            }
        }

        long endTime = System.nanoTime();
        long durationNanos = endTime - startTime;
        double timeSecs = durationNanos / 1_000_000_000.0;

        double microsPerOp = (durationNanos / 1000.0) / ops;
        double mbPerSec = ((double) ops * BYTES_PER_OP / (1024 * 1024)) / timeSecs;

        if (isWrite) {
            System.out.printf("%-12s : %11.3f micros/op; %6.1f MB/s\n", name, microsPerOp, mbPerSec);
        } else {
            System.out.printf("%-12s : %11.3f micros/op; %6.1f MB/s (Found: %,d)\n", name, microsPerOp, mbPerSec, foundCount);
        }
    }

    /**
     * Used to track p99 latencies.
     */
    private static void runP99Benchmark(String name, Store<ByteBuffer, byte[]> store, ByteBuffer[] keys, byte[] payload, int ops, boolean isWrite) {
        long[] latencies = new long[ops];
        long startTime = System.nanoTime();

        for (int i = 0; i < ops; i++) {
            ByteBuffer key = keys[i];
            key.clear();

            long start = System.nanoTime();
            if (isWrite) {
                store.put(key, payload);
            } else {
                store.get(key);
            }
            latencies[i] = System.nanoTime() - start;
        }

        long totalTimeNanos = System.nanoTime() - startTime;
        double timeSecs = totalTimeNanos / 1_000_000_000.0;

        double mbPerSec = ((double) ops * BYTES_PER_OP / (1024 * 1024)) / timeSecs;

        java.util.Arrays.sort(latencies);
        long p99Nanos = latencies[(int) (ops * 0.99)];

        System.out.printf("%-12s : P99= %.3f micros/op; %6.1f MB/s\n", name, p99Nanos / 1000.0, mbPerSec);
    }

    private static void shuffleArray(ByteBuffer[] array) {
        Random rnd = new Random();
        for (int i = array.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            ByteBuffer a = array[index];
            array[index] = array[i];
            array[i] = a;
        }
    }

    private static void cleanDirectory(Path path) throws IOException {
        if (!Files.exists(path)) return;
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}
