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

            runBenchmark("fillrandom", store, keys, payload, NUM_OPERATIONS, true);

            shuffleArray(keys);
            runBenchmark("overwrite", store, keys, payload, NUM_OPERATIONS, true);

            shuffleArray(keys);
            runBenchmark("readrandom", store, keys, null, NUM_OPERATIONS, false);

            printFootprint(dbDirectory);
        } finally {
            cleanDirectory(dbDirectory);
        }
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

    private static void printFootprint(Path dbDirectory) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        long usedMemoryBytes = runtime.totalMemory() - runtime.freeMemory();
        long diskSpaceBytes = calculateDirectorySize(dbDirectory);
        long rawDataBytes = (long) NUM_OPERATIONS * BYTES_PER_OP;

        System.out.println("\n--- Footprint Analysis ---");
        System.out.printf("Raw Size:   %.1f MB (estimated)\n", rawDataBytes / (1024.0 * 1024.0));
        System.out.printf("File Size:  %.1f MB (estimated)\n", diskSpaceBytes / (1024.0 * 1024.0));
        System.out.printf("Space Amplification: %.2fx\n", (double) diskSpaceBytes / rawDataBytes);
        System.out.printf("JVM Memory: %.1f MB\n", usedMemoryBytes / (1024.0 * 1024.0));
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

    private static long calculateDirectorySize(Path path) throws IOException {
        if (!Files.exists(path)) return 0;
        try (Stream<Path> stream = Files.walk(path)) {
            return stream.filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();
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
