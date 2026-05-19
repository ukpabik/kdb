package com.kdb.storage.engine;

import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PersistentStoreTest {

    private PersistentStore currentStore;
    @AfterEach
    void tearDown() {
        if (currentStore != null) {
            currentStore.close();
        }
    }

    @Test
    void testInitialization(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("init_key".getBytes());
        byte[] value = "init_value".getBytes();
        this.currentStore = (PersistentStore) StorageEngines.createPersistentStore(tempDir);

        currentStore.put(key, value);
        Optional<byte[]> result = currentStore.get(key);

        assertTrue(result.isPresent(), "Store should return the value just inserted");
        assertArrayEquals(value, result.get(), "Returned bytes must match exactly");
    }

    @Test
    void testCrashRecovery(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("user_1".getBytes());
        byte[] value = "Alice".getBytes();

        this.currentStore = (PersistentStore) StorageEngines.createPersistentStore(tempDir);

        this.currentStore.put(key, value);

        try(Store<ByteBuffer, byte[]> store2 = StorageEngines.createPersistentStore(tempDir)) {
            Optional<byte[]> result = store2.get(key);
            assertTrue(result.isPresent(), "Data must survive a system restart");
            assertArrayEquals(value, result.get(), "Recovered data must match the original input perfectly");
        }
    }

    @Test
    void testTombstoneValues(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("dead_key".getBytes());
        byte[] value = "To_Be_Deleted".getBytes();
        this.currentStore = (PersistentStore) StorageEngines.createPersistentStore(tempDir);

        currentStore.put(key, value);
        currentStore.remove(key);

        assertTrue(currentStore.get(key).isEmpty(), "MemTable should immediately hide deleted keys via tombstone");

        try(Store<ByteBuffer, byte[]> store2 = StorageEngines.createPersistentStore(tempDir)){
            assertTrue(store2.get(key).isEmpty(), "Tombstone must be replayed from WAL to keep data deleted post-crash");
        }
    }

    @Test
    void testLogRotation(@TempDir Path tempDir) throws Exception {
        this.currentStore = (PersistentStore) StorageEngines.createPersistentStore(tempDir);

        ByteBuffer key = ByteBuffer.wrap("rotation-test".getBytes());
        byte[] value = "initial-data".getBytes();
        currentStore.put(key, value);

        Path log0 = tempDir.resolve("00000.log");
        assertTrue(Files.exists(log0), "Initial log should exist");
        long sizeBefore = Files.size(log0);
        assertTrue(sizeBefore > 0, "Initial log should have data");

        currentStore.flush();

        Path log1 = tempDir.resolve("00001.log");

        assertFalse(Files.exists(log0), "Old log should be deleted after successful flush");

        assertTrue(Files.exists(log1), "New log should be created after rotation");
        assertEquals(0, Files.size(log1), "New log should be empty initially");

        currentStore.put(ByteBuffer.wrap("new-key".getBytes()), "new-data".getBytes());
        assertTrue(Files.size(log1) > 0, "New log should receive new writes");
    }

    @Test
    void testConcurrentPuts(@TempDir Path tempDir) throws Exception {
        this.currentStore = (PersistentStore) StorageEngines.createPersistentStore(tempDir);
        int threadCount = 20;
        int insertsPerThread = 500;
        int totalInserts = threadCount * insertsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < insertsPerThread; j++) {
                        String keyStr = "key-" + threadId + "-" + j;
                        String valStr = "val-" + threadId + "-" + j;
                        currentStore.put(ByteBuffer.wrap(keyStr.getBytes()), valStr.getBytes());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();

        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Concurrent inserts timed out");
        executor.shutdown();

        int verifiedCount = 0;
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < insertsPerThread; j++) {
                String keyStr = "key-" + i + "-" + j;
                String expectedValStr = "val-" + i + "-" + j;

                Optional<byte[]> val = currentStore.get(ByteBuffer.wrap(keyStr.getBytes()));
                assertTrue(val.isPresent(), "Missing key: " + keyStr);
                assertArrayEquals(expectedValStr.getBytes(), val.get(), "Corrupted value for key: " + keyStr);
                verifiedCount++;
            }
        }

        assertEquals(totalInserts, verifiedCount, "Total verified inserts should match expected");
    }

    @Test
    void testFlushDuringConcurrentWrites(@TempDir Path tempDir) throws Exception {
        this.currentStore = (PersistentStore) StorageEngines.createPersistentStore(tempDir);
        int threadCount = 10;
        int insertsPerThread = 500;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulInserts = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < insertsPerThread; j++) {
                        String keyStr = "concurrent-key-" + threadId + "-" + j;
                        currentStore.put(ByteBuffer.wrap(keyStr.getBytes()), "val".getBytes());
                        successfulInserts.incrementAndGet();

                        if (j % 50 == 0) Thread.sleep(1);
                    }
                } catch (Exception e) {
                    // Ignore for test
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();

        Thread.sleep(50);
        currentStore.flush();

        assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "Test timed out");
        executor.shutdown();

        assertEquals(threadCount * insertsPerThread, successfulInserts.get(), "All inserts should have finished");

        assertTrue(Files.exists(tempDir.resolve("00001.log")), "Log should have rotated");

        long sstCount = Files.list(tempDir).filter(p -> p.toString().endsWith(".sst")).count();
        assertTrue(sstCount > 0, "An SSTable should have been written to disk during the flush");

        assertTrue(currentStore.get(ByteBuffer.wrap("concurrent-key-0-0".getBytes())).isPresent(), "Failed to read early key");
        assertTrue(currentStore.get(ByteBuffer.wrap("concurrent-key-0-499".getBytes())).isPresent(), "Failed to read late key");
    }
}
