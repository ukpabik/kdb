package com.kdb.storage.engine;

import com.kdb.storage.Store;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MemTableTest {

    private Store<ByteBuffer, byte[]> store;

    @BeforeEach
    void init() {
       this.store = StorageEngines.createMemTable();
    }

    @Test
    void getForExistingKey() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        byte[] value = "value1".getBytes();

        store.put(key, value);
        Optional<byte[]> result = store.get(key);

        assertTrue(result.isPresent(), "Value is not present in map.");
        assertArrayEquals(value, result.get(), "Result is not equivalent to stored value.");
    }

    @Test
    void getForNonExistingKey() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());

        Optional<byte[]> result = store.get(key);

        assertTrue(result.isEmpty(), "Result contains unexpected value.");
    }

    @Test
    void getForNullKey() {
        assertThrows(NullPointerException.class, () -> {
            Optional<byte[]> result = store.get(null);
        });
    }

    @Test
    void getForNullValue() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        assertThrows(NullPointerException.class, () -> {
            store.put(key, null);
            Optional<byte[]> result = store.get(key);
        });
    }

    @Test
    void putForExistingKey() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key, value1);
        Optional<byte[]> result1 = store.get(key);

        store.put(key, value2);
        Optional<byte[]> result2 = store.get(key);

        assertTrue(result1.isPresent(), "First result is not present.");
        assertTrue(result2.isPresent(), "Second result is not present.");
        assertArrayEquals(result2.get(), value2, "Second put method call did not result in new value.");
    }

    @Test
    void putForNonExistingKey() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        byte[] value1 = "value1".getBytes();

        store.put(key, value1);
        Optional<byte[]> result1 = store.get(key);

        assertTrue(result1.isPresent(), "First result is not present.");
    }

    @Test
    void putForNullKey() {
        byte[] value1 = "value1".getBytes();

        assertThrows(NullPointerException.class, () -> {
            store.put(null, value1);
        });
    }

    @Test
    void putForNullValue() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());

        assertThrows(NullPointerException.class, () -> {
            store.put(key, null);
        });
    }

    @Test
    void removeForExistingKey() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        byte[] value1 = "value1".getBytes();

        store.put(key, value1);
        Optional<byte[]> result1 = store.remove(key);

        assertTrue(result1.isPresent(), "Result is not present.");
        assertArrayEquals(result1.get(), value1, "Removed value does not equal stored value.");
    }

    @Test
    void removeForNonExistingKey() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        Optional<byte[]> result = store.remove(key);

        assertFalse(result.isPresent(), "First result is present when it should not be.");
    }

    @Test
    void removeForNullKey() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        byte[] value1 = "value1".getBytes();

        store.put(key, value1);

        assertThrows(NullPointerException.class, () -> {
            store.remove(null);
        });
    }

    @Test
    void putConcurrentOperations() throws InterruptedException {
        int threadCount = 10;
        int opsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);

        // Submit to executors
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try{
                    latch.await();
                    for (int j = 0; j < opsPerThread; j++) {
                        ByteBuffer key = ByteBuffer.wrap(("key-" + threadId + "-" + j).getBytes());
                        store.put(key, new byte[]{(byte) j});
                    }
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        latch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));


        // Check if all the keys are in the map
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < opsPerThread; j++) {
                ByteBuffer key = ByteBuffer.wrap(("key-" + i + "-" + j).getBytes());
                Optional<byte[]> result = store.get(key);
                assertTrue(result.isPresent(), "Value was not inserted.");
                assertArrayEquals(result.get(), new byte[]{(byte) j}, "Result is not equal to original value.");
            }
        }
    }

    @Test
    void getConcurrentOperations() throws InterruptedException {
        final int threadCount = 10;
        final int opsPerThread = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        final ByteBuffer sharedKey = ByteBuffer.wrap("shared_key".getBytes());
        final byte[] originalValue = "value".getBytes();
        store.put(sharedKey, originalValue);

        // Submit to executors
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                for (int j = 0; j < opsPerThread; j++) {
                    Optional<byte[]> result = store.get(sharedKey);

                    assertTrue(result.isPresent(), "Value is not present in map.");
                    assertArrayEquals(result.get(), originalValue, "Values are not equivalent.");
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void removeConcurrentOperations() throws InterruptedException {
        final ByteBuffer sharedKey = ByteBuffer.wrap("concurrency_test_key".getBytes());
        final byte[] value = "value".getBytes();
        final int threadCount = 10;
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger successfulDeletions = new AtomicInteger(0);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        store.put(sharedKey, value);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    latch.await();
                    Optional<byte[]> result = store.remove(sharedKey);
                    if (result.isPresent()) {
                        successfulDeletions.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            });
        }

        latch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, successfulDeletions.get(), "Only one thread should successfully delete the key");
        assertTrue(store.get(sharedKey).isEmpty(), "Key should be gone after concurrent deletes");
    }
}