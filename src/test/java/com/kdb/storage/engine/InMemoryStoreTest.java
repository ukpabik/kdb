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

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStoreTest {

    private Store<ByteBuffer, byte[]> store;

    @BeforeEach
    void init() {
       this.store = StorageEngines.createInMemoryStore();
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
    void getConcurrentOperationsS() throws InterruptedException {
        int threadCount = 10;
        int opsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        ByteBuffer sharedKey = ByteBuffer.wrap("shared_key".getBytes());
        byte[] originalValue = "value".getBytes();
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
}