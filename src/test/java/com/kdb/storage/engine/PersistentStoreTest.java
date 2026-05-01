package com.kdb.storage.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class PersistentStoreTest {

    @Test
    void testInitialization(@TempDir Path tempDir) throws Exception {
        PersistentStore store = new PersistentStore(tempDir);
        ByteBuffer key = ByteBuffer.wrap("init_key".getBytes());
        byte[] value = "init_value".getBytes();

        store.put(key, value);
        Optional<byte[]> result = store.get(key);

        assertTrue(result.isPresent(), "Store should return the value just inserted");
        assertArrayEquals(value, result.get(), "Returned bytes must match exactly");
    }

    @Test
    void testCrashRecovery(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("user_1".getBytes());
        byte[] value = "Alice".getBytes();

        PersistentStore store1 = new PersistentStore(tempDir);
        store1.put(key, value);

        PersistentStore store2 = new PersistentStore(tempDir);

        Optional<byte[]> result = store2.get(key);
        assertTrue(result.isPresent(), "Data must survive a system restart");
        assertArrayEquals(value, result.get(), "Recovered data must match the original input perfectly");
    }

    @Test
    void testTombstoneValues(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("dead_key".getBytes());
        byte[] value = "To_Be_Deleted".getBytes();

        PersistentStore store1 = new PersistentStore(tempDir);
        store1.put(key, value);
        store1.remove(key);

        assertTrue(store1.get(key).isEmpty(), "MemTable should immediately hide deleted keys via tombstone");

        PersistentStore store2 = new PersistentStore(tempDir);

        assertTrue(store2.get(key).isEmpty(), "Tombstone must be replayed from WAL to keep data deleted post-crash");
    }
}
