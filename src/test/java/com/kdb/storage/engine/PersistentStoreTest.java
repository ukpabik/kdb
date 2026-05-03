package com.kdb.storage.engine;

import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class PersistentStoreTest {

    @Test
    void testInitialization(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("init_key".getBytes());
        byte[] value = "init_value".getBytes();
        Store<ByteBuffer, byte[]> store = StorageEngines.createPersistentStore(tempDir);

        store.put(key, value);
        Optional<byte[]> result = store.get(key);

        assertTrue(result.isPresent(), "Store should return the value just inserted");
        assertArrayEquals(value, result.get(), "Returned bytes must match exactly");
    }

    @Test
    void testCrashRecovery(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("user_1".getBytes());
        byte[] value = "Alice".getBytes();

        Store<ByteBuffer, byte[]> store = StorageEngines.createPersistentStore(tempDir);

        store.put(key, value);

        Store<ByteBuffer, byte[]> store2 = StorageEngines.createPersistentStore(tempDir);

        Optional<byte[]> result = store2.get(key);
        assertTrue(result.isPresent(), "Data must survive a system restart");
        assertArrayEquals(value, result.get(), "Recovered data must match the original input perfectly");
    }

    @Test
    void testTombstoneValues(@TempDir Path tempDir) throws Exception {
        ByteBuffer key = ByteBuffer.wrap("dead_key".getBytes());
        byte[] value = "To_Be_Deleted".getBytes();
        Store<ByteBuffer, byte[]> store = StorageEngines.createPersistentStore(tempDir);

        store.put(key, value);
        store.remove(key);

        assertTrue(store.get(key).isEmpty(), "MemTable should immediately hide deleted keys via tombstone");

        Store<ByteBuffer, byte[]> store2 = new PersistentStore(tempDir);

        assertTrue(store2.get(key).isEmpty(), "Tombstone must be replayed from WAL to keep data deleted post-crash");
    }

    @Test
    void testWriteAheadLogClear(@TempDir Path tempDir) throws Exception {

        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            byte[] value = "value1".getBytes();
            for (int i = 0; i < 10; i++) {
                ByteBuffer key = ByteBuffer.wrap(String.valueOf(i).getBytes());
                wal.append(OpCode.PUT, key, value);
            }

            long sizeBefore = java.nio.file.Files.size(walPath);
            assertTrue(sizeBefore > 0, "WAL should have data before clear");

            wal.clear();

            long sizeAfter = java.nio.file.Files.size(walPath);
            assertEquals(0, sizeAfter, "WAL should be empty after clear");
        }
    }
}
