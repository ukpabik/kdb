package com.kdb.storage.engine;

import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
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
    void testLogRotation(@TempDir Path tempDir) throws Exception {
        PersistentStore store = new PersistentStore(tempDir);

        ByteBuffer key = ByteBuffer.wrap("rotation-test".getBytes());
        byte[] value = "initial-data".getBytes();
        store.put(key, value);

        Path log0 = tempDir.resolve("00000.log");
        assertTrue(Files.exists(log0), "Initial log should exist");
        long sizeBefore = Files.size(log0);
        assertTrue(sizeBefore > 0, "Initial log should have data");

        store.flush();

        Path log1 = tempDir.resolve("00001.log");

        assertFalse(Files.exists(log0), "Old log should be deleted after successful flush");

        assertTrue(Files.exists(log1), "New log should be created after rotation");
        assertEquals(0, Files.size(log1), "New log should be empty initially");

        store.put(ByteBuffer.wrap("new-key".getBytes()), "new-data".getBytes());
        assertTrue(Files.size(log1) > 0, "New log should receive new writes");
    }
}
