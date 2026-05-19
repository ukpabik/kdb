package com.kdb.storage.engine;

import com.kdb.storage.common.OpCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WriteAheadLogTest {

    @Test
    void testPutAndReplay(@TempDir Path tempDir) throws Exception {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(OpCode.PUT, ByteBuffer.wrap("user_1".getBytes()), "kelechi".getBytes());
        }

        List<String> replayedPuts = new ArrayList<>();
        List<String> replayedRemoves = new ArrayList<>();

        try (WriteAheadLog recoveredWal = new WriteAheadLog(walPath)) {
            recoveredWal.replay(
                    (key, value) -> {
                        byte[] keyBytes = new byte[key.remaining()];
                        key.duplicate().get(keyBytes);
                        replayedPuts.add(new String(keyBytes) + "=" + new String(value));
                    },
                    (key) -> {
                        byte[] keyBytes = new byte[key.remaining()];
                        key.duplicate().get(keyBytes);
                        replayedRemoves.add(new String(keyBytes));
                    }
            );
        }

        assertEquals(1, replayedPuts.size(), "Should have replayed exactly one PUT");
        assertEquals("user_1=kelechi", replayedPuts.getFirst());
        assertTrue(replayedRemoves.isEmpty(), "Should not have replayed any REMOVEs");
    }

    @Test
    void testSequentialOperationsAndDeletes(@TempDir Path tempDir) throws Exception {
        Path walPath = tempDir.resolve("wal.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(OpCode.PUT, ByteBuffer.wrap("key1".getBytes()), "val1".getBytes());
            wal.append(OpCode.DELETE, ByteBuffer.wrap("key1".getBytes()), new byte[0]);
            wal.append(OpCode.PUT, ByteBuffer.wrap("key2".getBytes()), "val2".getBytes());
        }

        List<String> replayLog = new ArrayList<>();

        try (WriteAheadLog recoveredWal = new WriteAheadLog(walPath)) {
            recoveredWal.replay(
                    (key, value) -> {
                        byte[] keyBytes = new byte[key.remaining()];
                        key.duplicate().get(keyBytes);
                        replayLog.add("PUT:" + new String(keyBytes) + "=" + new String(value));
                    },
                    (key) -> {
                        byte[] keyBytes = new byte[key.remaining()];
                        key.duplicate().get(keyBytes);
                        replayLog.add("DEL:" + new String(keyBytes));
                    }
            );
        }

        assertEquals(3, replayLog.size(), "Must replay all 3 operations");
        assertEquals("PUT:key1=val1", replayLog.get(0));
        assertEquals("DEL:key1", replayLog.get(1), "DELETE operation must be correctly identified and parsed");
        assertEquals("PUT:key2=val2", replayLog.get(2));
    }

    @Test
    void testReplayOnNonExistentFileDoesNotCrash(@TempDir Path tempDir) throws Exception {
        Path walPath = tempDir.resolve("missing_wal.log");

        List<String> replayLog = new ArrayList<>();

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            assertDoesNotThrow(() -> wal.replay(
                    (k, v) -> replayLog.add("PUT"),
                    (k) -> replayLog.add("DEL")
            ), "Replaying a missing WAL on first boot must not crash the database");
        }

        assertTrue(replayLog.isEmpty(), "Nothing should have been replayed");
    }

    @Test
    void testAppendCreatesFileIfMissing(@TempDir Path tempDir) throws Exception {
        Path walPath = tempDir.resolve("new_wal.log");

        assertFalse(Files.exists(walPath), "File should not exist yet");

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.append(OpCode.PUT, ByteBuffer.wrap("init".getBytes()), "data".getBytes());
        }

        assertTrue(Files.exists(walPath), "Appending must force the OS to create the file");
        assertTrue(Files.size(walPath) > 0, "File should have bytes written to it");
    }
}
