package com.kdb.storage.engine;

import com.google.common.collect.ImmutableMap;
import com.kdb.storage.common.KVPair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SSTableIntegrationTest {

    @TempDir
    Path tempDir;

    private SSTableWriter writer;
    private SSTableManager manager;

    @BeforeEach
    void setUp() throws IOException {
        writer = new SSTableWriter(tempDir);
        manager = new SSTableManager(tempDir);
    }

    @Test
    void testWriteLoadAndSearchSSTable() throws IOException {
        ByteBuffer k1 = ByteBuffer.wrap("apple".getBytes(StandardCharsets.UTF_8));
        byte[] v1 = "red".getBytes(StandardCharsets.UTF_8);
        ByteBuffer k2 = ByteBuffer.wrap("banana".getBytes(StandardCharsets.UTF_8));
        byte[] v2 = "yellow".getBytes(StandardCharsets.UTF_8);

        ImmutableMap<ByteBuffer, byte[]> memTableSnapshot = ImmutableMap.of(
                k1, v1,
                k2, v2
        );

        Path sstPath = writer.writeToFile(memTableSnapshot, 1L);
        assertTrue(sstPath.toFile().exists());

        manager.registerSSTable(sstPath);

        Optional<byte[]> resultApple = manager.search(k1);
        Optional<byte[]> resultBanana = manager.search(k2);
        Optional<byte[]> resultCherry = manager.search(ByteBuffer.wrap("cherry".getBytes(StandardCharsets.UTF_8)));

        assertTrue(resultApple.isPresent());
        assertArrayEquals(v1, resultApple.get());

        assertTrue(resultBanana.isPresent());
        assertArrayEquals(v2, resultBanana.get());

        assertTrue(resultCherry.isEmpty());
    }

    @Test
    void testSSTableIterator() throws Exception {
        ByteBuffer k1 = ByteBuffer.wrap("keyA".getBytes(StandardCharsets.UTF_8));
        byte[] v1 = "valA".getBytes(StandardCharsets.UTF_8);
        ImmutableMap<ByteBuffer, byte[]> memTableSnapshot = ImmutableMap.of(k1, v1);

        Path sstPath = writer.writeToFile(memTableSnapshot, 1L);
        manager.registerSSTable(sstPath);

        SSTable table = manager.tables().getFirst();

        try (SSTableIterator iterator = (SSTableIterator) table.iterator()) {
            assertTrue(iterator.hasNext());
            KVPair pair = iterator.next();

            assertEquals(k1, pair.key());
            assertArrayEquals(v1, pair.value().array());

            assertFalse(iterator.hasNext());
        }
    }
}
