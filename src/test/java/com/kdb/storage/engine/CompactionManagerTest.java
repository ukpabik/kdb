package com.kdb.storage.engine;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CompactionManagerTest {

    @TempDir
    Path tempDir;

    private SSTableWriter writer;
    private SSTableManager manager;
    private CompactionManager compactionManager;

    @BeforeEach
    void setUp() throws IOException {
        writer = new SSTableWriter(tempDir);
        manager = new SSTableManager(tempDir);
        compactionManager = new CompactionManager(tempDir);
    }

    @Test
    void testCompactionDeduplication() throws IOException {
        ByteBuffer key1 = ByteBuffer.wrap("user123".getBytes(StandardCharsets.UTF_8));

        byte[] oldVal = "old_email@test.com".getBytes(StandardCharsets.UTF_8);
        Path sst1 = writer.writeToFile(ImmutableMap.of(key1, oldVal), 1L);

        byte[] newVal = "new_email@test.com".getBytes(StandardCharsets.UTF_8);
        Path sst2 = writer.writeToFile(ImmutableMap.of(key1, newVal), 2L);

        manager.registerSSTable(sst1);
        manager.registerSSTable(sst2);

        List<SSTable> activeTables = manager.tables();
        assertEquals(2, activeTables.size());

        SSTable newTable = compactionManager.compact(activeTables);
        manager.replaceCompactedTables(activeTables, newTable);

        SSTableManager compactedManager = new SSTableManager(tempDir);

        assertEquals(1, compactedManager.tables().size());

        Optional<byte[]> result = compactedManager.search(key1);
        assertTrue(result.isPresent());
        assertArrayEquals(newVal, result.get());
    }
}
