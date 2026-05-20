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

import static com.kdb.storage.common.FileSystemConstants.TOMBSTONE;
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

    @Test
    void testMajorCompactionTombstonePurging() throws IOException {
        // Arrange keys
        ByteBuffer deletedKey = ByteBuffer.wrap("user_to_delete".getBytes(StandardCharsets.UTF_8));
        ByteBuffer livingKey = ByteBuffer.wrap("user_to_keep".getBytes(StandardCharsets.UTF_8));

        byte[] activeVal = "keep_me@test.com".getBytes(StandardCharsets.UTF_8);
        byte[] obsoleteVal = "delete_me_soon@test.com".getBytes(StandardCharsets.UTF_8);
        Path sst1 = writer.writeToFile(ImmutableMap.of(
                livingKey, activeVal,
                deletedKey, obsoleteVal
        ), 1L);

        Path sst2 = writer.writeToFile(ImmutableMap.of(
                deletedKey, TOMBSTONE
        ), 2L);

        manager.registerSSTable(sst1);
        manager.registerSSTable(sst2);

        List<SSTable> activeTables = manager.tables();
        assertEquals(2, activeTables.size(), "Pre-compaction environment should manage exactly 2 SSTables.");

        SSTable compactedTable = compactionManager.compact(activeTables);
        manager.replaceCompactedTables(activeTables, compactedTable);

        SSTableManager verificationManager = new SSTableManager(tempDir);

        assertEquals(1, verificationManager.tables().size(), "Major compaction must flatten layout down to 1 file.");

        Optional<byte[]> deletedResult = verificationManager.search(deletedKey);
        assertFalse(deletedResult.isPresent(),
                "The tombstone key should be permanently dropped from disk during a major compaction, returning empty.");

        Optional<byte[]> livingResult = verificationManager.search(livingKey);
        assertTrue(livingResult.isPresent(), "Living parallel records must survive compaction loops.");
        assertArrayEquals(activeVal, livingResult.get(), "The preserved value should exactly match the living state data payload.");
    }
}
