package com.kdb.storage.engine;

import com.kdb.storage.common.KVPair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class CompactionManagerTest {

    private static final int INDEX_BUFFER_LENGTH = 20;

    @Test
    void testBasicCompactionWithoutOverlaps(@TempDir Path tempDir) throws Exception {
        Path sst1 = tempDir.resolve("00001.sst");
        Path sst2 = tempDir.resolve("00002.sst");

        SSTable table1 = createMockSSTable(sst1, 1L, Map.of("apple", "red", "banana", "yellow"));
        SSTable table2 = createMockSSTable(sst2, 2L, Map.of("cherry", "dark_red", "date", "brown"));

        CompactionManager manager = new CompactionManager(tempDir);

        List<SSTable> activeTables = List.of(table2, table1);

        manager.compact(activeTables);

        assertTrue(Files.exists(sst1), "Compacted file should replace the oldest SSTable in the list");

        Map<String, String> results = readCompactedFile(sst1);

        assertEquals(4, results.size());
        assertEquals("red", results.get("apple"));
        assertEquals("yellow", results.get("banana"));
        assertEquals("dark_red", results.get("cherry"));
        assertEquals("brown", results.get("date"));
    }

    @Test
    void testCompactionWithOverwritesAndGhostPop(@TempDir Path tempDir) throws Exception {
        Path oldSst = tempDir.resolve("00001.sst");
        Path newSst = tempDir.resolve("00002.sst");

        SSTable tableOld = createMockSSTable(oldSst, 1L, Map.of("user_1", "v1", "user_2", "v1"));

        SSTable tableNew = createMockSSTable(newSst, 2L, Map.of("user_1", "v2", "user_3", "v2"));

        CompactionManager manager = new CompactionManager(tempDir);
        List<SSTable> activeTables = List.of(tableNew, tableOld);

        manager.compact(activeTables);

        Map<String, String> results = readCompactedFile(oldSst);

        assertEquals(3, results.size(), "Compaction must merge down to exactly 3 unique keys");

        assertEquals("v2", results.get("user_1"), "Newer sequence number MUST overwrite the older value");
        assertEquals("v1", results.get("user_2"), "Non-overlapping older keys should still exist");
        assertEquals("v2", results.get("user_3"), "Non-overlapping newer keys should be included");
    }

    /**
     * Helper to write raw KVPairs to disk exactly how the SSTableIterator expects to read them.
     */
    private SSTable createMockSSTable(Path path, long sequenceNumber, Map<String, String> data) throws IOException {
        TreeMap<String, String> sortedData = new TreeMap<>(data);

        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for (Map.Entry<String, String> entry : sortedData.entrySet()) {
                byte[] kBytes = entry.getKey().getBytes();
                byte[] vBytes = entry.getValue().getBytes();

                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + kBytes.length + vBytes.length);
                buffer.putInt(kBytes.length);
                buffer.put(kBytes);
                buffer.putInt(vBytes.length);
                buffer.put(vBytes);
                buffer.flip();

                fc.write(buffer);
            }
            long indexOffset = fc.position();

            fc.write(ByteBuffer.allocate(INDEX_BUFFER_LENGTH + 100));

            return new SSTable(path, Collections.emptyMap(), indexOffset, sequenceNumber);
        }
    }

    /**
     * Helper to parse the newly compacted file using your own SSTableIterator to ensure
     * it conforms to your database's strict binary contracts.
     */
    private Map<String, String> readCompactedFile(Path path) throws Exception {
        Map<String, String> extractedData = new LinkedHashMap<>();

        long indexOffset;
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer footer = ByteBuffer.allocate(INDEX_BUFFER_LENGTH);
            fc.read(footer, fc.size() - INDEX_BUFFER_LENGTH);
            footer.flip();
            indexOffset = footer.getLong();
        }

        SSTable dummyTable = new SSTable(path, Collections.emptyMap(), indexOffset, 99L);
        try (SSTableIterator iterator = new SSTableIterator(dummyTable, indexOffset)) {
            while (iterator.hasNext()) {
                KVPair pair = iterator.next();
                extractedData.put(
                        new String(pair.key().array()),
                        new String(pair.value().array())
                );
            }
        }
        return extractedData;
    }
}
