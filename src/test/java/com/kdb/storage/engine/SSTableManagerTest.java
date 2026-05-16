package com.kdb.storage.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SSTableManagerTest {

    private static final int MAGIC_NUMBER = SSTable.MAGIC_NUMBER;
    private static final int INDEX_BUFFER_LENGTH = 20;

    @Test
    void testLoadTablesAndSortOrder(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("00001.sst");
        Path file2 = tempDir.resolve("00003.sst");
        Path file3 = tempDir.resolve("00002.sst");

        Map<String, Long> dummyIndex = Map.of("apple", 100L);

        createMockSSTable(file1, dummyIndex, MAGIC_NUMBER);
        createMockSSTable(file2, dummyIndex, MAGIC_NUMBER);
        createMockSSTable(file3, dummyIndex, MAGIC_NUMBER);

        SSTableManager manager = new SSTableManager(tempDir);
        List<SSTable> activeTables = manager.tables();

        assertEquals(3, activeTables.size(), "Manager should load all 3 valid SSTables");
        assertTrue(activeTables.get(0).path().endsWith("00003.sst"), "Newest sequence number must be first");
        assertTrue(activeTables.get(1).path().endsWith("00002.sst"), "Middle sequence number must be second");
        assertTrue(activeTables.get(2).path().endsWith("00001.sst"), "Oldest sequence number must be last");
    }

    @Test
    void testCorruptFileIsGracefullyDeleted(@TempDir Path tempDir) throws Exception {
        Path validFile = tempDir.resolve("00001.sst");
        Path corruptFile = tempDir.resolve("00002.sst");

        createMockSSTable(validFile, Map.of("key1", 50L), MAGIC_NUMBER);
        createMockSSTable(corruptFile, Map.of("key2", 60L), 0xBADBAD); // Invalid Magic Number

        SSTableManager manager = new SSTableManager(tempDir);
        List<SSTable> activeTables = manager.tables();

        assertEquals(1, activeTables.size(), "Manager should skip the corrupted table");
        assertTrue(activeTables.getFirst().path().endsWith("00001.sst"));

        assertFalse(Files.exists(corruptFile), "Manager must actively delete corrupted files from disk");
    }

    @Test
    void testRegisterNewSSTable(@TempDir Path tempDir) throws Exception {
        SSTableManager manager = new SSTableManager(tempDir);
        assertTrue(manager.tables().isEmpty());

        Path newFile = tempDir.resolve("00001.sst");
        createMockSSTable(newFile, Map.of("new_key", 100L), MAGIC_NUMBER);

        manager.registerSSTable(newFile);

        assertEquals(1, manager.tables().size());
        assertTrue(manager.tables().getFirst().path().endsWith("00001.sst"), "New table should be added to the active list");
    }

    @Test
    void testInvalidFilenameIsRejected(@TempDir Path tempDir) throws Exception {
        Path badNameFile = tempDir.resolve("garbage_name.sst");
        createMockSSTable(badNameFile, Map.of("key", 0L), MAGIC_NUMBER);

        SSTableManager manager = new SSTableManager(tempDir);

        assertTrue(manager.tables().isEmpty(), "File with bad name should not be loaded");
        assertFalse(Files.exists(badNameFile), "File with bad name should be treated as corrupt and deleted");
    }

    /**
     * Helper method to craft a raw, perfectly formatted .sst file
     * exactly the way your SSTableManager expects to read it.
     */
    private void createMockSSTable(Path path, Map<String, Long> indexEntries, int magicNumber) throws IOException {
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

            fc.write(ByteBuffer.wrap("dummy_data_block_".getBytes()));

            long indexOffset = fc.position();
            long indexStart = fc.position();

            for (Map.Entry<String, Long> entry : indexEntries.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes();

                ByteBuffer kSizeBuf = ByteBuffer.allocate(Integer.BYTES).putInt(keyBytes.length);
                kSizeBuf.flip();
                fc.write(kSizeBuf);

                fc.write(ByteBuffer.wrap(keyBytes));

                ByteBuffer vSizeBuf = ByteBuffer.allocate(Integer.BYTES).putInt(Long.BYTES);
                vSizeBuf.flip();
                fc.write(vSizeBuf);

                ByteBuffer valBuf = ByteBuffer.allocate(Long.BYTES).putLong(entry.getValue());
                valBuf.flip();
                fc.write(valBuf);
            }

            long indexSize = fc.position() - indexStart;

            ByteBuffer footer = ByteBuffer.allocate(INDEX_BUFFER_LENGTH);
            footer.putLong(indexOffset);
            footer.putLong(indexSize);
            footer.putInt(magicNumber);
            footer.flip();

            fc.write(footer);
        }
    }
}
