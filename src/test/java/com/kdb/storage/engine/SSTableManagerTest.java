package com.kdb.storage.engine;

import com.kdb.storage.exceptions.CorruptFileException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SSTableManagerTest {

    private static final int VALID_MAGIC_NUMBER = SSTable.MAGIC_NUMBER;

    @TempDir
    Path tempDir;

    @Test
    void testEmptyDirectoryLoadsNoTables() throws IOException {
        SSTableManager manager = new SSTableManager(tempDir);
        assertTrue(manager.tables().isEmpty(), "Manager should initialize with an empty list when no files exist.");
    }

    @Test
    void testCorruptedFileIsGracefullyDeleted() throws IOException {
        Path corruptedPath = tempDir.resolve("00001.sst");
        createMockSSTable(corruptedPath, 999999);

        assertTrue(Files.exists(corruptedPath), "Corrupted file should exist before manager loads.");

        SSTableManager manager = new SSTableManager(tempDir);

        assertTrue(manager.tables().isEmpty(), "Corrupted file should not be loaded into memory.");
        assertFalse(Files.exists(corruptedPath), "Manager should have self-healed and deleted the corrupted file.");
    }

    @Test
    void testValidFilesLoadedAndSortedChronologically() throws IOException {
        Path table1 = tempDir.resolve("00001.sst");
        Path table3 = tempDir.resolve("00003.sst");
        Path table2 = tempDir.resolve("00002.sst");

        createMockSSTable(table1, VALID_MAGIC_NUMBER);
        createMockSSTable(table3, VALID_MAGIC_NUMBER);
        createMockSSTable(table2, VALID_MAGIC_NUMBER);

        SSTableManager manager = new SSTableManager(tempDir);
        List<SSTable> loadedTables = manager.tables();

        assertEquals(3, loadedTables.size(), "All 3 valid tables should be loaded.");

        assertEquals(table3, loadedTables.get(0).path(), "00003.sst should be first");
        assertEquals(table2, loadedTables.get(1).path(), "00002.sst should be second");
        assertEquals(table1, loadedTables.get(2).path(), "00001.sst should be third");
    }

    @Test
    void testRegisterNewSSTableDynamically() throws IOException {
        SSTableManager manager = new SSTableManager(tempDir);
        assertTrue(manager.tables().isEmpty());

        Path newTable = tempDir.resolve("00001.sst");
        createMockSSTable(newTable, VALID_MAGIC_NUMBER);

        manager.registerSSTable(newTable);

        assertEquals(1, manager.tables().size(), "Table should be dynamically registered.");
        assertEquals(newTable, manager.tables().getFirst().path());
    }

    /**
     * Helper Method: Crafts a raw binary SSTable file with a valid footer structure.
     * We set indexSize to 0 so the while loop in the manager safely skips reading index entries,
     * allowing us to strictly test the file parsing and magic number validation.
     */
    private void createMockSSTable(Path path, int magicNumber) throws IOException {
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long indexOffset = 0;
            long indexSize = 0;

            // Allocate 20 bytes for the footer: [offset: 8] [size: 8] [magic: 4]
            ByteBuffer footer = ByteBuffer.allocate(20);
            footer.putLong(indexOffset);
            footer.putLong(indexSize);
            footer.putInt(magicNumber);
            footer.flip();

            fc.write(footer);
        }
    }
}
