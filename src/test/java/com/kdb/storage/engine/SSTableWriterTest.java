package com.kdb.storage.engine;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class SSTableWriterTest {
    @TempDir
    Path tempDir;

    private SSTableWriter writer;

    @BeforeEach
    void setUp() {
        writer = new SSTableWriter(tempDir);
    }

    @Test
    void testWriteToFile_CreatesFileWithValidFooter() throws IOException {
        ImmutableMap<ByteBuffer, byte[]> data = ImmutableMap.of(
                ByteBuffer.wrap("key1".getBytes()), "value1".getBytes(),
                ByteBuffer.wrap("key2".getBytes()), "value2".getBytes()
        );

        Path sstFile = writer.writeToFile(data, 0);

        assertTrue(Files.exists(sstFile), "SSTable file should be created on disk");
        assertTrue(sstFile.toString().endsWith(".sst"), "File must have the .sst extension");

        try (FileChannel fc = FileChannel.open(sstFile, StandardOpenOption.READ)) {
            long fileSize = fc.size();
            assertTrue(fileSize > 20, "File should contain data and a 20-byte footer");

            ByteBuffer footer = ByteBuffer.allocate(20);
            fc.position(fileSize - 20);
            fc.read(footer);
            footer.flip();

            long indexOffset = footer.getLong();
            long indexSize = footer.getLong();
            int magicNumber = footer.getInt();

            assertEquals(SSTable.MAGIC_NUMBER, magicNumber, "Magic number must match exactly at EOF");
            assertTrue(indexOffset > 0, "Index offset should be greater than 0");
            assertTrue(indexSize > 0, "Index size should be > 0 because the first key is always indexed");
        }
    }

    @Test
    void testWriteToFile_GeneratesIndexForLargeDataset() throws IOException {
        ImmutableMap.Builder<ByteBuffer, byte[]> builder = ImmutableMap.builder();
        for (int i = 0; i < 250; i++) {
            builder.put(ByteBuffer.wrap(("key" + i).getBytes()), ("value" + i).getBytes());
        }
        ImmutableMap<ByteBuffer, byte[]> data = builder.build();

        Path sstFile = writer.writeToFile(data, 0);

        try (FileChannel fc = FileChannel.open(sstFile, StandardOpenOption.READ)) {
            ByteBuffer footer = ByteBuffer.allocate(20);
            fc.position(fc.size() - 20);
            fc.read(footer);
            footer.flip();

            footer.getLong();
            long indexSize = footer.getLong();

            assertTrue(indexSize > 0, "Index size should be > 0 because we exceeded the 100 key threshold");
        }
    }

    @Test
    void testWriteToFile_HandlesEmptyMemTable() throws IOException {
        ImmutableMap<ByteBuffer, byte[]> emptyData = ImmutableMap.of();

        Path sstFile = writer.writeToFile(emptyData, 0);

        try (FileChannel fc = FileChannel.open(sstFile, StandardOpenOption.READ)) {
            assertEquals(20, fc.size(), "File should only contain the empty 20-byte footer");

            ByteBuffer footer = ByteBuffer.allocate(20);
            fc.read(footer);
            footer.flip();

            assertEquals(0, footer.getLong(), "Index offset should be 0");
            assertEquals(0, footer.getLong(), "Index size should be 0");
            assertEquals(SSTable.MAGIC_NUMBER, footer.getInt(), "Magic number must still be present");
        }
    }
}