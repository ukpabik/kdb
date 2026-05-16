package com.kdb.storage.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class SSTableTest {

    private static final int INDEX_BUFFER_LENGTH = 20;

    @Test
    void testExactMatchFromSparseIndex(@TempDir Path tempDir) throws Exception {
        Path sstPath = tempDir.resolve("00001.sst");
        Map<ByteBuffer, Long> mockIndex = new TreeMap<>();

        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long appleOffset = writeMockEntry(fc, "apple", "red");
            mockIndex.put(ByteBuffer.wrap("apple".getBytes()), appleOffset);

            writeDummyFooter(fc);
        }

        SSTable table = new SSTable(sstPath, mockIndex, 0L, 1L);

        Optional<byte[]> result = table.search(ByteBuffer.wrap("apple".getBytes()));

        assertTrue(result.isPresent(), "Should find 'apple'");
        assertArrayEquals("red".getBytes(), result.get(), "Value should perfectly match 'red'");
    }

    @Test
    void testScanMatchBetweenIndexEntries(@TempDir Path tempDir) throws Exception {
        Path sstPath = tempDir.resolve("00002.sst");
        Map<ByteBuffer, Long> mockIndex = new TreeMap<>();

        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long appleOffset = writeMockEntry(fc, "apple", "red");
            mockIndex.put(ByteBuffer.wrap("apple".getBytes()), appleOffset);

            writeMockEntry(fc, "banana", "yellow"); // NOT in index
            writeMockEntry(fc, "cherry", "dark_red"); // NOT in index

            long dateOffset = writeMockEntry(fc, "date", "brown");
            mockIndex.put(ByteBuffer.wrap("date".getBytes()), dateOffset);

            writeDummyFooter(fc);
        }

        SSTable table = new SSTable(sstPath, mockIndex, 0L, 2L);

        Optional<byte[]> result = table.search(ByteBuffer.wrap("banana".getBytes()));

        assertTrue(result.isPresent());
        assertArrayEquals("yellow".getBytes(), result.get());
    }

    @Test
    void testEarlyBreakWhenKeyDoesNotExist(@TempDir Path tempDir) throws Exception {
        Path sstPath = tempDir.resolve("00003.sst");
        Map<ByteBuffer, Long> mockIndex = new TreeMap<>();

        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long appleOffset = writeMockEntry(fc, "apple", "red");
            mockIndex.put(ByteBuffer.wrap("apple".getBytes()), appleOffset);

            writeMockEntry(fc, "banana", "yellow");
            writeMockEntry(fc, "date", "brown"); // 'cherry' is missing!

            writeDummyFooter(fc);
        }

        SSTable table = new SSTable(sstPath, mockIndex, 0L, 3L);

        Optional<byte[]> result = table.search(ByteBuffer.wrap("cherry".getBytes()));

        assertTrue(result.isEmpty(), "Cherry does not exist, should return empty");
    }

    @Test
    void testKeyOutOfIndexBounds(@TempDir Path tempDir) throws Exception {
        Path sstPath = tempDir.resolve("00004.sst");
        Map<ByteBuffer, Long> mockIndex = new TreeMap<>();

        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long bananaOffset = writeMockEntry(fc, "banana", "yellow");
            mockIndex.put(ByteBuffer.wrap("banana".getBytes()), bananaOffset);
            writeDummyFooter(fc);
        }

        SSTable table = new SSTable(sstPath, mockIndex, 0L, 4L);


        Optional<byte[]> underBound = table.search(ByteBuffer.wrap("apple".getBytes()));
        assertTrue(underBound.isEmpty());

        Optional<byte[]> overBound = table.search(ByteBuffer.wrap("zebra".getBytes()));
        assertTrue(overBound.isEmpty());
    }

    /**
     * Helper to write a perfect binary key-value frame as expected by SSTable.search()
     * Format: [Key Size (int)][Key Data][Value Size (int)][Value Data]
     * @return The starting offset of this entry in the file.
     */
    private long writeMockEntry(FileChannel fc, String key, String value) throws IOException {
        long offset = fc.position();
        byte[] kBytes = key.getBytes();
        byte[] vBytes = value.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + kBytes.length + vBytes.length);
        buffer.putInt(kBytes.length);
        buffer.put(kBytes);
        buffer.putInt(vBytes.length);
        buffer.put(vBytes);

        buffer.flip();
        fc.write(buffer);

        return offset;
    }

    /**
     * Helper to pad the end of the file so the `currentOffset < fileSize - INDEX_BUFFER_LENGTH`
     * condition in SSTable.search() correctly stops the loop before hitting the footer metadata.
     */
    private void writeDummyFooter(FileChannel fc) throws IOException {
        fc.write(ByteBuffer.allocate(INDEX_BUFFER_LENGTH));
    }
}
