package com.kdb.storage.engine;

import com.kdb.storage.common.KVPair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class SSTableIteratorTest {

    @Test
    void testSequentialIteration(@TempDir Path tempDir) throws Exception {
        Path sstPath = tempDir.resolve("00001.sst");
        long indexOffset;

        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            writeMockEntry(fc, "apple", "red");
            writeMockEntry(fc, "banana", "yellow");
            writeMockEntry(fc, "cherry", "dark_red");

            indexOffset = fc.position();

            fc.write(ByteBuffer.wrap("INDEX_DATA_DO_NOT_READ".getBytes()));
        }

        SSTable table = new SSTable(sstPath, Map.of(), indexOffset, 1L);

        try (SSTableIterator iterator = new SSTableIterator(table, indexOffset)) {

            assertTrue(iterator.hasNext());
            KVPair pair1 = iterator.next();
            assertEquals("apple", new String(pair1.key().array()));
            assertEquals("red", new String(pair1.value().array()));

            assertTrue(iterator.hasNext());
            KVPair pair2 = iterator.next();
            assertEquals("banana", new String(pair2.key().array()));
            assertEquals("yellow", new String(pair2.value().array()));

            assertTrue(iterator.hasNext());
            KVPair pair3 = iterator.next();
            assertEquals("cherry", new String(pair3.key().array()));
            assertEquals("dark_red", new String(pair3.value().array()));

            assertFalse(iterator.hasNext(), "Iterator should stop at the fileReadLimit");
        }
    }

    @Test
    void testNextThrowsExceptionWhenEmpty(@TempDir Path tempDir) throws Exception {
        Path sstPath = tempDir.resolve("00002.sst");
        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fc.write(ByteBuffer.wrap("INDEX_ONLY".getBytes()));
        }

        SSTable table = new SSTable(sstPath, Map.of(), 0L, 2L);

        try (SSTableIterator iterator = new SSTableIterator(table, 0L)) {
            assertFalse(iterator.hasNext(), "Should instantly be false");

            assertThrows(NoSuchElementException.class, iterator::next,
                    "Calling next() when hasNext() is false must throw NoSuchElementException");
        }
    }

    @Test
    void testAutoCloseableReleasesFileLock(@TempDir Path tempDir) throws Exception {
        Path sstPath = tempDir.resolve("00003.sst");
        long indexOffset;

        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            indexOffset = writeMockEntry(fc, "test", "data");
        }

        SSTable table = new SSTable(sstPath, Map.of(), indexOffset, 3L);
        SSTableIterator iterator = new SSTableIterator(table, indexOffset);

        iterator.close();

        assertThrows(RuntimeException.class, iterator::next,
                "Channel should be closed, resulting in an IO error on read");
    }

    /**
     * Helper to write a perfect binary key-value frame as expected by the Iterator
     * Format: [Key Size (int)][Key Data][Value Size (int)][Value Data]
     * @return The ending offset of this entry in the file.
     */
    private long writeMockEntry(FileChannel fc, String key, String value) throws IOException {
        byte[] kBytes = key.getBytes();
        byte[] vBytes = value.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + kBytes.length + vBytes.length);
        buffer.putInt(kBytes.length);
        buffer.put(kBytes);
        buffer.putInt(vBytes.length);
        buffer.put(vBytes);

        buffer.flip();
        fc.write(buffer);

        return fc.position();
    }
}
