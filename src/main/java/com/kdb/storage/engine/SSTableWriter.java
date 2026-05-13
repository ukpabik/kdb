package com.kdb.storage.engine;


import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

import static com.kdb.storage.engine.SSTable.MAGIC_NUMBER;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;


/**
 * A utility class responsible for flushing in-memory data to an immutable on-disk Sorted String Table (SSTable).
 *
 * <p>Serialized Structure:
 * [Data Block]
 * [4 bytes: size of key]
 * [N bytes: key]
 * [4 bytes: size of value]
 * [M bytes: value]
 * ... (Repeats for all entries)
 * [Index Block] (Sparse index, 1 entry per 100 keys)
 * [4 bytes: size of key]
 * [N bytes: key]
 * [4 bytes: size of long]
 * [8 bytes: offset in file]
 * [Footer]
 * [8 bytes: index block offset]
 * [8 bytes: index block size]
 * [4 bytes: magic number]</p>
 *
 * @see PersistentStore
 */
final class SSTableWriter {

    private static final String SST_FILE_EXT = ".sst";
    private static final int SST_FILENAME_SIZE = 32;
    private final Path directoryPath;

    // Index for every 100 keys
    private static final int INDEX_SEGMENT = 100;
    private final Random rand = new Random();

    static final int INDEX_BUFFER_LENGTH = 20;

    /**
     * Initializes the SSTableWriter with a target directory for new files.
     *
     * @param directory the directory path where new SSTable files will be generated and stored
     */
    SSTableWriter(Path directory) {
       this.directoryPath = directory;
    }

    /**
     * Persists a snapshot of the MemTable to a newly generated SSTable file.
     * * <p>Generates a random filename, sequentially writes all key-value pairs,
     * builds a sparse index to optimize read performance, and appends a footer
     * for file validation and index location.</p>
     *
     * @param memTableSnapshot an immutable map representing the current state of the MemTable
     * @return the {@link Path} to the newly created SSTable file
     * @throws RuntimeException thrown if an I/O error occurs while creating or writing to the file
     */
    public synchronized Path writeToFile(ImmutableMap<ByteBuffer, byte[]> memTableSnapshot, long sequenceNumber) {
        Objects.requireNonNull(memTableSnapshot);

        String fileName = String.format("%05d%s", sequenceNumber, SST_FILE_EXT);
        Path filePath = Path.of(directoryPath.toString(), fileName);

        int counter = 0; // For telling us what key we are on
        Map<ByteBuffer, Long> indexMap = new TreeMap<>();

        try (FileChannel fc = FileChannel.open(filePath, CREATE, APPEND)) {
            for (ImmutableMap.Entry<ByteBuffer, byte[]> entry : memTableSnapshot.entrySet()) {
                ByteBuffer serializedBytes = serialize(entry.getKey(), entry.getValue());

                counter++;

                if (counter == INDEX_SEGMENT) {
                    counter = 0;
                    indexMap.put(entry.getKey(), fc.size());
                }

                fc.write(serializedBytes);
            }

            // Index format: [index offset: 8 bytes, index size: 8 bytes, magic number (to know sst): 4 bytes]
            ByteBuffer indexBuffer = ByteBuffer.allocate(INDEX_BUFFER_LENGTH);
            long indexOffset = fc.size();
            long indexSize = 0;

            for (Map.Entry<ByteBuffer, Long> entry : indexMap.entrySet()) {
                ByteBuffer serializedBytes = serialize(entry.getKey(), entry.getValue());
                indexSize += serializedBytes.remaining();
                fc.write(serializedBytes);
            }

            indexBuffer.putLong(indexOffset);
            indexBuffer.putLong(indexSize);
            indexBuffer.putInt(MAGIC_NUMBER);
            indexBuffer.flip();

            fc.write(indexBuffer);
            fc.force(true);
            return filePath;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write MemTable to file", e);
        }
    }

    private ByteBuffer serialize(ByteBuffer key, byte[] value) {
        return serialize(key, ByteBuffer.wrap(value));
    }

    private ByteBuffer serialize(ByteBuffer key, long offset) {
        ByteBuffer valueBuffer = ByteBuffer.allocate(Long.BYTES).putLong(offset);
        valueBuffer.flip();
        return serialize(key, valueBuffer);
    }

    private ByteBuffer serialize(ByteBuffer key, ByteBuffer value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        int totalSize = (Integer.BYTES * 2) + key.remaining() + value.remaining();

        ByteBuffer result = ByteBuffer.allocate(totalSize);

        result.putInt(key.remaining());
        result.put(key.duplicate());

        result.putInt(value.remaining());
        result.put(value.duplicate());

        result.flip();
        return result;
    }
}