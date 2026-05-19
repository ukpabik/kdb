package com.kdb.storage.engine;


import com.google.common.collect.ImmutableMap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.kdb.storage.common.SafeReadWrite;
import com.kdb.storage.exceptions.StorageException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

import static com.kdb.storage.common.FileSystemConstants.*;
import static com.kdb.storage.common.Serializer.serialize;
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


    private final Random rand = new Random();



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
     * builds a sparse index and bloom filter to optimize read performance, and appends a footer
     * for file validation and index location.</p>
     *
     * @param memTableSnapshot an immutable map representing the current state of the MemTable
     * @return the {@link Path} to the newly created SSTable file
     * @throws StorageException thrown if an I/O error occurs while creating or writing to the file
     */
    public synchronized Path writeToFile(ImmutableMap<ByteBuffer, byte[]> memTableSnapshot, long sequenceNumber) {
        Objects.requireNonNull(memTableSnapshot);

        String fileName = String.format("%05d%s", sequenceNumber, SST_FILE_EXT);
        Path filePath = Path.of(directoryPath.toString(), fileName);

        int counter = 0;
        Map<ByteBuffer, Long> indexMap = new TreeMap<>();

        BloomFilter<byte[]> bloomFilter = BloomFilter.create(
                Funnels.byteArrayFunnel(), Math.max(1, memTableSnapshot.size()), 0.01);

        ByteBuffer pageBuf = ByteBuffer.allocate(PAGE_BUFFER_SIZE);
        long currentOffset = 0;

        try (FileChannel fc = FileChannel.open(filePath, CREATE, APPEND)) {
            boolean isFirstKey = true;
            for (ImmutableMap.Entry<ByteBuffer, byte[]> entry : memTableSnapshot.entrySet()) {

                byte[] keyBytes = new byte[entry.getKey().remaining()];
                entry.getKey().duplicate().get(keyBytes);
                bloomFilter.put(keyBytes);

                ByteBuffer serializedBytes = serialize(entry.getKey(), entry.getValue());
                int pairLength = serializedBytes.remaining();


                if (isFirstKey) {
                    indexMap.put(entry.getKey(), currentOffset);
                    isFirstKey = false;
                } else if (counter == INDEX_SEGMENT) {
                    counter = 0;
                    indexMap.put(entry.getKey(), currentOffset);
                }
                counter++;

                if (pageBuf.remaining() < pairLength) {
                    if (pageBuf.position() == 0) {
                        SafeReadWrite.writeFully(fc, serializedBytes);
                        currentOffset += pairLength;
                        continue;
                    }

                    pageBuf.flip();
                    SafeReadWrite.writeFully(fc, pageBuf);
                    pageBuf.clear();
                }
                pageBuf.put(serializedBytes);
                currentOffset += pairLength;
            }

            if (pageBuf.position() > 0) {
                pageBuf.flip();
                SafeReadWrite.writeFully(fc, pageBuf);
            }

            long indexOffset = fc.size();
            long indexSize = 0;

            for (Map.Entry<ByteBuffer, Long> entry : indexMap.entrySet()) {
                ByteBuffer serializedBytes = serialize(entry.getKey(), entry.getValue());
                indexSize += serializedBytes.remaining();
                SafeReadWrite.writeFully(fc, serializedBytes);
            }

            long bloomOffset = fc.size();
            OutputStream os = Channels.newOutputStream(fc);
            bloomFilter.writeTo(os);
            os.flush();

            // Index format: [index offset: 8 bytes, index size: 8 bytes, bloom offset: 8 bytes, magic number (to know sst): 4 bytes]
            ByteBuffer indexBuffer = ByteBuffer.allocate(INDEX_BUFFER_LENGTH);
            indexBuffer.putLong(indexOffset);
            indexBuffer.putLong(indexSize);
            indexBuffer.putLong(bloomOffset);
            indexBuffer.putInt(MAGIC_NUMBER);
            indexBuffer.flip();

            SafeReadWrite.writeFully(fc, indexBuffer);
            fc.force(true);
            return filePath;
        } catch (IOException e) {
            throw new StorageException("Failed to write MemTable to file", e);
        }
    }


}