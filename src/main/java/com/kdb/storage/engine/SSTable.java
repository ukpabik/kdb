package com.kdb.storage.engine;

import com.google.common.collect.ImmutableSortedMap;
import com.kdb.storage.common.KVPair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.kdb.storage.engine.SSTableWriter.INDEX_BUFFER_LENGTH;

/**
 * A read-only, on-disk data structure providing efficient key-value lookups.
 *
 * <p> The {@code SSTable} (Sorted String Table) stores key-value pairs sorted by key.
 * To avoid loading the entire file into memory, this class maintains a <b>Sparse Index</b>:
 * a subset of keys mapped to their byte offsets within the file. </p>
 *
 * <h3>On-Disk Format:</h3>
 * <pre>
 * [Key Size (int)][Key Data][Value Size (int)][Value Data] ... [Index Metadata]
 * </pre>
 *
 * @see SSTableManager
 * @see SSTableWriter
 */
final class SSTable {

    // Used for indicating a file is a .sst file.
    static final int MAGIC_NUMBER = 0x4B444249;

    private final Path filePath;
    private final ImmutableSortedMap<ByteBuffer, Long> sparseIndex;
    private final long indexOffset;
    private final long sequenceNumber;


    SSTable(Path path, Map<ByteBuffer, Long> index, long indexOffset, long sequenceNumber) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(index);
        filePath = path;
        sparseIndex = ImmutableSortedMap.copyOf(index);
        this.indexOffset = indexOffset;
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * @return The filesystem path where this table is stored.
     */
    Path path() {
        return this.filePath;
    }

    long sequenceNumber() {
        return this.sequenceNumber;
    }

    long indexOffset() {
        return this.indexOffset;
    }

    /**
     * Searches for a value associated with a given key.
     *
     * @param key The key to look up.
     * @return An {@link Optional} containing the byte array value if found, otherwise an empty Optional.
     * @throws IOException If an error occurs during file I/O
     */
    Optional<byte[]> search(ByteBuffer key) throws IOException {
        Map.Entry<ByteBuffer, Long> indexEntry = this.sparseIndex.floorEntry(key);
        long currentOffset = (indexEntry == null) ? 0L : indexEntry.getValue();

        try (FileChannel fc = FileChannel.open(this.filePath, StandardOpenOption.READ)) {
            long fileSize = fc.size();

            while (currentOffset < fileSize - INDEX_BUFFER_LENGTH) {
                ByteBuffer keySizeBuf = ByteBuffer.allocate(Integer.BYTES);
                fc.read(keySizeBuf, currentOffset);
                keySizeBuf.flip();
                int kSize = keySizeBuf.getInt();
                currentOffset += Integer.BYTES;

                ByteBuffer keyBytes = ByteBuffer.allocate(kSize);
                fc.read(keyBytes, currentOffset);
                keyBytes.flip();
                currentOffset += kSize;

                ByteBuffer valueSizeBuf = ByteBuffer.allocate(Integer.BYTES);
                fc.read(valueSizeBuf, currentOffset);
                valueSizeBuf.flip();
                int vSize = valueSizeBuf.getInt();
                currentOffset += Integer.BYTES;

                int compare = key.compareTo(keyBytes);
                if (compare == 0) {
                    ByteBuffer valueBytes = ByteBuffer.allocate(vSize);
                    fc.read(valueBytes, currentOffset);
                    return Optional.of(valueBytes.array());
                } else if (compare > 0) {
                    currentOffset += vSize;
                } else {
                    break;
                }
            }
        }

        return Optional.empty();
    }

    /**
     * @return A {@link SSTableIterator} object on this SSTable.
     * @throws IOException In case of file read error.
     */
    Iterator<KVPair> iterator() throws IOException {
        return new SSTableIterator(this, indexOffset);
    }
}