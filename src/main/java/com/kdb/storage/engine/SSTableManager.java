package com.kdb.storage.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.kdb.storage.common.SafeReadWrite;
import com.kdb.storage.exceptions.CorruptFileException;
import com.kdb.storage.exceptions.StorageException;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.kdb.storage.engine.SSTable.MAGIC_NUMBER;
import static com.kdb.storage.engine.SSTableWriter.INDEX_BUFFER_LENGTH;

/**
 * Orchestrates the lifestyle and logic for all {@link SSTable} instances.
 *
 * @see SSTable
 */
final class SSTableManager implements Closeable {

    private final Path directoryPath;
    private final List<SSTable> tables;

    SSTableManager(Path directory) throws IOException {
        this.directoryPath = directory;

        this.tables = loadTables();
    }


    /**
     * Performs a global search for a key across all SSTables, in order from newest to oldest.
     *
     * @param key The key to locate.
     * @return An {@link Optional} containing the value if found in any table, otherwise an empty Optional.
     * @throws StorageException If there is an underlying I/O error during search.
     */
    Optional<byte[]> search(ByteBuffer key) {
        for (SSTable table : tables) {
            try {
                Optional<byte[]> result = table.search(key.duplicate());

                if (result.isPresent()) {
                    return result;
                }
            } catch (IOException e) {
                throw new StorageException("Error while searching", e);
                // TODO: Log error
            }
        }
        return Optional.empty();
    }

    /**
     * Dynamically adds a new SSTable to the active pool.
     *
     * @param sstPath The path to the newly created .sst file.
     */
    void registerSSTable(Path sstPath) {
        Optional<SSTable> registeredTable = tryLoad(sstPath);

        registeredTable.ifPresent(this.tables::addFirst);
    }

    /**
     * Scans the directory for existing {@code .sst} files and loads them into memory.
     *
     * @return A list of all active SSTables.
     */
    private List<SSTable> loadTables() throws IOException {
        try (Stream<Path> stream = Files.list(directoryPath)) {
            return stream
                    .filter(path -> path.toString().endsWith(".sst"))
                    .map(this::tryLoad)
                    .flatMap(Optional::stream)
                    .sorted(Comparator.comparing(SSTable::path).reversed())
                    .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
        }
    }

    /**
     * Helper function for safely loading a file.
     */
    private Optional<SSTable> tryLoad(Path filePath) {
        try {
            return Optional.of(load(filePath));
        } catch (CorruptFileException e) {
            handleCorruptFile(filePath);
            return Optional.empty();
        } catch (IOException e) {
            throw new StorageException("Error reading .sst file", e);
        }
    }

    /**
     * Reads and parses an SSTable file from disk.
     *
     * @param sstPath Path to the file.
     * @return A fully initialized {@link SSTable}.
     * @throws IOException          If there is a file read error.
     * @throws CorruptFileException If the magic number is missing or file is corrupted.
     */
    private SSTable load(Path sstPath) throws IOException {
        TreeMap<ByteBuffer, Long> indexMap = new TreeMap<>();
        long indexOffset;
        long sequenceNumber = extractSequenceNumber(sstPath.getFileName().toString());
        BloomFilter<byte[]> bloomFilter;

        try (FileChannel fc = FileChannel.open(sstPath, StandardOpenOption.READ)) {
            long fileSize = fc.size();

            long footerOffset = fileSize - INDEX_BUFFER_LENGTH;
            ByteBuffer footerBytes = ByteBuffer.allocate(INDEX_BUFFER_LENGTH);
            SafeReadWrite.readFully(fc, footerBytes, footerOffset);
            footerBytes.flip();

            indexOffset = footerBytes.getLong();
            long indexSize = footerBytes.getLong();
            long bloomOffset = footerBytes.getLong();
            int magicNumber = footerBytes.getInt();

            if (magicNumber != MAGIC_NUMBER) {
                throw new CorruptFileException("This SST file is corrupted.");
            }

            long currentOffset = indexOffset;
            long bytesReadFromIndex = 0;
            ByteBuffer integerSize = ByteBuffer.allocate(Integer.BYTES);

            while (bytesReadFromIndex < indexSize) {
                SafeReadWrite.readFully(fc, integerSize, currentOffset);
                integerSize.flip();
                int kSize = integerSize.getInt();
                integerSize.clear();


                currentOffset += Integer.BYTES;
                bytesReadFromIndex += Integer.BYTES;

                ByteBuffer keyBytes = ByteBuffer.allocate(kSize);
                SafeReadWrite.readFully(fc, keyBytes, currentOffset);
                keyBytes.flip();

                currentOffset += kSize;
                bytesReadFromIndex += kSize;

                SafeReadWrite.readFully(fc, integerSize, currentOffset);
                integerSize.flip();
                int vSize = integerSize.getInt();
                integerSize.clear();

                currentOffset += Integer.BYTES;
                bytesReadFromIndex += Integer.BYTES;

                ByteBuffer valueBytes = ByteBuffer.allocate(vSize);
                SafeReadWrite.readFully(fc, valueBytes, currentOffset);
                valueBytes.flip();

                currentOffset += vSize;
                bytesReadFromIndex += vSize;

                ByteBuffer keyToStore = ByteBuffer.allocate(keyBytes.remaining());
                keyToStore.put(keyBytes);
                keyToStore.flip();

                indexMap.put(keyToStore, valueBytes.getLong());
            }

            long bloomSize = footerOffset - bloomOffset;

            ByteBuffer bloomBuffer = ByteBuffer.allocate((int) bloomSize);
            SafeReadWrite.readFully(fc, bloomBuffer, bloomOffset);
            bloomBuffer.flip();

            try (InputStream is = new ByteArrayInputStream(bloomBuffer.array())) {
                bloomFilter = BloomFilter.readFrom(is, Funnels.byteArrayFunnel());
            }
        }

        return new SSTable(sstPath, indexMap, indexOffset, sequenceNumber, bloomFilter);
    }

    /**
     * Removes all old files for the newly compacted file.
     *
     * @param oldTables List of all old tables to be removed.
     * @param newTable The newly compacted .sst file.
     */
    public synchronized void replaceCompactedTables(List<SSTable> oldTables, SSTable newTable) {
        this.tables.removeAll(oldTables);

        this.tables.addFirst(newTable);

        for (SSTable table : oldTables) {
            if (!table.path().equals(newTable.path())) {
                try {
                    Files.deleteIfExists(table.path());
                } catch (IOException e) {
                    // TODO: Log this
                }
            }
        }
    }

    /**
     * Helper function for handling corrupt files.
     */
    private void handleCorruptFile(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new StorageException("Error deleting corrupt file", e);
            // TODO: Log this??
        }
    }

    /**
     * Helper to extract the sequence number from filenames.
     */
    private long extractSequenceNumber(String fileName) {
        try {
            return Long.parseLong(fileName.replaceAll("[^0-9]", ""));
        } catch (NumberFormatException e) {
            throw new CorruptFileException("Invalid SSTable filename: " + fileName);
        }
    }

    /**
     * @return A list of all active SSTables.
     */
    List<SSTable> tables() {
        return ImmutableList.copyOf(tables);
    }

    @Override
    public void close() throws IOException {
        for (SSTable table : tables) {
            table.close();
        }
    }
}
