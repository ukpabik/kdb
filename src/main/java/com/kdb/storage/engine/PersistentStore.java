package com.kdb.storage.engine;


import com.google.common.collect.ImmutableMap;
import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import com.kdb.storage.exceptions.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * A high-performance, thread-safe, persistent key value store.
 *
 * <p>Provides a link between {@link MemTable} and {@link WriteAheadLog}
 * to create a crash-resilient data store, optimized for writes.</p>
 *
 * @see Store
 * @see StorageEngines#createPersistentStore(Path)
 */
final class PersistentStore implements Store<ByteBuffer, byte[]> {

    private final Store<ByteBuffer, byte[]> memTable;
    private final WriteAheadLog log;
    private final SSTableManager tableManager;
    private final SSTableWriter tableWriter;
    private final AtomicLong sequenceNumber;

    static final byte[] TOMBSTONE = new byte[0];

    // 4 MB flush capacity
    private static final int FLUSH_CAPACITY = 4_000_000;


    PersistentStore(Path directory) throws IOException {
        Objects.requireNonNull(directory);
        this.memTable = StorageEngines.createMemTable();
        this.log = new WriteAheadLog(directory.resolve("wal.log"));
        this.tableManager = new SSTableManager(directory);
        this.tableWriter = new SSTableWriter(directory);
        this.sequenceNumber = loadSequenceNumber(directory);

        recover();
    }

    @Override
    public Optional<byte[]> get(ByteBuffer key) {
        Optional<byte[]> result = memTable.get(key);

        if (result.isPresent()){
            return Arrays.equals(result.get(), TOMBSTONE) ? Optional.empty() : result;
        }

        result = tableManager.search(key);
        if (result.isPresent()) {
            return Arrays.equals(result.get(), TOMBSTONE) ? Optional.empty() : result;
        }

        return result;
    }

    @Override
    public void put(ByteBuffer key, byte[] value) {
        try {
            log.append(OpCode.PUT, key, value);
            memTable.put(key, value);
        } catch(Exception e) {
            throw new StorageException("Failed to persist data to WAL", e);
        }
    }

    @Override
    public Optional<byte[]> remove(ByteBuffer key) {
        try {
            Objects.requireNonNull(key, "Key cannot be null");

            Optional<byte[]> previousValue = memTable.get(key);

            log.append(OpCode.DELETE, key, null);
            memTable.put(key, TOMBSTONE);
            return previousValue;
        } catch (Exception e) {
            throw new StorageException("Failed to persist data to WAL", e);
        }
    }

    private void recover() throws IOException {
        log.replay(memTable::put, (key) -> {
          memTable.put(key, TOMBSTONE);
        });
    }

    private void flush() throws IOException {
        MemTable table = (MemTable) memTable;
        ImmutableMap<ByteBuffer, byte[]> immutableMemTable = table.immutableCopy();

        Path newSSTPath = tableWriter.writeToFile(immutableMemTable, sequenceNumber.get());

        tableManager.registerSSTable(newSSTPath);
        sequenceNumber.incrementAndGet();
        table.clear();
        log.clear();
    }

    private AtomicLong loadSequenceNumber(Path directory) throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            long maxSequence = stream
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.endsWith(".sst"))
                    .map(name -> name.replace(".sst", ""))
                    .filter(name -> name.matches("\\d+"))
                    .mapToLong(Long::parseLong)
                    .max()
                    .orElse(0L);

            return new AtomicLong(maxSequence + 1);
        }
    }
}