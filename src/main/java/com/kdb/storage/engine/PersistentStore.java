package com.kdb.storage.engine;


import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import com.kdb.storage.exceptions.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

    // TODO: Implement initialization for this
//    private final List<SSTable> ssTables;

    static final byte[] TOMBSTONE = new byte[0];

    // 4 MB flush capacity
    private static final int FLUSH_CAPACITY = 4_000_000;


    PersistentStore(Path logPath) throws IOException {
        Objects.requireNonNull(logPath);
        this.memTable = StorageEngines.createMemTable();
        this.log = new WriteAheadLog(logPath.resolve("wal.log"));


        // TODO: Load SSTables into the list?

        recover();
    }

    @Override
    public Optional<byte[]> get(ByteBuffer key) {
        Optional<byte[]> result = memTable.get(key);

        if (result.isPresent() && result.get() == TOMBSTONE){
            return Optional.empty();
        }

        // TODO: Check SSTables for value

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

    private void loadSSTables() {
        // TODO: Unimplemented
    }
}