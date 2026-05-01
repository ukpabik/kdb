package com.kdb.storage.engine;


import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import com.kdb.storage.persistence.WriteAheadLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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


    PersistentStore(Path logPath) throws IOException {
        this.memTable = StorageEngines.createMemTable();
        this.log = new WriteAheadLog(logPath.resolve("wal.log"));

        recover();
    }

    @Override
    public Optional<byte[]> get(ByteBuffer key) {
        Optional<byte[]> result = memTable.get(key);

        if (result.isPresent() && result.get() == null){
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
            // TODO: Create internal error for WAL (StorageException)
            throw new RuntimeException("Failed to persist data to WAL", e);
        }
    }

    @Override
    public Optional<byte[]> remove(ByteBuffer key) {
        try {
            Objects.requireNonNull(key, "Key cannot be null");

            Optional<byte[]> previousValue = memTable.get(key);

            log.append(OpCode.DELETE, key, null);
            memTable.put(key, null);
            return previousValue;
        } catch (Exception e) {
            throw new RuntimeException("Failed to persist data to WAL", e);
        }
    }

    private void recover() throws IOException {
        // TODO: Unimplemented
    }
}