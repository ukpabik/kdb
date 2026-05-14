package com.kdb.storage.engine;


import com.google.common.collect.ImmutableMap;
import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import com.kdb.storage.exceptions.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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

    private volatile Store<ByteBuffer, byte[]> activeMemTable;
    private volatile Store<ByteBuffer, byte[]> inactiveMemTable;
    private WriteAheadLog activeLog;
    private final SSTableManager tableManager;
    private final SSTableWriter tableWriter;
    private final AtomicLong sstSequenceNumber;
    private final AtomicLong logSequenceNumber;
    private final Path directory;
    private final ExecutorService flushService;

    static final byte[] TOMBSTONE = new byte[0];

    // 4 MB flush capacity
    private static final int FLUSH_CAPACITY = 4_000_000;


    PersistentStore(Path directory) throws IOException {
        Objects.requireNonNull(directory);
        this.directory = directory;
        this.sstSequenceNumber = loadSSTSequenceNumber();
        this.logSequenceNumber = loadLogSequenceNumber();

        this.activeMemTable = StorageEngines.createMemTable();
        this.activeLog = loadLog();

        this.tableManager = new SSTableManager(directory);
        this.tableWriter = new SSTableWriter(directory);
        this.flushService = Executors.newSingleThreadExecutor();

        recover();
        // TODO: Check if we need to perform a flush here.
        // TODO: Find some way to check if a flush is currently queued. We do not want to start multiple flushes at the same time.
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> get(ByteBuffer key) {
        Optional<byte[]> result = activeMemTable.get(key);

        if (result.isPresent()) {
            return result.get() == TOMBSTONE ? Optional.empty() : result;
        }

        if (inactiveMemTable != null) {
            result = inactiveMemTable.get(key);
            if (result.isPresent()) {
                return result.get() == TOMBSTONE ? Optional.empty() : result;
            }
        }

        result = tableManager.search(key);
        if (result.isPresent()) {
            return result.get() == TOMBSTONE ? Optional.empty() : result;
        }

        return result;
    }

    /**
     * @since 1.0
     */
    @Override
    public void put(ByteBuffer key, byte[] value) {
        // TODO: THIS FUNCTION SHOULD ACQUIRE A LOCK TO ENSURE NO CONCURRENT READ ISSUES.
        try {
            activeLog.append(OpCode.PUT, key, value);
            activeMemTable.put(key, value);

            MemTable table = (MemTable) activeMemTable;
            if (table.getCurrentSizeInBytes() >= FLUSH_CAPACITY) {
                triggerFlush();
            }
        } catch (IOException e) {
            throw new StorageException("Failed to persist data to WAL", e);
        }
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> remove(ByteBuffer key) {
        // TODO: THIS SHOULD ACQUIRE A READ LOCK.
        try {
            Objects.requireNonNull(key, "Key cannot be null");

            Optional<byte[]> previousValue = activeMemTable.get(key);

            activeLog.append(OpCode.DELETE, key, null);
            activeMemTable.put(key, TOMBSTONE);
            return previousValue;
        } catch (IOException e) {
            throw new StorageException("Failed to persist data to WAL", e);
        }
    }

    /**
     * Recovers the WAL log from disk.
     *
     * @throws IOException in case of file read error
     * @see WriteAheadLog#replay(BiConsumer, Consumer)
     */
    private void recover() throws IOException {
        List<Path> logFiles;

        try (Stream<Path> stream = Files.list(this.directory)) {
            logFiles = stream
                    .filter(path -> path.toString().endsWith(".log"))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .toList();
        }

        if (logFiles.isEmpty()) {
            return;
        }

        // TODO: Add logging

        for (Path logPath : logFiles) {
            try (WriteAheadLog recoveryLog = new WriteAheadLog(logPath)) {
                recoveryLog.replay(
                        activeMemTable::put,
                        (key) -> activeMemTable.put(key, TOMBSTONE)
                );
            }
        }
    }

    /**
     * Writes the current {@link MemTable} to disk, and resets state.
     *
     * @throws IOException in case of file read error
     * @see SSTableWriter#writeToFile(ImmutableMap, long)
     */
    void flush() throws IOException {
        // TODO: THIS SHOULD ACQUIRE A WRITE LOCK WHILE MOVING TABLES, AND THEN RELEASE IT.
        MemTable tableToFlush;
        ImmutableMap<ByteBuffer, byte[]> immutableMemTable;
        long currentSeqNum;
        WriteAheadLog oldLog;

        synchronized (this) {
            while (inactiveMemTable != null) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for flush to complete", e);
                }
            }

            this.inactiveMemTable = this.activeMemTable;
            this.activeMemTable = StorageEngines.createMemTable();

            tableToFlush = (MemTable) this.inactiveMemTable;
            immutableMemTable = tableToFlush.immutableCopy();

            currentSeqNum = sstSequenceNumber.getAndIncrement();

            // TODO: Rotate the log instead of clearing
            oldLog = rotateLog();
        }

        Path newSSTPath = tableWriter.writeToFile(immutableMemTable, currentSeqNum);

        synchronized (this) {
            tableManager.registerSSTable(newSSTPath);
            this.inactiveMemTable = null;
            deleteLog(oldLog);
            this.notifyAll();
        }
    }

    /**
     * Helper function to trigger a background flush process.
     */
    private void triggerFlush() {
        this.flushService.submit(() -> {
            try {
                flush();
            } catch (IOException e) {
                // TODO: Log this?
            }
        });
    }

    /**
     * Loads the SST sequence number from disk into memory.
     *
     * @return the sequence number as an {@link AtomicLong}
     * @throws IOException in case of file read error
     */
    private AtomicLong loadSSTSequenceNumber() throws IOException {
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

    /**
     * Loads the log sequence number from disk into memory.
     *
     * @return the sequence number as an {@link AtomicLong}
     * @throws IOException in case of file read error
     */
    private AtomicLong loadLogSequenceNumber() throws IOException {
        try (Stream<Path> stream = Files.list(this.directory)) {
            OptionalLong maxSequence = stream
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.endsWith(".log"))
                    .map(name -> name.replace(".log", ""))
                    .filter(name -> name.matches("\\d+"))
                    .mapToLong(Long::parseLong)
                    .max();

            return new AtomicLong(maxSequence.orElse(0L));
        }
    }

    /**
     * Rotates the current log to a new log, and returns the oldLog.
     *
     * @return The old log
     * @throws IOException In the event of a file read error.
     */
    private WriteAheadLog rotateLog() throws IOException {
        long nextSeq = logSequenceNumber.incrementAndGet();
        String fileName = String.format("%05d.log", nextSeq);
        Path filePath = directory.resolve(fileName);

        WriteAheadLog oldLog;
        synchronized (this) {
            oldLog = this.activeLog;

            this.activeLog = new WriteAheadLog(filePath);
        }

        return oldLog;
    }

    /**
     * Deletes the old log.
     *
     * @param oldLog The old log to be deleted.
     * @throws IOException In the event of a file read error.
     */
    private void deleteLog(WriteAheadLog oldLog) throws IOException {
        if (oldLog != null) {
            try {
                oldLog.close();
                Files.deleteIfExists(oldLog.path());
            } catch (IOException e) {
                // TODO: Log this
            }
        }
    }

    /**
     * Loads the most recent log on disk, or creates a new one if none exist.
     * @return The most recent active log, or a new log if none exist.
     * @throws IOException In the event of a file read error.
     */
    private WriteAheadLog loadLog() throws IOException {
        String fileName = String.format("%05d.log", logSequenceNumber.get());
        Path latestLogPath = directory.resolve(fileName);

        return new WriteAheadLog(latestLogPath);
    }
}