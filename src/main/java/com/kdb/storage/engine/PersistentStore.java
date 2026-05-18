package com.kdb.storage.engine;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.kdb.storage.Store;
import com.kdb.storage.common.OpCode;
import com.kdb.storage.exceptions.StorageException;
import org.jspecify.annotations.NonNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.kdb.storage.common.Serializer.calculateSize;

/**
 * A high-performance, thread-safe, persistent key-value store implementing an LSM-Tree architecture.
 *
 * It coordinates a volatile in-memory layer ({@link MemTable}) with a crash-resilient write-ahead log ({@link WriteAheadLog})
 * and an immutable on-disk storage tier of Sorted String Tables ({@link SSTable}) to optimize write-heavy workloads.
 *
 * <p>To mitigate read amplification and reclaim disk space from duplicate or deleted records, a background compaction engine executes
 * a thread-safe K-Way merge to atomically replace obsolete source files without blocking active readers.
 * </p>
 *
 * <p>It also utilizes a bloom filter under the hood to perform quicker reads for nonexistent keys.</p>
 *
 * @see Store
 * @see MemTable
 * @see WriteAheadLog
 * @see SSTable
 * @see StorageEngines#createPersistentStore(Path)
 * @since 1.0
 */

final class PersistentStore implements Store<ByteBuffer, byte[]>, Closeable {

    private volatile Store<ByteBuffer, byte[]> activeMemTable;
    private volatile Store<ByteBuffer, byte[]> inactiveMemTable;
    private WriteAheadLog activeLog;
    private final SSTableManager tableManager;
    private final SSTableWriter tableWriter;
    private final CompactionManager compactionManager;
    private final AtomicLong sstSequenceNumber;
    private final AtomicLong logSequenceNumber;
    private final AtomicBoolean isFlushQueued;
    private final AtomicBoolean isCompactionQueued;
    private final Path directory;
    private final ExecutorService flushService;
    private final ExecutorService compactService;
    private final ReadWriteLock lock;

    static final byte[] TOMBSTONE = new byte[0];

    // 4 MB flush capacity
    private static final int FLUSH_CAPACITY = 6_000_000;
    private static final int COMPACTION_CAPACITY = 6;

    // 30MB cache
    private static final long CACHE_SIZE_LIMIT = FLUSH_CAPACITY * 5;
    private final Cache<ByteBuffer, byte[]> memCache;


    PersistentStore(Path directory) throws IOException {
        Objects.requireNonNull(directory);
        this.directory = directory;
        this.lock = new ReentrantReadWriteLock();
        this.isFlushQueued = new AtomicBoolean(false);
        this.isCompactionQueued = new AtomicBoolean(false);
        this.sstSequenceNumber = loadSSTSequenceNumber();
        this.logSequenceNumber = loadLogSequenceNumber();

        this.activeMemTable = StorageEngines.createMemTable();
        this.activeLog = loadLog();
        this.tableManager = new SSTableManager(directory);
        this.tableWriter = new SSTableWriter(directory);
        this.compactionManager = new CompactionManager(directory);
        this.flushService = Executors.newSingleThreadExecutor();
        this.compactService = Executors.newSingleThreadExecutor();

        this.memCache = CacheBuilder
                .newBuilder()
                .maximumWeight(CACHE_SIZE_LIMIT)
                .weigher(new Weigher<ByteBuffer, byte[]>() {
                    public int weigh(@NonNull ByteBuffer key, byte @NonNull [] value) {
                        return (int) calculateSize(key, value);
                    }
                })
                .build();

        recover();
        MemTable table = (MemTable) activeMemTable;
        if (table.getCurrentSizeInBytes() >= FLUSH_CAPACITY) {
            triggerFlush();
        }
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> get(ByteBuffer key) {
        lock.readLock().lock();
        try {
            ByteBuffer searchKey = key.duplicate();
            byte[] cachedValue = memCache.getIfPresent(searchKey);
            if (cachedValue != null) {
               return cachedValue == TOMBSTONE ? Optional.empty() : Optional.of(cachedValue);
            }

            Optional<byte[]> result = lookupInStorage(searchKey);
            if (result.isPresent()) {
                memCache.put(searchKey, result.get());
            } else {
                memCache.put(searchKey, TOMBSTONE);
            }
            return result.filter(bytes -> bytes != TOMBSTONE);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @since 1.0
     */
    @Override
    public void put(ByteBuffer key, byte[] value) {
        lock.readLock().lock();
        try {
            activeLog.append(OpCode.PUT, key.duplicate(), value);
            activeMemTable.put(key.duplicate(), value);

            MemTable table = (MemTable) activeMemTable;
            if (table.getCurrentSizeInBytes() >= FLUSH_CAPACITY) {
                triggerFlush();
            }
        } catch (IOException e) {
            throw new StorageException("Failed to persist data to WAL", e);
        } finally {
            lock.readLock().unlock();
            memCache.invalidate(key.duplicate());
        }
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> remove(ByteBuffer key) {
        lock.readLock().lock();
        try {
            Objects.requireNonNull(key, "Key cannot be null");

            Optional<byte[]> previousValue = activeMemTable.get(key.duplicate());

            activeLog.append(OpCode.DELETE, key.duplicate(), null);
            activeMemTable.put(key.duplicate(), TOMBSTONE);
            return previousValue;
        } catch (IOException e) {
            throw new StorageException("Failed to persist data to WAL", e);
        } finally {
            lock.readLock().unlock();
            memCache.invalidate(key.duplicate());
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
     * Looks up a given key across all storage areas.
     *
     * @param key The key to be searched for.
     * @return The value, if exists, or an Optional empty if not exists.
     */
    private Optional<byte[]> lookupInStorage(ByteBuffer key) {
        Optional<byte[]> result = activeMemTable.get(key);
        if (result.isPresent()) return result;

        if (inactiveMemTable != null) {
            result = inactiveMemTable.get(key);
            if (result.isPresent()) return result;
        }

        return tableManager.search(key);
    }

    /**
     * Writes the current {@link MemTable} to disk, and resets state.
     *
     * @throws IOException in case of file read error
     * @see SSTableWriter#writeToFile(ImmutableMap, long)
     */
    void flush() throws IOException {
        if (inactiveMemTable != null) {
            return;
        }

        MemTable tableToFlush;
        long currentSeqNum;
        WriteAheadLog oldLog;

        lock.writeLock().lock();
        try {
            if (this.inactiveMemTable != null) return;
            this.inactiveMemTable = this.activeMemTable;
            this.activeMemTable = StorageEngines.createMemTable();

            tableToFlush = (MemTable) this.inactiveMemTable;
            currentSeqNum = sstSequenceNumber.getAndIncrement();

            oldLog = rotateLog();

            isFlushQueued.set(false);
        } finally {
            lock.writeLock().unlock();
        }

        try {
            ImmutableMap<ByteBuffer, byte[]> immutableMemTable = tableToFlush.immutableCopy();
            Path newSSTPath = tableWriter.writeToFile(immutableMemTable, currentSeqNum);

            lock.writeLock().lock();
            try {
                tableManager.registerSSTable(newSSTPath);
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            this.inactiveMemTable = null;
            deleteLog(oldLog);

            int numTables = tableManager.tables().size();
            if (numTables >= COMPACTION_CAPACITY) {
                triggerCompaction();
            }
        }
    }

    /**
     * Helper function to trigger a background flush process.
     */
    private void triggerFlush() {
        if (isFlushQueued.compareAndSet(false, true)) {
            this.flushService.submit(() -> {
                try {
                    flush();
                } catch (Throwable t) {
                    System.err.println("CRITICAL: Background flush crashed - " + t.getMessage());
                }
            });
        }
    }

    /**
     * Helper function to trigger a background compaction process.
     */
    private void triggerCompaction() {
        if (isCompactionQueued.compareAndSet(false, true)) {
            this.compactService.submit(() -> {
                try {
                    SSTable newTable = compactionManager.compact(tableManager.tables());
                    tableManager.replaceCompactedTables(tableManager.tables(), newTable);
                } catch (Throwable t) {
                    System.err.println("CRITICAL: Background compaction crashed - " + t.getMessage());
                } finally {
                    isCompactionQueued.set(false);
                }
            });
        }
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
     *
     * @return The most recent active log, or a new log if none exist.
     * @throws IOException In the event of a file read error.
     */
    private WriteAheadLog loadLog() throws IOException {
        String fileName = String.format("%05d.log", logSequenceNumber.get());
        Path latestLogPath = directory.resolve(fileName);

        return new WriteAheadLog(latestLogPath);
    }

    @Override
    public void close() {
        this.flushService.shutdown();
        this.compactService.shutdown();
        try {
            if (!this.flushService.awaitTermination(2, TimeUnit.SECONDS)) {
                this.flushService.shutdownNow();
            }
            if (!this.compactService.awaitTermination(2, TimeUnit.SECONDS)) {
                this.compactService.shutdownNow();
            }
        } catch (InterruptedException e) {
            this.compactService.shutdownNow();
            this.flushService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            if (activeLog != null) {
                activeLog.close();
            }

            this.tableManager.close();
        } catch (IOException e) {
            // TODO: Log this error
        }
    }
}