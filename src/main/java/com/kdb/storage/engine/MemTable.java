package com.kdb.storage.engine;

import com.google.common.collect.ImmutableMap;
import com.kdb.storage.Store;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.kdb.storage.engine.PersistentStore.TOMBSTONE;

/**
 * A high-performance, thread-safe implementation of {@link Store} that
 * maintains all data in volatile memory.
 *
 * <p>This implementation uses a {@link java.util.concurrent.ConcurrentSkipListMap} to provide O(log N)
 * average time complexity for all operations while ensuring sorted behavior and thread safety
 * through non-blocking operations.
 *
 * <p><b>Constraints:</b>
 * <ul>
 * <li><b>Volatility:</b> All data is lost upon JVM termination.</li>
 * <li><b>Value Limits:</b> Values are limited to 1 MB maximum per entry.</li>
 * </ul>
 *
 * @see Store
 * @see StorageEngines#createMemTable()
 * @since 1.0
 */
final class MemTable implements Store<ByteBuffer, byte[]> {

    // Estimate for JVM overhead per map entry.
    private static final int ESTIMATED_OVERHEAD_BYTES = 64;

    private final ConcurrentSkipListMap<ByteBuffer, byte[]> map;
    private final AtomicLong currentSizeInBytes;

    MemTable() {
        this.map = new ConcurrentSkipListMap<>();
        this.currentSizeInBytes = new AtomicLong(0);
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> get(ByteBuffer key) {
        Objects.requireNonNull(key);

        byte[] value = map.get(key);
        if (value == TOMBSTONE || value == null) {
            return Optional.empty();
        }

        return Optional.of(value);
    }

    /**
     * @since 1.0
     */
    @Override
    public void put(ByteBuffer key, byte[] value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        byte[] oldValue = map.put(key, value);

        long newEntrySize = calculateSize(key, value);
        if (oldValue != null) {
            currentSizeInBytes.addAndGet(newEntrySize - calculateSize(key, oldValue));
        } else {
            // New entry
            currentSizeInBytes.addAndGet(newEntrySize);
        }
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> remove(ByteBuffer key) {
        Objects.requireNonNull(key);

        byte[] oldValue = map.put(key, TOMBSTONE);
        long tombstoneSize = calculateSize(key, TOMBSTONE);

        if (oldValue != null) {
            currentSizeInBytes.addAndGet(tombstoneSize - calculateSize(key, oldValue));
        } else {
            currentSizeInBytes.addAndGet(tombstoneSize);
        }

        return (oldValue == null || oldValue == TOMBSTONE)
                ? Optional.empty()
                : Optional.of(oldValue);
        }

    public long getCurrentSizeInBytes() {
       return this.currentSizeInBytes.get();
    }

    ImmutableMap<ByteBuffer, byte[]> immutableCopy() {
        return ImmutableMap.copyOf(map);
    }

    void clear() {
        this.currentSizeInBytes.set(0);
        this.map.clear();
    }

    private long calculateSize(ByteBuffer key, byte[] value) {
        return key.remaining() + value.length + ESTIMATED_OVERHEAD_BYTES;
    }
}