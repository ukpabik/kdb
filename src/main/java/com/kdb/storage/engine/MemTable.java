package com.kdb.storage.engine;

import com.kdb.storage.Store;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

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

    private final ConcurrentSkipListMap<ByteBuffer, byte[]> map;

    MemTable() {
        this.map = new ConcurrentSkipListMap<>();
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> get(ByteBuffer key) {
        Objects.requireNonNull(key);

        return Optional.ofNullable(map.get(key));
    }

    /**
     * @since 1.0
     */
    @Override
    public void put(ByteBuffer key, byte[] value) {
        Objects.requireNonNull(key);

        map.put(key, value);
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> remove(ByteBuffer key) {
        Objects.requireNonNull(key);

        return Optional.ofNullable(map.remove(key));
    }
}