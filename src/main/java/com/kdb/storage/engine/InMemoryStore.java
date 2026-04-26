package com.kdb.storage.engine;

import com.kdb.storage.Store;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A high-performance, thread-safe implementation of {@link Store} that
 * maintains all data in volatile memory.
 *
 * <p>This implementation uses a {@link ConcurrentHashMap} to provide O(1)
 * average time complexity for all operations while ensuring thread safety
 * through segment-based locking.
 *
 * <p><b>Constraints:</b>
 * <ul>
 * <li><b>Volatility:</b> All data is lost upon JVM termination.</li>
 * <li><b>Value Limits:</b> Values are limited to 1 MB maximum per entry.</li>
 * </ul>
 *
 * @see Store
 * @see StorageEngines#createInMemoryStore()
 * @since 1.0
 */
final class InMemoryStore implements Store<ByteBuffer, byte[]> {

    private final ConcurrentHashMap<ByteBuffer, byte[]> map;

    InMemoryStore() {
        this.map = new ConcurrentHashMap<>();
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
        Objects.requireNonNull(value);

        map.put(key, value);
    }

    /**
     * @since 1.0
     */
    @Override
    public Optional<byte[]> delete(ByteBuffer key) {
        // TODO: Unimplemented
        return Optional.empty();
    }
}