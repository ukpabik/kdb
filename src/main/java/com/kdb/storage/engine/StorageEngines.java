package com.kdb.storage.engine;

import com.kdb.storage.Store;

import java.nio.ByteBuffer;

/**
 * A factory class for creating instances of Store types. A typical
 * usage of this class would be {@link #createMemTable()}, which returns
 * an in-memory, thread-safe key value store.
 *
 * <p>When creating a static factory method, ensure that the Store implementation
 * is thread-safe and follows all paradigms of the interface.</p>
 *
 * @see MemTable
 *
 * @since 1.0
 */
public final class StorageEngines {
    private StorageEngines() {};

    /**
     * Creates and returns a new thread-safe, in-memory {@link Store}.
     * <p>This implementation is backed by a ConcurrentSkipListMap and is
     * optimized for high-concurrency environments.</p>
     *
     * @return a new instance of an in-memory Store
     * @see MemTable
     */
    public static Store<ByteBuffer, byte[]> createMemTable() {
        return new MemTable();
    }
}