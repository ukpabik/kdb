package com.kdb.storage;

import java.util.Optional;

/**
 * A simple implementation of a key-value store. Entries are typically
 * inserted with {@link #put(Object, Object)} and retrieved with
 * {@link #get(Object)}. Entries are removed through {@link #remove(Object)}.
 *
 * <p>Implementations of this interface are expected to be thread-safe, and
 * able to be accessed by multiple threads.</p>
 *
 * @param <K> the type of the store's keys, which cannot be null
 * @param <V> the type of the store's values, which cannot be null
 *
 * @since 1.0
 */
public interface Store<K, V> {

    /**
     * Returns the value of a specified key.
     *
     * @throws NullPointerException if a null key has been passed in
     *
     * @since 1.0
     */
    Optional<V> get(K key);

    /**
     * Places the specified value (or updates if exists) at a specified key.
     *
     * @throws NullPointerException if a null key or value has been passed in
     *
     * @since 1.0
     */
    void put(K key, V value);

    /**
     * Deletes a specified key from the Store and returns the previous
     * value of that key (or an empty optional if non-existing).
     *
     * @throws NullPointerException if a null key has been passed in
     *
     * @since 1.0
     */
    Optional<V> remove(K key);
}