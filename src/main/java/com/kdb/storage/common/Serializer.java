package com.kdb.storage.common;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A serialization helper providing the fundamental layout serialization format
 * for all disk-bound data blocks within KDB.
 *
 * <p>{@link Serializer} marshals internal references into tightly structured, predictable byte streams.
 * It underpins the structural stability of the write path, ensuring binary format consistency across
 * components such as logs, active data segment blocks, and sparse indices.</p>
 *
 * <h3>Standard Variable-Length Record Layout:</h3>
 * <pre>
 * [4 bytes: Key Size (int)]
 * [N bytes: Raw Key Data Payloads]
 * [4 bytes: Value Size (int)]
 * [M bytes: Raw Value Data]
 * </pre>
 */
public abstract class Serializer {

    /**
     * Serializes a variable-length key and value into a single, self-contained {@link ByteBuffer}.
     *
     * <p>The returning buffer is automatically flipped and positioned at 0, ready to be channeled
     * to a file system or active append stream without requiring further pointer adjustment.</p>
     *
     * @param key   The unique identifier byte window mapping this entry; cannot be null.
     * @param value The raw byte data mapped to this unique key entity window; cannot be null.
     * @return A newly allocated, flipped byte buffer containing packed sequence structures.
     * @throws NullPointerException If either the supplied key or value maps to a null reference pointer.
     */
    public static ByteBuffer serialize(ByteBuffer key, ByteBuffer value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        int totalSize = (Integer.BYTES * 2) + key.remaining() + value.remaining();

        ByteBuffer result = ByteBuffer.allocate(totalSize);

        result.putInt(key.remaining());
        result.put(key.duplicate());

        result.putInt(value.remaining());
        result.put(value.duplicate());

        result.flip();
        return result;
    }


    /**
     * Overloaded serialization format transforming a key and byte array payload structure.
     *
     * @param key   The unique identifier byte window mapping this entry; cannot be null.
     * @param value The arbitrary raw array layout mapped to this frame key; cannot be null.
     * @return A newly allocated, flipped byte buffer containing packed sequence structures.
     * @throws NullPointerException If either the supplied key or array value maps to a null reference.
     */
    public static ByteBuffer serialize(ByteBuffer key, byte[] value) {
        return serialize(key, ByteBuffer.wrap(value));
    }

    /**
     * Overloaded serialization format transforming a key and a primitive file pointer offset.
     *
     * <p>This specialized format is primarily utilized by background file compactor threads and
     * indexing managers to map dictionary frames to concrete physical addresses on disk.</p>
     *
     * @param key    The structural lookup anchor segment being indexed; cannot be null.
     * @param offset The absolute physical byte coordinate mapping to this key frame inside an SST file.
     * @return A newly allocated, flipped byte buffer containing exactly 8 value payload bytes.
     * @throws NullPointerException If the supplied index anchor key references a null pointer.
     */
    public static ByteBuffer serialize(ByteBuffer key, long offset) {
        ByteBuffer valueBuffer = ByteBuffer.allocate(Long.BYTES).putLong(offset);
        valueBuffer.flip();
        return serialize(key, valueBuffer);
    }
}
