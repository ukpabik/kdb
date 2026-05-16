package com.kdb.storage.common;

import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class Serializer {
    /**
     * Helper function to serialize a key and value into a single {@link ByteBuffer}.
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
     * Overloaded helper function for serializing different values.
     */
    public static ByteBuffer serialize(ByteBuffer key, byte[] value) {
        return serialize(key, ByteBuffer.wrap(value));
    }

    /**
     * Overloaded helper function for serializing different values.
     */
    public static ByteBuffer serialize(ByteBuffer key, long offset) {
        ByteBuffer valueBuffer = ByteBuffer.allocate(Long.BYTES).putLong(offset);
        valueBuffer.flip();
        return serialize(key, valueBuffer);
    }
}
