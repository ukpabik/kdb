package com.kdb.storage.persistence;

import com.kdb.storage.common.OpCode;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A write ahead log implementation used for crash resiliency.
 *
 * <p>Serialized Structure:
 *  [1 byte: opcode]
 *  [4 bytes: size of key]
 *  [N bytes: key]
 *  [4 bytes: size of value]
 *  [M bytes: value]</p>
 *
 * @see com.kdb.storage.engine.StorageEngines#createPersistentStore(Path)
 */
public final class WriteAheadLog {
    private final Path filePath;
    private final FileChannel channel;

    public WriteAheadLog(Path filePath) throws IOException {
        this.filePath = filePath;
        this.channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    public void append(OpCode opCode, ByteBuffer key, byte[] value) throws IOException {
        int keySize = key.remaining();
        int totalSize = 1 + 4 + keySize;

        if (opCode.equals(OpCode.PUT)) {
            totalSize += 4 + value.length;
        }

        ByteBuffer serialized = ByteBuffer.allocate(totalSize);
        serialized.put(opCode.getCode());
        serialized.putInt(keySize);
        serialized.put(key.duplicate());

        if (opCode.equals(OpCode.PUT)) {
            serialized.putInt(value.length);
            serialized.put(value);
        }

        serialized.flip();
        while (serialized.hasRemaining()) {
            channel.write(serialized);
        }

        channel.force(true);
    }

    public void replay(BiConsumer<ByteBuffer, byte[]> put, Consumer<ByteBuffer> remove) throws IOException {
        // TODO: Open the file for reading, read, and perform either put or delete
    }

    public void clear() {
        // TODO: unimplemented
    }
}