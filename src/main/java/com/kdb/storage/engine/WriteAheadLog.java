package com.kdb.storage.engine;

import com.kdb.storage.common.OpCode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
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
public final class WriteAheadLog implements AutoCloseable {
    private final Path filePath;
    private final FileChannel channel;

    private final int OPCODE_LENGTH = 1;
    private final int SIZE_BUFFER_LENGTH = 4;

    public WriteAheadLog(Path filePath) throws IOException {
        Objects.requireNonNull(filePath);
        this.filePath = filePath;
        this.channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    /**
     * Appends an operation to the log file.
     *
     * @param opCode defines the operation type based on {@link OpCode}
     * @param key defines the key being used in the operation
     * @param value defines the value being used in the operation
     * @throws IOException thrown in case of error reading file
     */
    public synchronized void append(OpCode opCode, ByteBuffer key, byte[] value) throws IOException {
        int keySize = key.remaining();
        int totalSize = OPCODE_LENGTH + SIZE_BUFFER_LENGTH + keySize;

        if (opCode.equals(OpCode.PUT)) {
            totalSize += SIZE_BUFFER_LENGTH + value.length;
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

    /**
     * Replays all in-memory operations occurred prior to flush.
     *
     * <p>Ensures crash recovery for {@link com.kdb.storage.engine.MemTable},
     * equipping the {@link com.kdb.storage.engine.PersistentStore} efficient data security.</p>
     *
     * @param put defines the put method that will be called
     * @param remove defines the remove method that will be called
     * @throws IOException throws in the event of a file read error
     */
    public synchronized void replay(BiConsumer<ByteBuffer, byte[]> put, Consumer<ByteBuffer> remove) throws IOException {

        try (FileChannel readChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            // header == 5 bytes, opcode + keySize
            int headerSize = OPCODE_LENGTH + SIZE_BUFFER_LENGTH;
            ByteBuffer header = ByteBuffer.allocate(headerSize);

            while (readChannel.read(header) != -1) {
                header.flip();
                byte opCode = header.get();
                int keySize = header.getInt();

                ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                while (keyBuffer.hasRemaining()) {
                    readChannel.read(keyBuffer);
                }
                keyBuffer.flip();

                if (opCode == OpCode.PUT.getCode()) {
                    ByteBuffer valueSizeBuffer = ByteBuffer.allocate(SIZE_BUFFER_LENGTH);
                    while (valueSizeBuffer.hasRemaining()) {
                        readChannel.read(valueSizeBuffer);
                    }
                    valueSizeBuffer.flip();
                    int valueSize = valueSizeBuffer.getInt();

                    ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                    while (valueBuffer.hasRemaining()) {
                        readChannel.read(valueBuffer);
                    }

                    valueBuffer.flip();

                    put.accept(keyBuffer, valueBuffer.array());
                } else {
                    remove.accept(keyBuffer);
                }
                header.clear();
            }
        } catch (NoSuchFileException _) {
           // Note: Should only occur on first boot. (log doesn't exist yet)
        } catch (IOException e) {
            throw new RuntimeException("Failed to read WAL during recovery", e);
        }
    }

    public synchronized void clear() throws IOException {
        channel.truncate(0);
    }

    @Override
    public void close() throws Exception {
        channel.close();
    }
}