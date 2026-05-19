package com.kdb.storage.engine;

import com.kdb.storage.common.OpCode;
import com.kdb.storage.common.SafeReadWrite;
import com.kdb.storage.exceptions.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    private final ScheduledExecutorService syncService;

    public WriteAheadLog(Path filePath) throws IOException {
        Objects.requireNonNull(filePath);
        this.filePath = filePath;
        this.channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        this.syncService = Executors.newSingleThreadScheduledExecutor();

        this.syncService.scheduleAtFixedRate(() -> {
            try {
                if (channel.isOpen()) {
                    channel.force(true);
                }
            } catch (IOException e) {
                // TODO: Log this?
            }
        }, 50, 50, TimeUnit.MILLISECONDS);
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
        int totalSize = Byte.BYTES + Integer.BYTES + keySize;

        if (opCode.equals(OpCode.PUT)) {
            totalSize += Integer.BYTES + value.length;
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
        SafeReadWrite.writeFully(channel, serialized);
    }

    /**
     * Replays all in-memory operations occurred prior to flush.
     *
     * <p>Ensures crash recovery for {@link com.kdb.storage.engine.MemTable},
     * equipping the {@link com.kdb.storage.engine.PersistentStore} efficient data security.</p>
     *
     * @param put defines the put method that will be called
     * @param remove defines the remove method that will be called
     * @throws RuntimeException throws in the event of a file read error
     */
    public synchronized void replay(BiConsumer<ByteBuffer, byte[]> put, Consumer<ByteBuffer> remove) {

        try (FileChannel readChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = readChannel.size();
            MappedByteBuffer walBuffer = readChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

            // header == 5 bytes, opcode + keySize
            int headerSize = Byte.BYTES + Integer.BYTES;
            while (walBuffer.remaining() >= headerSize) {
                walBuffer.mark();

                byte opCode = walBuffer.get();
                int keySize = walBuffer.getInt();

                if (walBuffer.remaining() < keySize) {
                    break;
                }

                ByteBuffer keyBuffer = walBuffer.slice();
                keyBuffer.limit(keySize);
                walBuffer.position(walBuffer.position() + keySize);
                if (opCode == OpCode.PUT.getCode()) {
                    if (walBuffer.remaining() < Integer.BYTES) {
                        break;
                    }
                    int valueSize = walBuffer.getInt();

                    if (walBuffer.remaining() < valueSize) {
                        break;
                    }

                    byte[] valueBytes = new byte[valueSize];
                    walBuffer.get(valueBytes);
                    put.accept(keyBuffer, valueBytes);
                } else {
                    remove.accept(keyBuffer);
                }
            }
        } catch (NoSuchFileException _) {
           // Note: Should only occur on first boot. (log doesn't exist yet)
        } catch (IOException e) {
            throw new StorageException("Failed to read WAL during recovery", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            channel.force(true);
            channel.close();
        }

        try {
            if (!this.syncService.awaitTermination(20, TimeUnit.SECONDS)) {
                this.syncService.shutdownNow();
            }
        } catch (InterruptedException e) {
            this.syncService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    Path path() {
        return this.filePath;
    }
}