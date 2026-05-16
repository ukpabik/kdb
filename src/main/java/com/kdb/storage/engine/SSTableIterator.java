package com.kdb.storage.engine;

import com.kdb.storage.common.KVPair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class SSTableIterator implements Iterator<KVPair>, AutoCloseable {
    private final FileChannel readChannel;
    private final long fileReadLimit;
    private long currentOffset = 0;

    SSTableIterator(SSTable table, long fileLimit) throws IOException {
        Path filePath = table.path();
        readChannel = FileChannel.open(filePath, StandardOpenOption.READ);
        fileReadLimit = fileLimit;
    }

    @Override
    public boolean hasNext() {
        return this.currentOffset < fileReadLimit;
    }

    /**
     * @return A {@link ByteBuffer} containing the key size, key, value size, and value of the next key-value pair in the
     * SSTable.
     */
    @Override
    public KVPair next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements left in iterator.");
        }
        try {
            ByteBuffer keySizeBuf = ByteBuffer.allocate(Integer.BYTES);
            this.readChannel.read(keySizeBuf, currentOffset);
            keySizeBuf.flip();
            int kSize = keySizeBuf.getInt();
            currentOffset += Integer.BYTES;

            ByteBuffer keyBytes = ByteBuffer.allocate(kSize);
            this.readChannel.read(keyBytes, currentOffset);
            keyBytes.flip();
            currentOffset += kSize;

            ByteBuffer valueSizeBuf = ByteBuffer.allocate(Integer.BYTES);
            this.readChannel.read(valueSizeBuf, currentOffset);
            valueSizeBuf.flip();
            int vSize = valueSizeBuf.getInt();
            currentOffset += Integer.BYTES;

            ByteBuffer valueBytes = ByteBuffer.allocate(vSize);
            this.readChannel.read(valueBytes, currentOffset);
            valueBytes.flip();
            currentOffset += vSize;

            return new KVPair(keySizeBuf, keyBytes, valueSizeBuf, valueBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        this.readChannel.close();
    }
}
