package com.kdb.storage.engine;

import com.kdb.storage.common.KVPair;
import com.kdb.storage.common.SafeReadWrite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A heavy-duty, sequential streaming iterator used to scan the binary data blocks of an {@link SSTable}.
 *
 * <p>This iterator reads key-value records sequentially directly from disk via an underlying
 * {@link FileChannel}. It is designed for maximum memory efficiency, avoiding loading whole files
 * into heap memory, which makes it the core streaming engine for background compaction merges.</p>
 *
 * <h3>Expected Binary Layout Block:</h3>
 * <pre>
 * [4 bytes: Key Size (int)]
 * [N bytes: Raw Key Data]
 * [4 bytes: Value Size (int)]
 * [M bytes: Raw Value Data]
 * </pre>
 *
 * @see SSTable
 * @see CompactionManager
 * @see KVPair
 */

final class SSTableIterator implements Iterator<KVPair>{
    private final FileChannel channel;
    private final long fileReadLimit;
    private long currentPosition;

    SSTableIterator(SSTable table, long fileLimit) throws IOException {
        this.channel = table.channel();
        fileReadLimit = fileLimit;
        this.currentPosition = 0;
    }

    @Override
    public boolean hasNext() {
        return currentPosition < fileReadLimit;
    }

    /**
     * @return A {@link ByteBuffer} containing the key size, key, value size, and value of the next key-value pair in the
     * SSTable.
     * @throws NoSuchElementException If called when the data block boundary has already been fully exhausted.
     * @throws RuntimeException Wrapping an underlying {@link IOException} if an absolute physical seek
     * or buffer fill operation encounters a hardware failure.
     */
    @Override
    public KVPair next() {
        if (!hasNext()) throw new NoSuchElementException();

        try {
            // Read key size
            ByteBuffer sizeBuf = ByteBuffer.allocate(Integer.BYTES);
            channel.read(sizeBuf, currentPosition);
            sizeBuf.flip();
            int kSize = sizeBuf.getInt();
            currentPosition += Integer.BYTES;

            // Read key
            ByteBuffer keyBuf = ByteBuffer.allocate(kSize);
            channel.read(keyBuf, currentPosition);
            keyBuf.flip();
            currentPosition += kSize;

            // Read value size
            sizeBuf.clear();
            channel.read(sizeBuf, currentPosition);
            sizeBuf.flip();
            int vSize = sizeBuf.getInt();
            currentPosition += Integer.BYTES;

            // Read value
            ByteBuffer valBuf = ByteBuffer.allocate(vSize);
            channel.read(valBuf, currentPosition);
            valBuf.flip();
            currentPosition += vSize;

            return new KVPair(keyBuf, valBuf);
        } catch (IOException e) {
            throw new RuntimeException("IO error during iteration", e);
        }
    }
}
