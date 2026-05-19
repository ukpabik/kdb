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
    private final long fileReadLimit;
    private final ByteBuffer buffer;

    SSTableIterator(SSTable table, long fileLimit) throws IOException {
        this.buffer = table.dataBuffer();
        fileReadLimit = fileLimit;
        this.buffer.position(0);
    }

    @Override
    public boolean hasNext() {
        return this.buffer.position() < fileReadLimit;
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
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements left in iterator.");
        }

        int kSize = buffer.getInt();

        ByteBuffer keyBytes = buffer.slice();
        keyBytes.limit(kSize);
        buffer.position(buffer.position() + kSize);

        int vSize = buffer.getInt();

        ByteBuffer valueBytes = buffer.slice();
        valueBytes.limit(vSize);
        buffer.position(buffer.position() + vSize);

        return new KVPair(keyBytes, valueBytes);
    }
}
