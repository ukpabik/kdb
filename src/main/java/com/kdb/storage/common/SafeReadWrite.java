package com.kdb.storage.common;

import com.kdb.storage.exceptions.CorruptFileException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class provides safe read and write methods when reading from or writing to SSTables.
 *
 * @see com.kdb.storage.engine.SSTableWriter
 * @see com.kdb.storage.engine.SSTableManager
 */
public abstract class SafeReadWrite {

    public static void writeFully(FileChannel fc, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            fc.write(buf);
        }
    }

    public static void readFully(FileChannel fc, ByteBuffer buf, long offset) throws IOException {
        long currentOffset = offset;

        while (buf.hasRemaining()) {
            int bytesRead = fc.read(buf, currentOffset);
            if (bytesRead == -1) {
                throw new CorruptFileException("Unexpected EOF reached while reading file.");
            }

            currentOffset += bytesRead;
        }
    }
}
