package com.kdb.storage.engine;

import com.google.common.collect.ImmutableSortedMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.kdb.storage.engine.SSTableWriter.INDEX_BUFFER_LENGTH;

final class SSTable {

    // Used for indicating a file is a .sst file.
    static final int MAGIC_NUMBER = 0x4B444249;

    private final Path filePath;
    private final ImmutableSortedMap<ByteBuffer, Long> sparseIndex;


    SSTable(Path path, Map<ByteBuffer, Long> index) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(index);
        filePath = path;
        sparseIndex = ImmutableSortedMap.copyOf(index);
    }

    Path path() {
        return this.filePath;
    }

    Optional<byte[]> search(ByteBuffer key) throws IOException {
        Map.Entry<ByteBuffer, Long> indexEntry = this.sparseIndex.floorEntry(key);

        if (indexEntry == null) {
            return Optional.empty();
        }

        long offset = indexEntry.getValue();

        try (FileChannel fc = FileChannel.open(this.filePath, StandardOpenOption.READ)) {
            fc.position(offset);

            while (fc.position() < fc.size() - INDEX_BUFFER_LENGTH) {
                ByteBuffer keySize = ByteBuffer.allocate(Integer.BYTES);
                fc.read(keySize);
                keySize.flip();

                ByteBuffer keyBytes = ByteBuffer.allocate(keySize.getInt());
                fc.read(keyBytes);
                keyBytes.flip();

                ByteBuffer valueSize = ByteBuffer.allocate(Integer.BYTES);
                fc.read(valueSize);
                valueSize.flip();

                int compare = key.compareTo(keyBytes);
                if (compare == 0) {
                    ByteBuffer valueBytes = ByteBuffer.allocate(valueSize.getInt());
                    fc.read(valueBytes);
                    valueBytes.flip();

                    return Optional.of(valueBytes.array());
                } else if (compare > 0) {
                    fc.position(fc.position() + valueSize.getInt());
                } else {
                    break;
                }
            }
        }

        return Optional.empty();
    }
}