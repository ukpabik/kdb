package com.kdb.storage.engine;

import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

final class SSTable {

    // Used for indicating a file is a .sst file.
    static final int MAGIC_NUMBER = 0x4B444249;

    private final Path filePath;
    private final ImmutableMap<ByteBuffer, Long> sparseIndex;


    SSTable(Path path, Map<ByteBuffer, Long> index) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(index);
        filePath = path;
        sparseIndex = ImmutableMap.copyOf(index);
    }

    Path path() {
        return this.filePath;
    }
}