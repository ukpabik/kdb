package com.kdb.storage.engine;

import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;

final class SSTableReader {

    SSTableReader() {
    }

    SSTable load() {
       return new SSTable(Path.of(""), Map.of());
    }
}
