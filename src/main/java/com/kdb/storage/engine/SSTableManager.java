package com.kdb.storage.engine;

import java.nio.file.Path;
import java.util.Map;

final class SSTableManager {

    private final Path directoryPath;
    SSTableManager(Path directory) {
        this.directoryPath = directory;
    }

    SSTable load() {
       return new SSTable(Path.of(""), Map.of());
    }
}
