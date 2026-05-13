package com.kdb.storage.engine;

import com.kdb.storage.exceptions.CorruptFileException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.kdb.storage.engine.SSTable.MAGIC_NUMBER;
import static com.kdb.storage.engine.SSTableWriter.INDEX_BUFFER_LENGTH;
import static java.nio.file.StandardOpenOption.READ;

final class SSTableManager {

    private final Path directoryPath;
    private final List<SSTable> tables;
    
    SSTableManager(Path directory) throws IOException {
        this.directoryPath = directory;

        this.tables = loadTables();
    }


    Optional<byte[]> search(ByteBuffer key) {
        for (SSTable table : tables) {
            try {
                Optional<byte[]> result = table.search(key);

                if (result.isPresent()) {
                    return result;
                }
            } catch (IOException e) {
                // TODO: Log error
            }
        }
        return Optional.empty();
    }

    void registerSSTable(Path sstPath) {
        Optional<SSTable> registeredTable = tryLoad(sstPath);

        registeredTable.ifPresent(this.tables::add);
    }

    private List<SSTable> loadTables() throws IOException {
        try(Stream<Path> stream = Files.list(directoryPath)) {
            return stream
                    .filter(path -> path.toString().endsWith(".sst"))
                    .map(this::tryLoad)
                    .flatMap(Optional::stream)
                    .sorted(Comparator.comparing(SSTable::path).reversed())
                    .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
        }
    }

    private Optional<SSTable> tryLoad(Path filePath) {
        try {
            return Optional.of(load(filePath));
        } catch(CorruptFileException e) {
            handleCorruptFile(filePath);
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException("Error reading .sst file", e);
        }
    }

    private SSTable load(Path sstPath) throws IOException {
        Map<ByteBuffer, Long> indexMap = new TreeMap<>();

        try (FileChannel fc = FileChannel.open(sstPath, READ)) {
            fc.position(fc.size() - INDEX_BUFFER_LENGTH);

            ByteBuffer footerBytes = ByteBuffer.allocate(INDEX_BUFFER_LENGTH);
            fc.read(footerBytes);
            footerBytes.flip();

            long indexOffset = footerBytes.getLong();
            long indexSize = footerBytes.getLong();
            int magicNumber = footerBytes.getInt();

            if (magicNumber != MAGIC_NUMBER) {
                throw new CorruptFileException("This SST file is corrupted.");
            }

            fc.position(indexOffset);

            long bytesRead = 0;
            while(bytesRead < indexSize) {
                ByteBuffer keySize = ByteBuffer.allocate(Integer.BYTES);
                bytesRead += keySize.remaining();
                fc.read(keySize);
                keySize.flip();

                ByteBuffer keyBytes = ByteBuffer.allocate(keySize.getInt());
                bytesRead += keyBytes.remaining();
                fc.read(keyBytes);
                keyBytes.flip();

                ByteBuffer valueSize = ByteBuffer.allocate(Integer.BYTES);
                bytesRead += valueSize.remaining();
                fc.read(valueSize);
                valueSize.flip();

                ByteBuffer valueBytes = ByteBuffer.allocate(valueSize.getInt());
                bytesRead += valueBytes.remaining();
                fc.read(valueBytes);
                valueBytes.flip();

                ByteBuffer keyToStore = ByteBuffer.allocate(keyBytes.remaining());
                keyToStore.put(keyBytes);
                keyToStore.flip();

                indexMap.put(keyToStore, valueBytes.getLong());
            }
        }

        return new SSTable(sstPath, indexMap);
    }

    private void handleCorruptFile(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            // TODO: Log this??
        }
    }

    List<SSTable> tables() {
        return tables;
    }
}
