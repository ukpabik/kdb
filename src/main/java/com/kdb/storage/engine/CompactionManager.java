package com.kdb.storage.engine;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.kdb.storage.common.KVPair;
import com.kdb.storage.common.SafeReadWrite;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static com.kdb.storage.common.Serializer.serialize;
import static com.kdb.storage.engine.SSTable.MAGIC_NUMBER;
import static com.kdb.storage.engine.SSTableWriter.INDEX_BUFFER_LENGTH;
import static com.kdb.storage.engine.SSTableWriter.INDEX_SEGMENT;
import static java.nio.file.StandardOpenOption.*;

/**
 *  Manages and coordinates the background compaction process for the storage engine.
 *
 *  <p>Compaction is essentially the garbage collection system of an LSM-Tree. As data flushes
 *  from volatile memory into immutable {@link SSTable} files, keys become fragmented and obsolete
 *  values accumulate across multiple files. This manager performs an asynchronous K-Way Merge
 *  to reconcile duplicates and consolidate disk space.</p>
 *
 * @see SSTable
 * @see SSTableIterator
 */
final class CompactionManager {
    private final Path directory;

    /**
     * Comparator for sorting {@link MergeNode} entries within the min-heap.
     * <p>Primary Sort (Ascending): Lexicographical comparison of byte keys.</p>
     * <p>Secondary Sort (Descending): Chronological sorting via the sequence number.
     * Higher sequence numbers (newer records) take structural priority during data collisions.</p>
     */
    private final Comparator<MergeNode> mergeNodeComparator = Comparator
            .comparing((MergeNode node) -> node.pair().key())
            .thenComparing(MergeNode::sequenceNumber, Collections.reverseOrder());

    private record MergeNode(KVPair pair, long sequenceNumber, SSTableIterator iterator) {
    }

    CompactionManager(Path directory) {
        this.directory = directory;
    }

    /**
     * Executes a compaction run by merging a collection of source SSTables into a single, highly optimized target file.
     *
     * @param immutableTableList The list of active SSTables, ordered by creation date.
     * @throws IOException In case of file read error.
     */
    SSTable compact(List<SSTable> immutableTableList) throws IOException {
        ByteBuffer lastWrittenKey = null;
        boolean isFirstKey = true;
        Path compactionFile = directory.resolve("tmpCompactFile.tmp");
        Path newPath = immutableTableList.getLast().path();
        PriorityQueue<MergeNode> pq = new PriorityQueue<>(mergeNodeComparator);
        List<SSTableIterator> iterators = new ArrayList<>();

        for (SSTable table : immutableTableList) {
            SSTableIterator it = new SSTableIterator(table, table.indexOffset());
            iterators.add(it);

            if (it.hasNext()) {
                pq.add(new MergeNode(it.next(), table.sequenceNumber(), it));
            }
        }

        int counter = 0;
        Map<ByteBuffer, Long> indexMap = new TreeMap<>();

        long estimatedKeys = 0;
        for (SSTable table : immutableTableList) {
            estimatedKeys += (table.indexOffset() / INDEX_BUFFER_LENGTH) * INDEX_SEGMENT;
        }
        estimatedKeys = Math.max(100_000, estimatedKeys);
        BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), estimatedKeys, 0.01);

        try (FileChannel fc = FileChannel.open(compactionFile, CREATE, APPEND)) {
            while (!pq.isEmpty()) {
                MergeNode currentNode = pq.poll();
                ByteBuffer key = currentNode.pair().key().duplicate();
                ByteBuffer value = currentNode.pair().value().duplicate();

                if (isFirstKey) {
                    indexMap.put(key, fc.size());
                    isFirstKey = false;
                }

                if (!key.equals(lastWrittenKey)) {
                    byte[] keyBytesArr = new byte[key.remaining()];
                    key.duplicate().get(keyBytesArr);
                    bloomFilter.put(keyBytesArr);

                    ByteBuffer serializedBytes = serialize(key.duplicate(), value.duplicate());
                    counter++;

                    if (counter == INDEX_SEGMENT) {
                        counter = 0;
                        indexMap.put(currentNode.pair().key(), fc.size());
                    }

                    SafeReadWrite.writeFully(fc, serializedBytes);
                }

                if (currentNode.iterator().hasNext()) {
                    pq.add(new MergeNode(currentNode.iterator().next(), currentNode.sequenceNumber(), currentNode.iterator()));
                }

                lastWrittenKey = key.duplicate();
            }

            long indexOffset = fc.size();
            long indexSize = 0;

            for (Map.Entry<ByteBuffer, Long> entry : indexMap.entrySet()) {
                ByteBuffer serializedBytes = serialize(entry.getKey(), entry.getValue());
                indexSize += serializedBytes.remaining();
                SafeReadWrite.writeFully(fc, serializedBytes);
            }

            long bloomOffset = fc.size();
            OutputStream os = Channels.newOutputStream(fc);
            bloomFilter.writeTo(os);
            os.flush();

            ByteBuffer indexBuffer = ByteBuffer.allocate(INDEX_BUFFER_LENGTH);
            indexBuffer.putLong(indexOffset);
            indexBuffer.putLong(indexSize);
            indexBuffer.putLong(bloomOffset);
            indexBuffer.putInt(MAGIC_NUMBER);
            indexBuffer.flip();

            SafeReadWrite.writeFully(fc, indexBuffer);
            fc.force(true);
            Files.move(compactionFile, newPath, StandardCopyOption.REPLACE_EXISTING);
            return new SSTable(newPath, indexMap, indexOffset, immutableTableList.getLast().sequenceNumber(), bloomFilter);
        }
    }

}
