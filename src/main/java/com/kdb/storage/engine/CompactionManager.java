package com.kdb.storage.engine;

import com.google.common.collect.ImmutableMap;
import com.kdb.storage.common.KVPair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

final class CompactionManager {
    private final Path directory;

    private final Comparator mergeNodeComparator = Comparator
            .comparing((MergeNode node) -> node.pair().key())
            .thenComparing(MergeNode::sequenceNumber, Collections.reverseOrder());

    private record MergeNode(KVPair pair, long sequenceNumber, SSTableIterator iterator) {
    }

    CompactionManager(Path directory) {
        this.directory = directory;
    }

    void compact(List<SSTable> immutableTableList) throws IOException {
        ByteBuffer lastWrittenKey;
        boolean isFirstKey = true;
        Path compactionFile = directory.resolve("tmpCompactFile.tmp");
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
        try (FileChannel fc = FileChannel.open(compactionFile, CREATE, APPEND)) {
            MergeNode currentNode = pq.poll();

            if (isFirstKey) {
                isFirstKey = false;
            }

            // TODO:

            /**
             * TODO: Finish this logic here.
             * Step by step:
             *  Check if first key, if so, add to index.
             *  Check if equal to lastWrittenKey
             *      if so, we don't add it and do the hasNext check (add next key in iterator to pq).
             *  Update lastWrittenKey to current key.
             *  Increase counter and see if we add to index here.
             *  Write this to file and do hasNext check (add next key in iterator to pq, if exists).
             */
        } finally {
            closeIterators(iterators);
        }
    }

    private void closeIterators(List<SSTableIterator> iterators) {
        for (SSTableIterator it : iterators) {
            try {
                if (it != null) {
                    it.close();
                }
            } catch (Exception e) {
                System.err.println("Failed to close iterator: " + e.getMessage());
            }
        }
    }
}
