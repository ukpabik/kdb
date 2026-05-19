package com.kdb.storage.common;

import java.nio.ByteBuffer;


/**
 * A data class wrapping the raw binary fragments of a key-value entry stream parsed from an SSTable.
 *
 * @param key       The raw identifier byte sequence segment mapped to this data item frame.
 * @param value     The raw binary attribute data array payload mapped to this unique key frame segment.
 *
 * @see com.kdb.storage.engine.SSTableIterator
 * @see com.kdb.storage.engine.CompactionManager
 */
public record KVPair(ByteBuffer key, ByteBuffer value) {
}
