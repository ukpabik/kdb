package com.kdb.storage.common;

import java.nio.ByteBuffer;

public record KVPair(ByteBuffer keySize, ByteBuffer key, ByteBuffer valueSize, ByteBuffer value) {
}
