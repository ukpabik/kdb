package com.kdb.storage.common;


/**
 * This utility class holds global constants used during file I/O operations.
 */
public final class FileSystemConstants {
    public static final byte[] TOMBSTONE = new byte[0];

    // Used for indicating a file is a .sst file.
    public static final int MAGIC_NUMBER = 0x4B444249;

    // Index for every 100 keys
    public static final int INDEX_SEGMENT = 100;
    public static final int INDEX_BUFFER_LENGTH = 28;

    // 64KB Pages
    public static final int PAGE_BUFFER_SIZE = 64 * 1024;
}
