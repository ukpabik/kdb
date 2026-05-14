package com.kdb.storage.exceptions;

/**
 * An exception class used to signify errors when persisting data or doing any file operations.
 *
 */
public class StorageException extends RuntimeException {
    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
