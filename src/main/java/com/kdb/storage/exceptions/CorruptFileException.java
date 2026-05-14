package com.kdb.storage.exceptions;

/**
 * An exception class used to signify corrupt file errors, in case of a crash.
 *
 * <p>An example of this would be: during a flush to disk, the system crashes, and you are left with a half-written .sst
 * file. This would result in a file without the magic number at the end, meaning that the file is corrupt.</p>
 */
public class CorruptFileException extends RuntimeException {
    public CorruptFileException(String message) {
        super(message);
    }

    public CorruptFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
