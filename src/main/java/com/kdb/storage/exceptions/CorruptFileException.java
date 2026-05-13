package com.kdb.storage.exceptions;

public class CorruptFileException extends RuntimeException {
    public CorruptFileException(String message) {
        super(message);
    }

    public CorruptFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
