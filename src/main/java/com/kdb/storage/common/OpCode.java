package com.kdb.storage.common;

public enum OpCode {
    PUT((byte) 0), DELETE((byte) 1);

    private final byte code;
    OpCode(byte code) { this.code = code; }
    public byte getCode() { return code; }
}
