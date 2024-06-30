package com.mydb.core;

/**
 * Operation for MYDB.
 */
public enum OperationType {

    SELECT(0x01, 0x00, "SELECT"),
    INSERT(0x02, 0x01, "INSERT"),
    UPDATE(0x04, 0x01, "UPDATE"),
    DELETE(0x08, 0x01, "DELETE");

    // the code for operation
    int opCode;

    // flushDB means that whether flush data into DB Disk
    int flushDB;

    // the Method for opCode
    String method;

    OperationType(int opCode, int flushDB, String method) {
        this.opCode = opCode;
        this.flushDB = flushDB;
        this.method = method;
    }

    public int getOpCode() {
        return opCode;
    }

    public int getFlushDB() {
        return flushDB;
    }

    public String getMethod() {
        return method;
    }

    public static OperationType getInstance(String method) {
        for (OperationType opType: OperationType.values()) {
            if (method.equalsIgnoreCase(opType.method)){
                return opType;
            }
        }
        return null;
    }
}
