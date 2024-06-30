package com.mydb.core;

import java.util.concurrent.*;

public class Operation {

    OperationType operationType;

    String K;

    String V;

    int sync;   // 序号

    long timeStamp; // 时间戳

    public Operation(OperationType operationType, String k, String v) {
        this.operationType = operationType;
        this.K = k;
        this.V = v;
        this.sync = GlobalIncreator.addAndGet();
        this.timeStamp = System.currentTimeMillis();
    }
}
