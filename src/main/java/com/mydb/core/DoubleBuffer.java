package com.mydb.core;

import java.util.concurrent.*;

public class DoubleBuffer {

    private BlockingQueue<Operation> readyQueue;

    private BlockingQueue<Operation> usingQueue = new ArrayBlockingQueue<>(256);

    public void exchage() {
        this.readyQueue = this.usingQueue;
        this.usingQueue = new ArrayBlockingQueue<>(256);
    }

    public void clearReady() {
        readyQueue.clear();
    }

    public BlockingQueue<Operation> getReadyQueue() {
        return readyQueue;
    }

    public BlockingQueue<Operation> getUsingQueue() {
        return usingQueue;
    }
}
