package com.mydb.core;

import java.io.*;
import java.util.concurrent.*;

public class AsyncWriter implements Runnable{

    MYDB mydb;

    DoubleBuffer doubleBuffer;

    private BufferedWriter writer;

    private String path;

    private boolean interrupt;


    public AsyncWriter(MYDB mydb, String path, DoubleBuffer doubleBuffer) throws IOException {
        this.mydb = mydb;
        this.doubleBuffer = doubleBuffer;
        this.path = path;
        this.writer = new BufferedWriter(new FileWriter(path + "/mydb", true));
        this.interrupt = false;
    }

    @Override
    public void run() {
        while (true) {
            if (interrupt == true) {
                System.out.printf("thread[%s] be interruppted.\n", Thread.currentThread().getName());
                break;
            }

            try {
                synchronized (doubleBuffer) {
                    System.out.printf("thread[%s] is waiting.\n", Thread.currentThread().getName());
                    this.doubleBuffer.wait();
                }
                System.out.printf("thread[%s] is notified.\n", Thread.currentThread().getName());
                this.doubleBuffer.exchage();
                BlockingQueue<Operation> readyQueue = this.doubleBuffer.getReadyQueue();
                Operation element;
                while ((element = readyQueue.poll()) != null) {
                    int klength = element.K.length();
                    int vlength = element.V.length();
                    this.writer.write((klength + vlength));
                    this.writer.write(klength);
                    this.writer.write(element.K);
                    this.writer.write(vlength);
                    this.writer.write(element.V);
                    System.out.printf("write k:%s, v:%s\n", element.K, element.V);
                }
                this.writer.flush();
                this.doubleBuffer.exchage();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public void setWriter(BufferedWriter writer) {
        this.writer = writer;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isInterrupt() {
        return interrupt;
    }

    public void setInterrupt(boolean interrupt) {
        this.interrupt = interrupt;
    }
}
