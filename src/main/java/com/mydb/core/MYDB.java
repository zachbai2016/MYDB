package com.mydb.core;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * MYDB Instance.
 */
public class MYDB {

    AsyncReader asyncReader;
    Thread asyncReaderThread;

    AsyncWriter asyncWriter;
    Thread asyncThread;

    CountDownLatch countDownLatch;

    private Map<String, String> mykv = new ConcurrentHashMap<String, String>();

    private DoubleBuffer doubleBuffer = new DoubleBuffer();

    private Operation lastUnsyncOperation = null;

    public MYDB() throws IOException, InterruptedException {

        countDownLatch = new CountDownLatch(1);

        // 创建DB实例的时候 启动WRITER 和 READER 异步线程
        asyncReader = new AsyncReader(this, ".", mykv, countDownLatch);
        asyncReaderThread = new Thread(asyncReader, "asyncReader-1");
        asyncReaderThread.start();

        //
        countDownLatch.await();
        asyncWriter = new AsyncWriter(this, ".", doubleBuffer);
        asyncThread = new Thread(asyncWriter, "asyncWriter-1");
        asyncThread.start();
    }

    public boolean operate(Operation operation) {
        if (operation == null) {
            System.out.println("null operation");
            return false;
        }

        int opCode = operation.operationType.opCode;
        switch (opCode) {
            case 0x01:
                select(operation);
                break;
            case 0x02:
                insert(operation);
                break;
            case 0x04:
                update(operation);
                break;
            case 0x08:
                delete(operation);
                break;
            default:
        }
        return true;
    }



    private void select(Operation operation) {
        String k = operation.K;
        if (k != null && k != "") {
            // 1658176806
            for (String kk: mykv.keySet()) {
                if (kk.equals(k)) {
                    System.out.printf("k=%s, v=%s\n", k, mykv.get(kk));
                    return;
                }
            }
            String v = this.mykv.get(k);
            System.out.printf("k=%s, v=%s\n", k, v);
            return;
        }

        for(String key : this.mykv.keySet()) {
            System.out.printf("key: %s, value: %s\n", key, this.mykv.get(key));
        }
    }

    private boolean insert(Operation op) {
        String oldV = mykv.put(op.K, op.V);
        this.doubleBuffer.getUsingQueue().add(op);
        if (oldV != null) {
            System.out.printf("replace old value: %s\n", oldV);
        }

        // 记录上一次操作 有什么用？
        // 超过5秒 或者5个操作 进行刷盘 为了方便刷盘

        // 超过5秒 或者5个操作 进行刷盘
        if (this.lastUnsyncOperation != null)
            System.out.print(((op.timeStamp - this.lastUnsyncOperation.timeStamp)/1000 > 5 || (op.sync - lastUnsyncOperation.sync) > 5));

        // 时间大于5s优先 如果时间没有大于5s 才判断操作是否大于5s
//        this.lastUnsyncOperation = this.lastUnsyncOperation == null ? op
//            : ((op.timeStamp - lastUnsyncOperation.timeStamp)/1000 > 5 ? op
//            : (op.sync - lastUnsyncOperation.sync) > 5 ? op : this.lastUnsyncOperation);



        if (this.lastUnsyncOperation == null){
            this.lastUnsyncOperation = op;
        } else if ((op.timeStamp - this.lastUnsyncOperation.timeStamp)/1000 > 5 || (op.sync - lastUnsyncOperation.sync) > 5) {
            synchronized (doubleBuffer) {
                System.out.printf("超过5");
                this.doubleBuffer.notify();
            }
        }
        return true;
    }

    private void update(Operation operation) {
        insert(operation);
    }

    private void delete(Operation operation) {
        String k = operation.K;
        String removedV = this.mykv.remove(k);
        System.out.printf("removed key:%s, value:%s", k, removedV);
    }

    public void shutdown() {
        this.asyncWriter.setInterrupt(true);
        this.asyncReader.setInterrupt(true);
        synchronized (doubleBuffer) {
            System.out.printf("MYDB exit.\n");
            this.doubleBuffer.notify(); // a hook thread will be notified.
        }
    }

    public Map<String, String> getMykv() {
        return mykv;
    }

    public void setMykv(Map<String, String> mykv) {
        this.mykv = mykv;
    }

}
