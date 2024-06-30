package com.mydb.core;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class AsyncReader implements Runnable{

    MYDB mydb;

    Map<String, String> mykv;

    CountDownLatch countDownLatch;

    private BufferedReader reader;

    private String path;

    private boolean interrupt;

    public AsyncReader(MYDB mydb, String path, Map<String, String> mykv, CountDownLatch countDownLatch) {
        this.mydb = mydb;
        this.mykv = mykv;
        this.path = path;
        this.countDownLatch = countDownLatch;
        try {
            File file = new File(path + "/mydb");
            try {
                if (!file.exists()){
                    file.createNewFile();
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            this.reader = new BufferedReader(new FileReader(path + "/mydb"));
        } catch (FileNotFoundException e) {
        }
        this.interrupt = false;
    }

    @Override
    public void run() {
        try {
            int offset = 0;
            int kvLength = -1;
//            Map<String, String> mykv = new ConcurrentHashMap<String, String>();
            while ((kvLength = this.reader.read()) != -1) {
                int klen = this.reader.read();
                char[] kChs = new char[klen];
                offset += Constants.HeaderLength + Constants.KeyLength;
                this.reader.read(kChs, 0, klen);
                String key = new String(kChs);

                // 42k12v1
                // 1+1+klen+1
                int vlen = this.reader.read();

                assert kvLength == (klen + vlen);

                char[] vChs = new char[vlen];
                offset += klen + Constants.ValueLength;
                this.reader.read(vChs, 0, vlen);
                String value = new String(vChs);
                System.out.printf("restore from disk: key:%s, value:%s.\n", key, value);

                this.mykv.put(key, value);
            }
            this.countDownLatch.countDown();

            // 需要回调吗？
//            this.mydb.setMykv(this.mykv);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.printf("thread[%s] normally exit.\n", Thread.currentThread().getName());
    }

    public void setInterrupt(boolean interrupt) {
        this.interrupt = interrupt;
    }
}
