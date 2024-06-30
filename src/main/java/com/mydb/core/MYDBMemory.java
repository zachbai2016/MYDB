package com.mydb.core;

/**
 * If MYDB Instance has been created, we need to restart it from memory,
 * else we need to create a new MYDB Instance.
 */
public class MYDBMemory {
    private MYDB instance;
    private boolean init;

    public MYDBMemory(MYDB instance, boolean init) {
        this.instance = instance;
        this.init = init;
    }

}
