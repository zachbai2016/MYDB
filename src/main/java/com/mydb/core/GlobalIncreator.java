package com.mydb.core;

import java.util.concurrent.atomic.*;

public class GlobalIncreator {

    private final static AtomicInteger increator = new AtomicInteger(0);

    public static int addAndGet(){
        return increator.addAndGet(1);
    }

}
