package com.mydb.core;

import java.io.*;

/**
 * The main class for start to provide service.
 * usage: INSERT KEY1 VALUE1
 */
public class MYDBMain {

    static MYDB mydb;

    public static void main(String[] args) throws IOException, InterruptedException {

        mydb = new MYDB();


        InputStreamReader inputStreamReader = new InputStreamReader(System.in);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        while (true) {
            String line = reader.readLine();
            if (line.contains("exit")) {
                mydb.shutdown();
                break;
            }
            Operation operation = transOperation(line.split(" "));
            mydb.operate(operation);
        }
    }

    private static Operation transOperation(String[] args) {
        if (args.length < 1) {
            return null;
        }

        printOperation(args);

        OperationType operationType = OperationType.getInstance(args[0]);
        if (null == operationType){
            return null;
        }

        String K = "";
        if (args.length >= 2) {
            K = String.valueOf(args[1]);
            K.hashCode();
        }

        String V = "";
        if (args.length >= 3) {
            V = String.valueOf(args[2]);
            V.hashCode();
        }

        Operation operation = new Operation(operationType, K, V);
        return operation;
    }

    private static void printOperation(String[] args) {
        for (String arg: args) {
            System.out.printf(arg + " ");
        }
        System.out.printf("\n");
    }

}
