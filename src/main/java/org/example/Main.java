package org.example;

import org.example.nem.NEMBatchProcessor;
import org.example.nem.factory.SqlOutputFactory;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        long startTime  = System.currentTimeMillis();
        NEMBatchProcessor processor = new NEMBatchProcessor();
        try {
            processor.asyncProcess(List.of(args), new SqlOutputFactory());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println(String.format("Processing complete. duration=%d ms", (System.currentTimeMillis() - startTime)));
    }
}