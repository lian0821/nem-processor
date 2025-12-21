package org.example;

import org.example.nem.NEMBatchProcessor;
import org.example.nem.factory.SqlOutputFactory;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        NEMBatchProcessor processor = new NEMBatchProcessor();
        try {
            processor.asyncProcess(List.of(args), new SqlOutputFactory());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}