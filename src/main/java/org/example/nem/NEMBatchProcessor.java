package org.example.nem;

import org.example.nem.factory.NEMProcessorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class NEMBatchProcessor {

    public void asyncProcess(List<String> inputFiles, NEMProcessorFactory factory) throws InterruptedException {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (String inputFile : inputFiles) {
                tasks.add(() -> {
                    NEMProcessor processor = new NEMProcessor();
                    try {
                        processor.process(inputFile, factory);
                    } catch (Exception e) {
                        System.err.println("Error processing file " + inputFile + ": " + e.getMessage());
                    }
                    return null;
                });
            }
            executor.invokeAll(tasks);
        }
    }

}
