package org.example.nem;

import org.example.nem.factory.NEMProcessorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class NEMBatchProcessor {

    private static final int MAX_CONCURRENT_FILES = 100;
    private Semaphore fileSemaphore = new Semaphore(MAX_CONCURRENT_FILES);

    public void asyncProcess(List<String> inputFiles, NEMProcessorFactory factory) throws InterruptedException {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (String inputFile : inputFiles) {
                tasks.add(() -> {
                    try {
                        fileSemaphore.acquire();
                        NEMProcessor processor = new NEMProcessor();
                        processor.process(inputFile, factory);
                    } catch (Exception e) {
                        System.err.println("Error processing file " + inputFile + ": " + e.getMessage());
                    } finally {
                        fileSemaphore.release();
                    }
                    return null;
                });
            }
            executor.invokeAll(tasks);
        }
    }

}
