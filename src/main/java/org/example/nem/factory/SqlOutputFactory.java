package org.example.nem.factory;

import org.example.nem.writer.*;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class SqlOutputFactory implements NEMProcessorFactory {

    @Override
    public NEMWriter createNEMWriter(String inputFile) {
        try {
            String outFileName = inputFile.replace(".csv", ".out.sql");
            return new NEMSqlWriter(new BufferedWriter(new FileWriter(outFileName)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create NEMSqlWriter for file: " + inputFile, e);
        }
    }

    @Override
    public NEMErrorWriter createNEMErrorWriter(String inputFile) {
        return new StdErrorWriter();
    }

    @Override
    public NEMCheckpointWriter createNEMCheckpointWriter(String inputFile) {
        return new NEMCheckpointFileWriter(inputFile + ".ckpt");
    }
}
