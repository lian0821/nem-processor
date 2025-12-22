package org.example.nem.factory;

import org.example.nem.writer.*;

import javax.sql.DataSource;

public class JdbcOutputFactory implements NEMProcessorFactory {
    private DataSource dataSource;

    public JdbcOutputFactory(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public NEMWriter createNEMWriter(String inputFile) {
        return new NEMJdbcWriter(dataSource);
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
