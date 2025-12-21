package org.example.nem.factory;

import org.example.nem.data.JdbcConfig;
import org.example.nem.writer.NEMErrorWriter;
import org.example.nem.writer.NEMJdbcWriter;
import org.example.nem.writer.NEMWriter;
import org.example.nem.writer.StdErrorWriter;

import javax.sql.DataSource;

public class JdbcOutputFactory implements NEMProcessorFactor {
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
}
