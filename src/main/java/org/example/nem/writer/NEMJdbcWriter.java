package org.example.nem.writer;

import org.example.nem.data.MeterReading;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;

public class NEMJdbcWriter implements NEMWriter {

    private DataSource dataSource;

    public NEMJdbcWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void close() {
        // keep empty
    }

    @Override
    public void write(Collection<MeterReading> readings) throws IOException {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            String sql = "INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES (?, ?, ?)";
            try {
                try (var preparedStatement = connection.prepareStatement(sql)) {
                    for (MeterReading reading : readings) {
                        preparedStatement.setString(1, reading.nmi());
                        preparedStatement.setObject(2, reading.timestamp());
                        preparedStatement.setDouble(3, reading.value());
                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to write meter readings to database", e);
        }
    }
}
