package org.example.nem.writer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.nem.data.MeterReading;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;

public class DBSupport {

    public static DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.h2.Driver");
        config.setJdbcUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");  // in-memory database
        config.setUsername("sa");
        config.setPassword("");
        return new HikariDataSource(config);
    }

    public static void createTable(DataSource dataSource) throws SQLException {
        String createTableSQL = "CREATE TABLE meter_readings (" +
                "id UUID DEFAULT uuid() NOT NULL, " +
                "nmi VARCHAR(10) NOT NULL, " +
                "timestamp TIMESTAMP NOT NULL, " +
                "consumption DECIMAL NOT NULL, " +
                "PRIMARY KEY (id), " +
                "UNIQUE (nmi, timestamp))";

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
        }
    }

    public static List<MeterReading> fetchAllReadings(DataSource dataSource) throws SQLException {
        String querySQL = "SELECT nmi, timestamp, consumption FROM meter_readings";
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement();
             var rs = stmt.executeQuery(querySQL)) {
            List<MeterReading> readings = new java.util.ArrayList<>();
            while (rs.next()) {
                String nmi = rs.getString("nmi");
                LocalDateTime timestamp = rs.getObject("timestamp", LocalDateTime.class);
                double consumption = rs.getDouble("consumption");
                readings.add(new MeterReading(nmi, timestamp, consumption));
            }
            return readings;
        }
    }

    public static void shutdownDataSource(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.execute("SHUTDOWN");
        } catch (SQLException e) {
            // Ignore
        }
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            hikariDataSource.close();
        }
    }
}
