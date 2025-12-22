package org.example.nem.writer;

import org.example.nem.data.MeterReading;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NEMJdbcWriterTest {

    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        dataSource = DBSupport.createDataSource();
        try {
            DBSupport.createTable(dataSource);
        } catch (SQLException e) {
            fail("Failed to create table: " + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        DBSupport.shutdownDataSource(dataSource);
    }

    @Test
    void write() {
        NEMJdbcWriter writer = new NEMJdbcWriter(dataSource);
        MeterReading mr = new MeterReading("1234567890", LocalDateTime.now(), 100.0);
        try {
            writer.write(List.of(mr));
        } catch (IOException e) {
            fail("IOException thrown during write: " + e.getMessage());
        }
        List<MeterReading> readings;
        try {
            readings = DBSupport.fetchAllReadings(dataSource);
            assertEquals(1, readings.size());
        } catch (SQLException e) {
            fail("SQLException thrown during fetch: " + e.getMessage());
            return;
        }
    }
}