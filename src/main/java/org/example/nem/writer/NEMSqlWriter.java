package org.example.nem.writer;

import org.example.nem.data.MeterReading;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

public class NEMSqlWriter implements NEMWriter {

    private final static String INSERT_SQL_BEGIN =
            "INSERT INTO meter_readings (\"nmi\", \"timestamp\", \"consumption\") VALUES";
    private final static String INSERT_TEMPLATE = "('%s', '%s', %.3f)";
    private static final DateTimeFormatter SQL_TS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private BufferedWriter writer;

    public NEMSqlWriter(Writer outputWriter) {
        this.writer = new BufferedWriter(outputWriter);
    }

    @Override
    public void write(Collection<MeterReading> readings) throws IOException {
        this.writer.write(INSERT_SQL_BEGIN);
        int i = 0;
        for (MeterReading reading : readings) {
            if (i++ > 0) {
                this.writer.write(",");
            }
            this.writer.write(String.format(INSERT_TEMPLATE,
                    reading.nmi(),
                    reading.timestamp().format(SQL_TS_FORMATTER),
                    reading.value()));
        }
        this.writer.write(";");
        this.writer.newLine();
    }

    @Override
    public void close() throws IOException {
        this.writer.flush();
        this.writer.close();
    }
}
