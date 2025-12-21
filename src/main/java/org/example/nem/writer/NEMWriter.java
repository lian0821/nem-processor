package org.example.nem.writer;

import org.example.nem.data.MeterReading;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

public interface NEMWriter extends Closeable {
    void write(Collection<MeterReading> readings) throws IOException;
}
