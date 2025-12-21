package org.example.nem.writer;

import org.example.nem.data.ErrorRecord;

import java.io.Closeable;

public interface NEMErrorWriter extends Closeable {
    void writeErrors(ErrorRecord errorRecord);
}
