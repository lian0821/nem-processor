package org.example.nem.writer;

import java.io.Closeable;

public interface NEMCheckpointWriter extends Closeable {

    long getStartingLineNumber();

    void flushLineNumber(long lineNumber);
}
