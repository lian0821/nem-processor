package org.example.nem.writer;

import org.example.nem.data.RuntimeState;

import java.io.Closeable;

public interface NEMCheckpointWriter extends Closeable {

    RuntimeState getStartingState();

    void flush(RuntimeState state);
}
