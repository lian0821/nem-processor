package org.example.nem.writer;

import org.example.nem.data.ErrorRecord;

public class StdErrorWriter implements NEMErrorWriter {
    private static String ERROR_MSG_FORMAT = "Line %d: [%d] %s\n";

    @Override
    public void writeErrors(ErrorRecord errorRecord) {
        System.err.printf(ERROR_MSG_FORMAT,
                errorRecord.lineNumber(),
                errorRecord.errorType().getCode(),
                errorRecord.errorType().getMessage());
    }

    @Override
    public void close() {
        // No resources to close
    }
}
