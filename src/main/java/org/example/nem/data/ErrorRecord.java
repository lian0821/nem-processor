package org.example.nem.data;

import org.example.nem.constant.ErrorType;

public record ErrorRecord(String inputFileName, long lineNumber, ErrorType errorType) {
}
