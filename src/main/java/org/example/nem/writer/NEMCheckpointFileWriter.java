package org.example.nem.writer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class NEMCheckpointFileWriter implements NEMCheckpointWriter {

    private String checkpointFilePath;
    private Writer writer;

    public NEMCheckpointFileWriter(String checkpointFilePath) {
        this.checkpointFilePath = checkpointFilePath;
        try {
            this.writer = Files.newBufferedWriter(
                    Path.of(checkpointFilePath),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getStartingLineNumber() {
        File checkpointFile = new File(this.checkpointFilePath);
        if (checkpointFile.exists()) {
            try (RandomAccessFile reader = new RandomAccessFile(checkpointFile, "r")) {
                long length = reader.length();
                if (length == 0) {
                    return 0;
                }
                long pointer = length - 1;
                StringBuilder sb = new StringBuilder();
                while (pointer >= 0) {
                    reader.seek(pointer);
                    int c = reader.read();
                    if ((c == '\n' || c == '\r') && !sb.isEmpty()) {
                        break;
                    }
                    if (!Character.isWhitespace(c)) {
                        sb.append((char) c);
                    }
                    pointer--;
                }
                return Long.parseLong(sb.reverse().toString().trim());
            } catch (IOException | NumberFormatException e) {
                System.err.println("Error reading checkpoint file: " + e.getMessage());
                throw new RuntimeException(e);
            }
        } else {
            return 0;
        }
    }

    @Override
    public void flushLineNumber(long lineNumber) {
        try {
            this.writer.write(String.valueOf(lineNumber));
            this.writer.write(System.lineSeparator());
            this.writer.flush();
        } catch (IOException e) {
            System.err.println("Error writing to checkpoint file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (this.writer != null) {
            try {
                this.writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
