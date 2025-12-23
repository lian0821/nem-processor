package org.example.nem;


import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvParser;
import org.example.nem.constant.ErrorType;
import org.example.nem.data.ErrorRecord;
import org.example.nem.data.MeterReading;
import org.example.nem.factory.NEMProcessorFactory;
import org.example.nem.writer.NEMCheckpointWriter;
import org.example.nem.writer.NEMErrorWriter;
import org.example.nem.writer.NEMWriter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;


public class NEMProcessor {

    enum RowType {
        Begin("100"),
        NMI_Details("200"),
        Consumption_Data("300"),
        Quality_Indicator("400"),
        Trailing_Comments("500"),
        End("900");

        private final String code;

        RowType(String code) {
            this.code = code;
        }

        public static RowType codeOf(String code) {
            for (RowType type : RowType.values()) {
                if (type.code.equals(code)) {
                    return type;
                }
            }
            return null;
        }
    }

    static class MeterBufferWriter {
        Collection<MeterReading> readings;
        NEMWriter writer;
        NEMCheckpointWriter checkpointWriter;
        RuntimeState rs;
        private int bufferSize;


        public MeterBufferWriter(int bufferSize, RuntimeState rs, NEMWriter writer, NEMCheckpointWriter checkpointWriter) {
            this.writer = writer;
            this.bufferSize = bufferSize;
            this.readings = new ArrayList<>();
            this.checkpointWriter = checkpointWriter;
            this.rs = rs;
        }

        public void add(Collection<MeterReading> newReadings) throws IOException {
            readings.addAll(newReadings);
            if (readings.size() >= bufferSize) {
                writer.write(readings);
                readings.clear();
                checkpointWriter.flushLineNumber(rs.currentLineCnt);
            }
        }

        public void flush() throws IOException {
            if (!readings.isEmpty()) {
                writer.write(readings);
                readings.clear();
            }
            checkpointWriter.flushLineNumber(rs.currentLineCnt);
        }
    }

    static class RuntimeState {
        String nmi = "";
        int interval = 0;
        String inputFileName = "";
        long currentLineCnt = 0;
    }

    private static final int BATCH_SIZE = 200;
    private static final int ONE_DAY_MINUTES = 24 * 60;


    public NEMProcessor() {
    }

    private CsvParserSettings createParserSettings(long skipRows) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.setIgnoreLeadingWhitespaces(true);
        settings.setIgnoreTrailingWhitespaces(true);
        settings.setEmptyValue("");
        settings.setNullValue("");
        settings.setNumberOfRowsToSkip(skipRows);
        return settings;
    }

    public void process(String inputName, NEMProcessorFactory factor) throws IOException {
        RuntimeState rs = new RuntimeState();
        rs.inputFileName = inputName;
        rs.nmi = "";
        rs.interval = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(inputName, StandardCharsets.UTF_8));
             NEMWriter outputWriter = factor.createNEMWriter(inputName);
             NEMErrorWriter errorWriter = factor.createNEMErrorWriter(inputName);
             NEMCheckpointWriter checkpointWriter = factor.createNEMCheckpointWriter(inputName);
        ) {
            MeterBufferWriter valueBuffer = new MeterBufferWriter(BATCH_SIZE, rs, outputWriter, checkpointWriter);
            rs.currentLineCnt = checkpointWriter.getStartingLineNumber();
            CsvParserSettings settings = createParserSettings(rs.currentLineCnt);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(reader);
            String[] row;
            while (true) {
                try {
                    row = parser.parseNext();
                } catch (Exception ex) {
                    rs.currentLineCnt++;
                    errorRecord(rs, ErrorType.Parse_Error, errorWriter);
                    continue;
                }
                if (row == null) {
                    break;
                }
                rs.currentLineCnt++;
                List<MeterReading> readings = rowProcess(row, rs, errorWriter);
                try {
                    valueBuffer.add(readings);
                } catch (IOException ex) {
                    String errMsg = String.format("Error writing meter readings at FileName: %s, line %d~%d: %s",
                            rs.inputFileName, rs.currentLineCnt - readings.size(), rs.currentLineCnt, ex.getMessage());
                    System.err.println(errMsg);
                    throw ex;
                }
            }
            valueBuffer.flush();
            parser.stopParsing();
        } catch (IOException ex) {
            String errMsg = String.format("Error processing file %s at line %d: %s",
                    rs.inputFileName, rs.currentLineCnt, ex.getMessage());
            System.err.println(errMsg);
            throw ex;
        }
    }

    List<MeterReading> rowProcess(String[] row, RuntimeState rs, NEMErrorWriter errorWriter) {
        if (row.length == 0) {
            return List.of();
        }
        String indicator = row[0];
        RowType type = RowType.codeOf(indicator);
        if (type == null) {
            errorRecord(rs, ErrorType.Invalid_RowType, errorWriter);
            return List.of();
        }
        switch (type) {
            case Begin, Quality_Indicator, Trailing_Comments, End -> {
                // skip
            }
            case NMI_Details -> processNMIBaseInfo(row, rs, errorWriter);
            case Consumption_Data -> {
                return processConsumptionData(row, rs, errorWriter);
            }
        }
        return List.of();
    }

    private final int NMI_Details_Min_Len = 9;

    void processNMIBaseInfo(String[] row, RuntimeState rs, NEMErrorWriter errorWriter) {
        rs.nmi = "";
        rs.interval = 0;
        if (row.length < NMI_Details_Min_Len) {
            errorRecord(rs, ErrorType.Invalid_NMI_Info, errorWriter);
            return;
        }
        if (row[1] == null || row[1].isEmpty() || row[1].contains("'")) {
            errorRecord(rs, ErrorType.Invalid_NMI_Info, errorWriter);
            return;
        }
        rs.nmi = row[1];
        try {
            rs.interval = Integer.parseInt(row[8]);
        } catch (NumberFormatException ex) {
            errorRecord(rs, ErrorType.Invalid_NMI_Info, errorWriter);
            return;
        }
        if (rs.interval <= 0 || ONE_DAY_MINUTES % rs.interval != 0) {
            errorRecord(rs, ErrorType.Invalid_NMI_Info, errorWriter);
        }
    }

    private final int NMI_Data_Suffix_Len_MIN = 4;
    private final int NMI_Data_Min_Len = 3 + NMI_Data_Suffix_Len_MIN;
    private final Set<String> QualityFlags = Set.of("A", "E", "S", "F", "V", "N");

    List<MeterReading> processConsumptionData(String[] row, RuntimeState rs, NEMErrorWriter errorWriter) {
        List<MeterReading> ret = new ArrayList<>();
        if (rs.nmi == null || rs.nmi.isEmpty() || rs.interval <= 0) {
            errorRecord(rs, ErrorType.NMI_Missing, errorWriter);
            return ret;
        }
        if (row.length < NMI_Data_Min_Len) {
            errorRecord(rs, ErrorType.No_Consumption_Data, errorWriter);
            return ret;
        }
        String dateStr = row[1];
        LocalDateTime baseTime = LocalDateTime.parse(dateStr + "0000",
                DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
        int pointsCount = ONE_DAY_MINUTES / rs.interval;
        int startIdx = 2;
        int qualityFlagIndex = startIdx + pointsCount;
        if (row.length < qualityFlagIndex + 1 || !QualityFlags.contains(row[qualityFlagIndex])) {
            errorRecord(rs, ErrorType.Freq_Mismatch, errorWriter);
            return ret;
        }
        for (int i = 0; i < pointsCount; i++) {
            String consumption = row[startIdx + i];
            //cal timestamp
            LocalDateTime ts = baseTime.plusMinutes((long) (i + 1) * rs.interval);
            double consumptionValue;
            try {
                consumptionValue = Double.parseDouble(consumption);
            } catch (NumberFormatException ex) {
                errorRecord(rs, ErrorType.Invalid_Consumption_Value, errorWriter);
                return List.of();
            }
            ret.add(new MeterReading(rs.nmi, ts, consumptionValue));
        }
        return ret;
    }

    private void errorRecord(RuntimeState rs, ErrorType errorType, NEMErrorWriter errOutput) {
        errOutput.writeErrors(new ErrorRecord(rs.inputFileName, rs.currentLineCnt, errorType));
    }
}

