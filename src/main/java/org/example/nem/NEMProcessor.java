package org.example.nem;


import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvParser;
import org.example.nem.constant.ErrorType;
import org.example.nem.data.ErrorRecord;
import org.example.nem.data.MeterReading;
import org.example.nem.factory.NEMProcessorFactor;
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

    class MeterBufferWriter {
        Collection<MeterReading> readings;
        NEMWriter writer;
        private int bufferSize;

        public MeterBufferWriter(int bufferSize, NEMWriter writer) {
            this.writer = writer;
            this.bufferSize = bufferSize;
            this.readings = new ArrayList<>();
        }

        public void add(Collection<MeterReading> newReadings) throws IOException {
            readings.addAll(newReadings);
            if (readings.size() >= bufferSize) {
                writer.write(readings);
                readings.clear();
            }
        }

        public void flush() throws IOException {
            if (!readings.isEmpty()) {
                writer.write(readings);
            }
        }
    }

    private static final int BATCH_SIZE = 200;
    private static final int OneDayMinutes = 24 * 60;

    private MeterBufferWriter valueBuffer;
    private String nmi = "";
    private int interval = 0;
    private String inputFileName = "";
    private int currentLineCnt = 0;

    public NEMProcessor() {
    }

    public void process(String inputName, NEMProcessorFactor factor) throws IOException {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.setIgnoreLeadingWhitespaces(true);
        settings.setIgnoreTrailingWhitespaces(true);
        CsvParser parser = new CsvParser(settings);
        currentLineCnt = 0;
        inputFileName = inputName;
        try (BufferedReader reader = new BufferedReader(new FileReader(inputName, StandardCharsets.UTF_8));
             NEMWriter outputWriter = factor.createNEMWriter(inputName);
             NEMErrorWriter errorWriter = factor.createNEMErrorWriter(inputName)
        ) {
            valueBuffer = new MeterBufferWriter(BATCH_SIZE, outputWriter);
            parser.beginParsing(reader);
            String[] row;
            while (true) {
                try {
                    row = parser.parseNext();
                } catch (Exception ex) {
                    currentLineCnt++;
                    errorRecord(currentLineCnt, ErrorType.Parse_Error, errorWriter);
                    continue;
                }
                if (row == null) {
                    break;
                }
                currentLineCnt++;
                try {
                    valueBuffer.add(rowProcess(row, currentLineCnt, errorWriter));
                } catch (IOException ex) {
                    errorRecord(currentLineCnt, ErrorType.Unknown_Error, errorWriter);
                }
            }
            valueBuffer.flush();
        } finally {
            try {
                parser.stopParsing();
            } catch (Exception ex) {
                //ignore
            }
        }
    }

    List<MeterReading> rowProcess(String[] row, int currentLineCnt, NEMErrorWriter errorWriter) {
        if (row.length == 0) {
            return List.of();
        }
        String indicator = row[0];
        RowType type = RowType.codeOf(indicator);
        if (type == null) {
            errorRecord(currentLineCnt, ErrorType.Invalid_RowType, errorWriter);
            return List.of();
        }
        switch (type) {
            case Begin, Quality_Indicator, Trailing_Comments, End -> {
                // skip
            }
            case NMI_Details -> processNMIBaseInfo(row, currentLineCnt, errorWriter);
            case Consumption_Data -> {
                return processConsumptionData(row, currentLineCnt, errorWriter);
            }
        }
        return List.of();
    }

    void processNMIBaseInfo(String[] row, int currentLineCnt, NEMErrorWriter errorWriter) {
        nmi = "";
        interval = 0;
        if (row.length < 9) {
            errorRecord(currentLineCnt, ErrorType.Invalid_NMI_Info, errorWriter);
            return;
        }
        nmi = row[1];
        try {
            interval = Integer.parseInt(row[8]);
        } catch (NumberFormatException ex) {
            errorRecord(currentLineCnt, ErrorType.Invalid_NMI_Info, errorWriter);
            return;
        }
        if (interval <= 0 || OneDayMinutes % interval != 0) {
            errorRecord(currentLineCnt, ErrorType.Invalid_NMI_Info, errorWriter);
        }
    }

    private final int MNI_Data_Suffix_Len_MIN = 4;
    private final int MNI_Data_Min_Len = 3 + MNI_Data_Suffix_Len_MIN;
    private final Set<String> QualityFlags = Set.of("A", "E", "S", "F", "V", "N");

    List<MeterReading> processConsumptionData(String[] row, int currentLineCnt, NEMErrorWriter errorWriter) {
        List<MeterReading> ret = new ArrayList<>();
        if ("".equals(nmi) || interval <= 0) {
            errorRecord(currentLineCnt, ErrorType.NMI_Missing, errorWriter);
            return ret;
        }
        if (row.length < MNI_Data_Min_Len) {
            errorRecord(currentLineCnt, ErrorType.No_Consumption_Data, errorWriter);
            return ret;
        }
        String dateStr = row[1];
        LocalDateTime baseTime = LocalDateTime.parse(dateStr + "0000",
                DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
        int pointsCount = OneDayMinutes / interval;
        int startIdx = 2;
        int qualityFlagIndex = startIdx + pointsCount;
        if (row.length < qualityFlagIndex + 1 || !QualityFlags.contains(row[qualityFlagIndex])) {
            errorRecord(currentLineCnt, ErrorType.Freq_Mismatch, errorWriter);
            return ret;
        }
        for (int i = 0; i < pointsCount; i++) {
            String consumption = row[startIdx + i];
            //cal timestamp
            LocalDateTime ts = baseTime.plusMinutes((long) (i + 1) * interval);
            double consumptionValue;
            try {
                consumptionValue = Double.parseDouble(consumption);
            } catch (NumberFormatException ex) {
                errorRecord(currentLineCnt, ErrorType.Invalid_Consumption_Value, errorWriter);
                return List.of();
            }
            ret.add(new MeterReading(nmi, ts, consumptionValue));
        }
        return ret;
    }

    private void errorRecord(int lineCount, ErrorType errorType, NEMErrorWriter errOutput) {
        errOutput.writeErrors(new ErrorRecord(inputFileName, lineCount, errorType));
    }
}

