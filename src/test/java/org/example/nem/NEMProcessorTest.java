package org.example.nem;

import org.example.nem.constant.ErrorType;
import org.example.nem.data.ErrorRecord;
import org.example.nem.data.MeterReading;
import org.example.nem.factory.NEMProcessorFactor;
import org.example.nem.writer.DBSupport;
import org.example.nem.writer.NEMErrorWriter;
import org.example.nem.writer.NEMJdbcWriter;
import org.example.nem.writer.NEMWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.fail;

class NEMProcessorTest {

    @Test
    void TestProcessWithJdbcWriter() {
        DataSource ds = DBSupport.createDataSource();
        try {
            DBSupport.createTable(ds);
        } catch (SQLException e) {
            fail("Failed to create table: " + e.getMessage());
        }
        NEMProcessor processor = new NEMProcessor();
        NEMProcessorFactor factor = new NEMProcessorFactor() {
            @Override
            public NEMWriter createNEMWriter(String inputFile) {
                return new NEMJdbcWriter(ds);
            }

            @Override
            public NEMErrorWriter createNEMErrorWriter(String inputFile) {
                return new NEMErrorWriterMock();
            }
        };
        try {
            processor.process("src/test/resources/nem_sample.csv", factor);
        } catch (IOException e) {
            fail("IOException thrown during processing: " + e.getMessage());
        }
        List<MeterReading> readings;
        try {
            readings = DBSupport.fetchAllReadings(ds);
            Assertions.assertEquals(48, readings.size());
        } catch (SQLException e) {
            fail("SQLException thrown during fetch: " + e.getMessage());
        }
        DBSupport.shutdownDataSource(ds);
    }

    @Test
    void rowProcess() {
        String[] testData = new String[]{
                "",
                "100,extra_field",
                "500,extra_field",
                "900,extra_field",
                "400,extra_field",
                "999,unknown_row_type",
                "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610",
                "300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204"
        };
        NEMProcessor processor = new NEMProcessor();
        NEMProcessorFactor factor = new MockNEMProcessorFactory();
        NEMErrorWriter errorWriter = factor.createNEMErrorWriter("");
        NEMWriterMock nemWriter = new NEMWriterMock();
        for (String row : testData) {
            String[] columns = row.split(",");
            nemWriter.write(processor.rowProcess(columns, 0, errorWriter));
        }
        Assertions.assertEquals(2, ((NEMErrorWriterMock) errorWriter).errorRecords.size());
        Assertions.assertEquals(48, nemWriter.writtenReadings.size());
    }

    @Test
    void processNMIBaseInfo() {
        NEMProcessor processor = new NEMProcessor();
        Set<String> errorTestData = Set.of(
                "200,NEM1201009,E1E2,1,01009,kWh,30,20050610",
                "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,-30,20050610",
                "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,a,20250610",
                "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,11,20050610"
        );
        Set<String> validTestData = Set.of(
                "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610",
                "200,NEM1201009,E1E2,2,E1,N1,01009,kWh,15,20050610,20050611,10,AB"
        );
        NEMProcessorFactor factor = new MockNEMProcessorFactory();
        for (String row : errorTestData) {
            String[] columns = row.split(",");
            NEMErrorWriter errorWriter = factor.createNEMErrorWriter("");
            processor.processNMIBaseInfo(columns, 0, errorWriter);
            Assertions.assertEquals(1, ((NEMErrorWriterMock) errorWriter).errorRecords.size());
        }
        for (String row : validTestData) {
            String[] columns = row.split(",");
            NEMErrorWriter errorWriter = factor.createNEMErrorWriter("");
            processor.processNMIBaseInfo(columns, 0, errorWriter);
            Assertions.assertEquals(0, ((NEMErrorWriterMock) errorWriter).errorRecords.size());
        }
    }

    @Test
    void processConsumptionDataWithIllegalNEMInfo() {
        NEMProcessor processor = new NEMProcessor();
        NEMProcessorFactor factor = new MockNEMProcessorFactory();
        String row = "300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204";
        String[] columns = row.split(",");
        NEMErrorWriter errorWriter = factor.createNEMErrorWriter("");
        List<MeterReading> ret = processor.processConsumptionData(columns, 0, errorWriter);
        Assertions.assertEquals(0, ret.size());
        Assertions.assertEquals(1, ((NEMErrorWriterMock) errorWriter).errorRecords.size());
        Assertions.assertEquals(ErrorType.NMI_Missing, ((NEMErrorWriterMock) errorWriter).errorRecords.get(0).errorType());
    }

    @Test
    void processConsumptionData() {
        NEMProcessor processor = new NEMProcessor();
        NEMProcessorFactor factor = new MockNEMProcessorFactory();
        String validBaseInfoRow = "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610";
        String[] baseInfoColumns = validBaseInfoRow.split(",");
        processor.processNMIBaseInfo(baseInfoColumns, 0, factor.createNEMErrorWriter(""));
        Map<String, ErrorType> errorTestData = Map.of(
                // < min_len
                "300,20050301", ErrorType.No_Consumption_Data,
                // freq mismatch - freq with 15min but 30min data
                "300,20250101,1.197,1.014,0.401,1.544,0.877,0.854,1.189,0.776,0.480,1.416,1.257,1.482,0.933,0.152,0.487,1.714,0.812,0.573,1.935,1.631,0.807,1.282,0.147,1.742,0.769,1.861,1.897,0.262,0.885,1.431,1.152,1.209,0.859,1.950,1.033,0.870,0.530,1.789,1.835,1.357,1.070,1.856,1.087,1.631,0.794,1.435,1.720,0.732,1.073,1.189,1.063,0.858,1.244,0.946,1.437,0.248,1.138,0.644,1.953,0.688,1.810,1.345,0.941,0.118,1.573,0.363,1.068,0.322,0.802,1.999,1.780,0.653,1.328,0.339,1.253,1.772,0.792,1.154,0.997,1.096,1.077,1.080,0.105,0.161,0.792,1.170,1.856,1.106,1.605,0.877,0.173,0.687,0.309,0.748,0.613,0.373,A,,,20250101000000", ErrorType.Freq_Mismatch,
                // invalid value
                "300,20050301,a,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204", ErrorType.Invalid_Consumption_Value
        );
        Set<String> validTestData = Set.of(
                "300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204"
        );
        for (Map.Entry<String, ErrorType> entry : errorTestData.entrySet()) {
            String[] columns = entry.getKey().split(",");
            NEMErrorWriter errorWriter = factor.createNEMErrorWriter("");
            List<MeterReading> ret = processor.processConsumptionData(columns, 0, errorWriter);
            Assertions.assertEquals(0, ret.size());
            Assertions.assertEquals(1, ((NEMErrorWriterMock) errorWriter).errorRecords.size());
            Assertions.assertEquals(entry.getValue(), ((NEMErrorWriterMock) errorWriter).errorRecords.get(0).errorType());
        }
        for (String row : validTestData) {
            String[] columns = row.split(",");
            NEMErrorWriter errorWriter = factor.createNEMErrorWriter("");
            List<MeterReading> ret = processor.processConsumptionData(columns, 0, errorWriter);
            Assertions.assertEquals(48, ret.size());
            Assertions.assertEquals(0, ((NEMErrorWriterMock) errorWriter).errorRecords.size());
        }
    }

    class MockNEMProcessorFactory implements NEMProcessorFactor {

        @Override
        public NEMWriter createNEMWriter(String inputFile) {
            return new NEMWriterMock();
        }

        @Override
        public NEMErrorWriter createNEMErrorWriter(String inputFile) {
            return new NEMErrorWriterMock();
        }
    }

    class NEMWriterMock implements NEMWriter {

        private List<MeterReading> writtenReadings = new ArrayList<>();

        @Override
        public void write(Collection<MeterReading> readings) {
            this.writtenReadings.addAll(readings);
        }

        @Override
        public void close() {

        }
    }

    class NEMErrorWriterMock implements NEMErrorWriter {

        private List<ErrorRecord> errorRecords = new ArrayList<>();

        @Override
        public void writeErrors(ErrorRecord errorRecord) {
            errorRecords.add(errorRecord);
        }

        @Override
        public void close() {

        }
    }
}