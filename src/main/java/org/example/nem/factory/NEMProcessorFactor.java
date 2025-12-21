package org.example.nem.factory;

import org.example.nem.writer.NEMErrorWriter;
import org.example.nem.writer.NEMWriter;

public interface NEMProcessorFactor {
    NEMWriter createNEMWriter(String inputFile);

    NEMErrorWriter createNEMErrorWriter(String inputFile);
}
