package org.example.nem.data;

import java.time.LocalDateTime;

public record MeterReading(String nmi, LocalDateTime timestamp, double value) {
}
