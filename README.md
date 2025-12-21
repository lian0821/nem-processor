# nem-processor
# Description
A high-performance NEM12 (Meter Data File Format) parser designed for large-scale data ingestion.

* **Input**: Local NEM12 CSV files.
* **Output**:
  - Batch SQL Scripts: Optimized .sql files for mass import.
  - RDBMS Storage: Direct insertion via JDBC with connection pooling.
  - Observability: Structured error reporting to stderr (extensible to persistent logs/db).

# Usage
1. (Optional) generate test files (file generated under src/main/resources/):
   ```bash
   cd src/main/resources/scripts
   python gen_sample_nem.py --nmi_count=100 --days=365
   ```
2. execute mvn (.sql output under src/main/resources/):
   ```bash
    mvn exec:java -Dexec.mainClass=org.example.Main -Dexec.args=<AbsolutemPath to Test Files> 
   ```
3. run unit tests:
   ```bash
   mvn test
   ```
# Design Philosophy (Q3)
To meet the "Production Grade" requirement for high-throughput, large-scale data ingestion, the system is built upon following considerations:
1. **Massive Parallelism via Virtual Threads**: Leveraging Virtual Threads, the application processes multiple input files concurrently with minimal overhead. This ensures the system fully utilizes I/O bandwidth without the scaling bottlenecks of platform threads.
2. **Stream Processing for Memory Efficiency**: The application implements a strictly stream-oriented CSV reading strategy to maintain a constant memory footprint. On the output side, a MeterBufferWriter manages write-through batching, allowing adjustable multi-row SQL inserts to reduce database round-trips.
3. **Pluggable & Extensible Architecture**: Adhering to the Dependency Inversion Principle, both data sinks (NEMWriter) and error reporters (NEMErrorWriter) are defined as interfaces. This allows the system to be easily extended or swapped for more advanced implementations (e.g., Kafka, or NoSQL stores) without modifying the current pipeline.
4. **State-Aware Fault Isolation**: To ensure data integrity, it enforces strict contextual bounds: if a "200" (NMI Header) record fails validation, all dependent "300" (Interval) records are automatically invalidated until the next valid "200" record is encountered. 
5. **Structured Error Observability**: A dedicated NEMErrorWriter captures structured metadata for every failure—including filename, line number, and specific error type. This provides a clear Audit Trail, facilitating rapid debugging and data reconciliation in production environments.

# Technologies Used (Q1)
- **Java**: Chosen for its concurrency support, stream processing capabilities, and production level integration in the future
- **Univocity Parsers**: Selected as the engine because it is arguably the fastest CSV parser for Java. It utilizes a zero-copy approach and efficient buffer management, outperforming Apache Commons CSV and OpenCSV, especially for large files.
- **Raw JDBC + HikariCP**: Avoided heavy ORMs (Hibernate) to eliminate overhead. Direct JDBC allows fine-grained control over batch sizes and transaction boundaries, which is critical for massive data ingestion. HikariCP is used for efficient connection pooling. (JdbcTemplate can be considered if run with Spring in the future)
- **H2 (In-Memory/File)**:Used for the demo and unit tests to provide a "zero-dependency" experience while maintaining SQL compatibility

# Future Improvemen (Q2)
- Parallel input & output processing: Implement a Producer-Consumer pattern using a Disruptor or BlockingQueue to parallelize file reading, parsing, and DB writing.
- Abstract InputReader: Support different load sources, including cloud storage (e.g., AWS S3/Azure Blob Storage) or ftp server
- Advanced Observability: Write error report to a persistent storage instead of stderr, merging multiple error reports
- Quality Handling: Add logic for "Actual vs. Estimated" (A vs E) data overrides, ensuring the most accurate data is stored
- Pluggable Validation Framework: Use Strategy based validation pipeline to allow new validation rules to be added without modifying existing code
