# DSAPIAdvPart1TaxiPy

W projekcie wykorzystywany jest plik jar zawierający definicję klasy źródła. 
Poniżej, dla czytelności całego rozwiązania, definicja klasy `TaxiEventSource`
```java
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.time.Instant;

// Wersja dla rozwiązań zewnętrznych
public class TaxiEventSource implements SourceFunction<Row> {
    private final String directoryPath;
    private final long elementDelayMillis;
    private final int maxElements;
    private transient volatile boolean running = true;

    public TaxiEventSource(String directoryPath, long elementDelayMillis, int maxElements) {
        this.directoryPath = directoryPath;
        this.elementDelayMillis = elementDelayMillis;
        this.maxElements = maxElements;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        running = true;
        File directory = new File(directoryPath);
        File[] files = directory.listFiles();

        if (files != null) {
            java.util.Arrays.sort(files);

            int elementCount = 0;

            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".csv")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        String line;
                        boolean headerSkipped = false;

                        while ((line = reader.readLine()) != null && running) {
                            if (!headerSkipped) {
                                headerSkipped = true;
                                continue;
                            }

                            Row taxiEvent = fromString(line);
                            Long epochMillis = (Long) taxiEvent.getField(2);
                            if (epochMillis == null) {
                                throw new IllegalStateException("Epoch millis is null in record: " + taxiEvent);
                            }
                            Timestamp timestamp = new Timestamp(epochMillis);
                            synchronized (sourceContext.getCheckpointLock()) {
                                sourceContext.collectWithTimestamp(taxiEvent, timestamp.getTime());
                            }
                            elementCount++;
                            if (elementCount >= maxElements) {
                                cancel();
                                break;
                            }
                            Thread.sleep(elementDelayMillis);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static Row fromString(String line) {
        String[] parts = line.split(",");
        if (parts.length == 9) {
            // ISO-8601 format: 2021-08-01T12:34:56.789Z
            Instant instant = Instant.parse(parts[2]); // Parsuje poprawnie ISO z 'Z' na końcu

            return Row.of(
                    Long.parseLong(parts[0]),      // Long
                    Integer.parseInt(parts[1]),    // Integer
                    instant.toEpochMilli(),        // Long
                    Integer.parseInt(parts[3]),    // Integer
                    Integer.parseInt(parts[4]),    // Integer
                    Double.parseDouble(parts[5]),  // Double
                    Integer.parseInt(parts[6]),    // Integer
                    Double.parseDouble(parts[7]),  // Double
                    Integer.parseInt(parts[8])     // Integer
            );
        } else {
            throw new IllegalArgumentException("Malformed line: " + line);
        }
    }
}
```
