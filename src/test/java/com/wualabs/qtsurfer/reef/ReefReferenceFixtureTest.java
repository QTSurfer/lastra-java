package com.wualabs.qtsurfer.reef;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Generates the canonical reference fixture and validates it in Java.
 * The same fixture file and expected values are used by reef-ts tests.
 *
 * <p>Seed: Random(42), baseTs: 1711152000000000000 (ns)
 *
 * <p>Run with -Dreef.fixture.dir=path to write the .reef file to disk for TS tests.
 */
class ReefReferenceFixtureTest {

    static final long BASE_TS = 1_711_152_000_000_000_000L;
    static final int SERIES_ROWS = 100;
    static final int EVENT_COUNT = 5;

    // --- Reference data (deterministic via Random(42)) ---

    static long[] seriesTs() {
        long[] ts = new long[SERIES_ROWS];
        for (int i = 0; i < SERIES_ROWS; i++) {
            ts[i] = BASE_TS + i * 1_000_000_000L;
        }
        return ts;
    }

    static double[] seriesClose() {
        double[] close = new double[SERIES_ROWS];
        Random rng = new Random(42);
        for (int i = 0; i < SERIES_ROWS; i++) {
            close[i] = Math.round((65000.0 + Math.sin(i * 0.01) * 500 + rng.nextDouble() * 10) * 100.0) / 100.0;
        }
        return close;
    }

    static double[] seriesRsi() {
        double[] rsi = new double[SERIES_ROWS];
        Random rng = new Random(99);
        for (int i = 0; i < SERIES_ROWS; i++) {
            rsi[i] = Math.round(rng.nextDouble() * 10000.0) / 100.0; // 0.00 - 100.00
        }
        return rsi;
    }

    static long[] eventTs() {
        return new long[] {
            BASE_TS + 10_000_000_000L,
            BASE_TS + 25_000_000_000L,
            BASE_TS + 40_000_000_000L,
            BASE_TS + 60_000_000_000L,
            BASE_TS + 85_000_000_000L
        };
    }

    static byte[][] eventTypes() {
        return new byte[][] {
            "BUY".getBytes(StandardCharsets.UTF_8),
            "SELL".getBytes(StandardCharsets.UTF_8),
            "BUY".getBytes(StandardCharsets.UTF_8),
            "STOP_LOSS".getBytes(StandardCharsets.UTF_8),
            "SELL".getBytes(StandardCharsets.UTF_8)
        };
    }

    static byte[][] eventData() {
        return new byte[][] {
            "{\"price\":65042.17,\"qty\":0.5}".getBytes(StandardCharsets.UTF_8),
            "{\"price\":65100.33,\"qty\":0.5}".getBytes(StandardCharsets.UTF_8),
            null,
            "{\"price\":64900.00,\"reason\":\"stop_hit\"}".getBytes(StandardCharsets.UTF_8),
            null
        };
    }

    byte[] generateFixture() throws Exception {
        long[] ts = seriesTs();
        double[] close = seriesClose();
        double[] rsi = seriesRsi();
        long[] evTs = eventTs();
        byte[][] evTypes = eventTypes();
        byte[][] evData = eventData();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ReefWriter w = new ReefWriter(baos)) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.addSeriesColumn("rsi1", Reef.DataType.DOUBLE, Reef.Codec.ALP,
                    Map.of("indicator", "rsi", "periods", "14"));
            w.addEventColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addEventColumn("type", Reef.DataType.BINARY, Reef.Codec.VARLEN);
            w.addEventColumn("data", Reef.DataType.BINARY, Reef.Codec.VARLEN_ZSTD);
            w.writeSeries(SERIES_ROWS, ts, close, rsi);
            w.writeEvents(EVENT_COUNT, evTs, evTypes, evData);
        }
        return baos.toByteArray();
    }

    @Test
    void testReferenceFixtureRoundtrip() throws Exception {
        byte[] fixture = generateFixture();

        ReefReader r = ReefReader.from(fixture);

        // Header
        assertThat(r.seriesRowCount()).isEqualTo(SERIES_ROWS);
        assertThat(r.eventsRowCount()).isEqualTo(EVENT_COUNT);
        assertThat(r.seriesColumns()).hasSize(3);
        assertThat(r.eventColumns()).hasSize(3);

        // Series columns
        assertThat(r.seriesColumns().get(0).name()).isEqualTo("ts");
        assertThat(r.seriesColumns().get(1).name()).isEqualTo("close");
        assertThat(r.seriesColumns().get(2).name()).isEqualTo("rsi1");

        // Metadata
        assertThat(r.getSeriesColumn("rsi1").metadata()).containsEntry("indicator", "rsi");
        assertThat(r.getSeriesColumn("rsi1").metadata()).containsEntry("periods", "14");

        // Series data: timestamps
        long[] gotTs = r.readSeriesLong("ts");
        long[] expectedTs = seriesTs();
        assertThat(gotTs).containsExactly(expectedTs);

        // Series data: close (first 5 + last 5 spot checks)
        double[] gotClose = r.readSeriesDouble("close");
        double[] expectedClose = seriesClose();
        assertThat(gotClose).hasSize(SERIES_ROWS);
        for (int i = 0; i < 5; i++) {
            assertThat(Double.doubleToRawLongBits(gotClose[i]))
                    .as("close[%d]", i)
                    .isEqualTo(Double.doubleToRawLongBits(expectedClose[i]));
        }
        for (int i = SERIES_ROWS - 5; i < SERIES_ROWS; i++) {
            assertThat(Double.doubleToRawLongBits(gotClose[i]))
                    .as("close[%d]", i)
                    .isEqualTo(Double.doubleToRawLongBits(expectedClose[i]));
        }

        // Series data: rsi
        double[] gotRsi = r.readSeriesDouble("rsi1");
        double[] expectedRsi = seriesRsi();
        assertThat(gotRsi).hasSize(SERIES_ROWS);
        for (int i = 0; i < 5; i++) {
            assertThat(Double.doubleToRawLongBits(gotRsi[i]))
                    .as("rsi1[%d]", i)
                    .isEqualTo(Double.doubleToRawLongBits(expectedRsi[i]));
        }

        // Events: timestamps
        long[] gotEvTs = r.readEventLong("ts");
        assertThat(gotEvTs).containsExactly(eventTs());

        // Events: types
        byte[][] gotTypes = r.readEventBinary("type");
        assertThat(new String(gotTypes[0], StandardCharsets.UTF_8)).isEqualTo("BUY");
        assertThat(new String(gotTypes[1], StandardCharsets.UTF_8)).isEqualTo("SELL");
        assertThat(new String(gotTypes[2], StandardCharsets.UTF_8)).isEqualTo("BUY");
        assertThat(new String(gotTypes[3], StandardCharsets.UTF_8)).isEqualTo("STOP_LOSS");
        assertThat(new String(gotTypes[4], StandardCharsets.UTF_8)).isEqualTo("SELL");

        // Events: data (with nulls)
        byte[][] gotData = r.readEventBinary("data");
        assertThat(new String(gotData[0], StandardCharsets.UTF_8)).contains("65042.17");
        assertThat(new String(gotData[1], StandardCharsets.UTF_8)).contains("65100.33");
        assertThat(gotData[2]).isNull();
        assertThat(new String(gotData[3], StandardCharsets.UTF_8)).contains("stop_hit");
        assertThat(gotData[4]).isNull();

        // Print expected values for TS test reference
        System.out.println("=== Reference values for TS tests ===");
        System.out.printf("ts[0] = %d%n", expectedTs[0]);
        System.out.printf("ts[99] = %d%n", expectedTs[99]);
        System.out.printf("close[0] = %.2f%n", expectedClose[0]);
        System.out.printf("close[1] = %.2f%n", expectedClose[1]);
        System.out.printf("close[99] = %.2f%n", expectedClose[99]);
        System.out.printf("rsi1[0] = %.2f%n", expectedRsi[0]);
        System.out.printf("rsi1[1] = %.2f%n", expectedRsi[1]);

        // Write fixture to disk if dir specified
        String fixtureDir = System.getProperty("reef.fixture.dir");
        if (fixtureDir != null) {
            Path dir = Path.of(fixtureDir);
            Files.createDirectories(dir);
            Files.write(dir.resolve("reference.reef"), fixture);
            System.out.println("Fixture written to " + dir.resolve("reference.reef"));
        }
    }
}
