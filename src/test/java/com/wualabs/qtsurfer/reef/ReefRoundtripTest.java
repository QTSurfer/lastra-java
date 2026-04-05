package com.wualabs.qtsurfer.reef;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ReefRoundtripTest {

    @Test
    void testSeriesOnlyTickerData() throws Exception {
        int rows = 3600;
        long[] ts = new long[rows];
        double[] close = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L; // ns
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            close[i] = Math.round((65000.0 + Math.sin(i * 0.001) * 500 + rng.nextDouble() * 10) * 100.0) / 100.0;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ReefWriter w = new ReefWriter(baos)) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.writeSeries(rows, ts, close);
        }

        byte[] reefBytes = baos.toByteArray();
        double ratio = (double) (rows * 16) / reefBytes.length;
        System.out.printf("Series only: %d rows, %d bytes, ratio=%.2fx%n", rows, reefBytes.length, ratio);

        ReefReader r = ReefReader.from(reefBytes);
        assertThat(r.seriesRowCount()).isEqualTo(rows);
        assertThat(r.seriesColumns()).hasSize(2);

        long[] gotTs = r.readSeriesLong("ts");
        double[] gotClose = r.readSeriesDouble("close");
        assertThat(gotTs).containsExactly(ts);
        for (int i = 0; i < rows; i++) {
            assertThat(Double.doubleToRawLongBits(gotClose[i]))
                    .as("close[%d]", i)
                    .isEqualTo(Double.doubleToRawLongBits(close[i]));
        }
    }

    @Test
    void testOhlcvData() throws Exception {
        int rows = 1000;
        long[] ts = new long[rows];
        double[] open = new double[rows];
        double[] high = new double[rows];
        double[] low = new double[rows];
        double[] close = new double[rows];
        double[] volume = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L;
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            double base = 65000.0 + Math.sin(i * 0.001) * 500 + rng.nextDouble() * 10;
            close[i] = Math.round(base * 100.0) / 100.0;
            open[i] = Math.round((base - rng.nextDouble() * 5) * 100.0) / 100.0;
            high[i] = Math.round((base + rng.nextDouble() * 10) * 100.0) / 100.0;
            low[i] = Math.round((base - rng.nextDouble() * 10) * 100.0) / 100.0;
            volume[i] = Math.round(rng.nextDouble() * 100000 * 100.0) / 100.0;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ReefWriter w = new ReefWriter(baos)) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("open", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.addSeriesColumn("high", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.addSeriesColumn("low", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.addSeriesColumn("close", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.addSeriesColumn("volume", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.writeSeries(rows, ts, open, high, low, close, volume);
        }

        byte[] reefBytes = baos.toByteArray();
        double ratio = (double) (rows * 48) / reefBytes.length;
        System.out.printf("OHLCV: %d rows, %d bytes, ratio=%.2fx%n", rows, reefBytes.length, ratio);

        ReefReader r = ReefReader.from(reefBytes);
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
        assertBitExact(r.readSeriesDouble("close"), close);
        assertBitExact(r.readSeriesDouble("volume"), volume);
    }

    @Test
    void testSeriesWithEvents() throws Exception {
        int rows = 500;
        long[] ts = new long[rows];
        double[] close = new double[rows];
        Random rng = new Random(42);
        long baseTs = 1711152000000000000L;
        for (int i = 0; i < rows; i++) {
            ts[i] = baseTs + i * 1_000_000_000L;
            close[i] = Math.round((65000.0 + rng.nextDouble() * 100) * 100.0) / 100.0;
        }

        // Events: 5 signals at specific timestamps
        int eventCount = 5;
        long[] eventTs = {
                baseTs + 50_000_000_000L,
                baseTs + 120_000_000_000L,
                baseTs + 200_000_000_000L,
                baseTs + 350_000_000_000L,
                baseTs + 480_000_000_000L
        };
        byte[][] eventTypes = {
                "BUY".getBytes(StandardCharsets.UTF_8),
                "SELL".getBytes(StandardCharsets.UTF_8),
                "BUY".getBytes(StandardCharsets.UTF_8),
                "STOP_LOSS".getBytes(StandardCharsets.UTF_8),
                "SELL".getBytes(StandardCharsets.UTF_8)
        };
        byte[][] eventData = {
                "{\"price\":65042.17,\"qty\":0.5}".getBytes(StandardCharsets.UTF_8),
                "{\"price\":65100.33,\"qty\":0.5}".getBytes(StandardCharsets.UTF_8),
                null,
                "{\"price\":64900.00,\"reason\":\"stop_hit\"}".getBytes(StandardCharsets.UTF_8),
                null
        };

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ReefWriter w = new ReefWriter(baos)) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.addEventColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addEventColumn("type", Reef.DataType.BINARY, Reef.Codec.VARLEN);
            w.addEventColumn("data", Reef.DataType.BINARY, Reef.Codec.VARLEN_ZSTD);
            w.writeSeries(rows, ts, close);
            w.writeEvents(eventCount, eventTs, eventTypes, eventData);
        }

        byte[] reefBytes = baos.toByteArray();
        System.out.printf("Series+Events: %d series rows + %d events, %d bytes%n",
                rows, eventCount, reefBytes.length);

        ReefReader r = ReefReader.from(reefBytes);
        assertThat(r.seriesRowCount()).isEqualTo(rows);
        assertThat(r.eventsRowCount()).isEqualTo(eventCount);
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
        assertBitExact(r.readSeriesDouble("close"), close);

        assertThat(r.readEventLong("ts")).containsExactly(eventTs);
        byte[][] gotTypes = r.readEventBinary("type");
        assertThat(new String(gotTypes[0], StandardCharsets.UTF_8)).isEqualTo("BUY");
        assertThat(new String(gotTypes[3], StandardCharsets.UTF_8)).isEqualTo("STOP_LOSS");
        byte[][] gotData = r.readEventBinary("data");
        assertThat(gotData[2]).isNull();
        assertThat(gotData[4]).isNull();
        assertThat(new String(gotData[0], StandardCharsets.UTF_8)).contains("65042.17");
    }

    @Test
    void testColumnMetadata() throws Exception {
        int rows = 100;
        long[] ts = new long[rows];
        double[] ema = new double[rows];
        for (int i = 0; i < rows; i++) {
            ts[i] = 1711152000000000000L + i * 1_000_000_000L;
            ema[i] = Math.round((65000.0 + i * 0.1) * 100.0) / 100.0;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ReefWriter w = new ReefWriter(baos)) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("ema1", Reef.DataType.DOUBLE, Reef.Codec.ALP,
                    Map.of("indicator", "ema", "periods", "10"));
            w.writeSeries(rows, ts, ema);
        }

        ReefReader r = ReefReader.from(baos.toByteArray());
        ColumnDescriptor emaCol = r.getSeriesColumn("ema1");
        assertThat(emaCol.metadata()).containsEntry("indicator", "ema");
        assertThat(emaCol.metadata()).containsEntry("periods", "10");
        assertBitExact(r.readSeriesDouble("ema1"), ema);
    }

    @Test
    void testReadFromInputStream() throws Exception {
        int rows = 50;
        long[] ts = new long[rows];
        double[] values = new double[rows];
        for (int i = 0; i < rows; i++) {
            ts[i] = i * 1000L;
            values[i] = i * 1.5;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ReefWriter w = new ReefWriter(baos)) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("v", Reef.DataType.DOUBLE, Reef.Codec.ALP);
            w.writeSeries(rows, ts, values);
        }

        ReefReader r = ReefReader.from(new ByteArrayInputStream(baos.toByteArray()));
        assertThat(r.readSeriesLong("ts")).containsExactly(ts);
        assertBitExact(r.readSeriesDouble("v"), values);
    }

    private static void assertBitExact(double[] actual, double[] expected) {
        assertThat(actual).hasSize(expected.length);
        for (int i = 0; i < expected.length; i++) {
            assertThat(Double.doubleToRawLongBits(actual[i]))
                    .as("value[%d] expected=%s got=%s", i, expected[i], actual[i])
                    .isEqualTo(Double.doubleToRawLongBits(expected[i]));
        }
    }
}
