package com.wualabs.qtsurfer.reef.codec;

import com.wualabs.qtsurfer.reef.Reef;
import com.wualabs.qtsurfer.reef.ReefReader;
import com.wualabs.qtsurfer.reef.ReefWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class GorillaCodecTest {

    @Test
    void roundtripFinancialData() {
        double[] prices = {65007.28, 65011.83, 65011.83, 65015.50, 64998.12, 65023.45, 65023.45, 65030.00};
        byte[] encoded = GorillaCodec.encode(prices, prices.length);
        double[] decoded = GorillaCodec.decode(encoded, prices.length);
        assertThat(decoded).containsExactly(prices);
    }

    @Test
    void roundtripIdenticalValues() {
        double[] values = new double[100];
        java.util.Arrays.fill(values, 42.0);
        byte[] encoded = GorillaCodec.encode(values, values.length);
        double[] decoded = GorillaCodec.decode(encoded, values.length);
        assertThat(decoded).containsExactly(values);
        // Identical values should compress very well: 1 bit per value after first
        assertThat(encoded.length).isLessThan(30);
    }

    @Test
    void roundtripSpecialValues() {
        double[] values = {0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_NORMAL};
        byte[] encoded = GorillaCodec.encode(values, values.length);
        double[] decoded = GorillaCodec.decode(encoded, values.length);
        // Compare raw bits for NaN and -0.0 correctness
        for (int i = 0; i < values.length; i++) {
            assertThat(Double.doubleToRawLongBits(decoded[i]))
                    .as("value[%d]", i)
                    .isEqualTo(Double.doubleToRawLongBits(values[i]));
        }
    }

    @Test
    void roundtripSingleValue() {
        double[] values = {123.456};
        byte[] encoded = GorillaCodec.encode(values, 1);
        double[] decoded = GorillaCodec.decode(encoded, 1);
        assertThat(decoded).containsExactly(values);
    }

    @Test
    void roundtripEmpty() {
        byte[] encoded = GorillaCodec.encode(new double[0], 0);
        double[] decoded = GorillaCodec.decode(encoded, 0);
        assertThat(encoded).isEmpty();
        assertThat(decoded).isEmpty();
    }

    @Test
    void endToEndReefRoundtrip() throws Exception {
        double[] prices = {65007.28, 65011.83, 65015.50, 64998.12, 65023.45};
        long[] timestamps = {1000L, 2000L, 3000L, 4000L, 5000L};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ReefWriter w = new ReefWriter(out)) {
            w.addSeriesColumn("ts", Reef.DataType.LONG, Reef.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Reef.DataType.DOUBLE, Reef.Codec.GORILLA);
            w.writeSeries(5, timestamps, prices);
        }

        ReefReader r = ReefReader.from(out.toByteArray());
        assertThat(r.readSeriesLong("ts")).containsExactly(timestamps);
        assertThat(r.readSeriesDouble("close")).containsExactly(prices);
    }
}
