package com.wualabs.qtsurfer.lastra.codec;

import com.wualabs.qtsurfer.lastra.Lastra;
import com.wualabs.qtsurfer.lastra.LastraReader;
import com.wualabs.qtsurfer.lastra.LastraWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class PongoCodecTest {

    @Test
    void roundtripFinancialData2dp() {
        // BTC/USDT style: 2 decimal places
        double[] prices = {65007.28, 65011.83, 65011.83, 65015.50, 64998.12, 65023.45, 65023.45, 65030.00};
        byte[] encoded = PongoCodec.encode(prices, prices.length);
        double[] decoded = PongoCodec.decode(encoded, prices.length);
        assertThat(decoded).containsExactly(prices);
    }

    @Test
    void roundtripFinancialData5dp() {
        // ETH/BTC style: ~5 decimal places
        double[] prices = {0.03215, 0.03218, 0.03218, 0.03220, 0.03199, 0.03225};
        byte[] encoded = PongoCodec.encode(prices, prices.length);
        double[] decoded = PongoCodec.decode(encoded, prices.length);
        assertThat(decoded).containsExactly(prices);
    }

    @Test
    void roundtripMixedDecimalPlaces() {
        // Mix of different decimal precisions
        double[] values = {100.0, 100.5, 100.55, 100.555, 100.5555, 100.55555};
        byte[] encoded = PongoCodec.encode(values, values.length);
        double[] decoded = PongoCodec.decode(encoded, values.length);
        assertThat(decoded).containsExactly(values);
    }

    @Test
    void roundtripSpecialValues() {
        double[] values = {0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 42.0};
        byte[] encoded = PongoCodec.encode(values, values.length);
        double[] decoded = PongoCodec.decode(encoded, values.length);
        for (int i = 0; i < values.length; i++) {
            assertThat(Double.doubleToRawLongBits(decoded[i]))
                    .as("value[%d]", i)
                    .isEqualTo(Double.doubleToRawLongBits(values[i]));
        }
    }

    @Test
    void roundtripNonDecimalNative() {
        // Values that aren't decimal-native (e.g. irrational numbers)
        double[] values = {Math.PI, Math.E, Math.sqrt(2), Math.log(10)};
        byte[] encoded = PongoCodec.encode(values, values.length);
        double[] decoded = PongoCodec.decode(encoded, values.length);
        assertThat(decoded).containsExactly(values);
    }

    @Test
    void betterCompressionThanGorillaOnDecimalData() {
        // 100 values simulating financial prices with 2dp
        double[] prices = new double[100];
        prices[0] = 65000.00;
        java.util.Random rng = new java.util.Random(42);
        for (int i = 1; i < 100; i++) {
            prices[i] = Math.round((prices[i - 1] + (rng.nextGaussian() * 5)) * 100.0) / 100.0;
        }

        byte[] gorillaEncoded = GorillaCodec.encode(prices, prices.length);
        byte[] pongoEncoded = PongoCodec.encode(prices, prices.length);

        // Verify Pongo roundtrip
        double[] decoded = PongoCodec.decode(pongoEncoded, prices.length);
        assertThat(decoded).containsExactly(prices);

        // Pongo should compress better on 2dp financial data
        assertThat(pongoEncoded.length).isLessThan(gorillaEncoded.length);
    }

    @Test
    void endToEndLastraRoundtrip() throws Exception {
        double[] prices = {65007.28, 65011.83, 65015.50, 64998.12, 65023.45};
        long[] timestamps = {1000L, 2000L, 3000L, 4000L, 5000L};

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (LastraWriter w = new LastraWriter(out)) {
            w.addSeriesColumn("ts", Lastra.DataType.LONG, Lastra.Codec.DELTA_VARINT);
            w.addSeriesColumn("close", Lastra.DataType.DOUBLE, Lastra.Codec.PONGO);
            w.writeSeries(5, timestamps, prices);
        }

        LastraReader r = LastraReader.from(out.toByteArray());
        assertThat(r.readSeriesLong("ts")).containsExactly(timestamps);
        assertThat(r.readSeriesDouble("close")).containsExactly(prices);
    }

    @Test
    void roundtripSingleValue() {
        double[] values = {65007.28};
        byte[] encoded = PongoCodec.encode(values, 1);
        double[] decoded = PongoCodec.decode(encoded, 1);
        assertThat(decoded).containsExactly(values);
    }

    @Test
    void roundtripEmpty() {
        byte[] encoded = PongoCodec.encode(new double[0], 0);
        double[] decoded = PongoCodec.decode(encoded, 0);
        assertThat(encoded).isEmpty();
        assertThat(decoded).isEmpty();
    }
}
