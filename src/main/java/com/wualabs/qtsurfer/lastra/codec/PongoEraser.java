package com.wualabs.qtsurfer.lastra.codec;

/**
 * Decimal-aware lossless bit eraser for IEEE 754 doubles.
 *
 * <p>Based on the Pongo algorithm (Shen et al., CCDS 2025). Exploits the fact that most real-world
 * floating-point values originate from decimal numbers (prices, sensor readings). For these
 * "decimal-native" values, Pongo can safely zero out more trailing mantissa bits than
 * binary-only analysis, resulting in better XOR compression.
 *
 * <p>The erased value can be exactly restored via {@code Math.round(erased * 10^dp) / 10^dp}.
 */
public final class PongoEraser {

    /** Maximum decimal places we attempt to detect. Covers virtually all financial data. */
    static final int MAX_DECIMAL_PLACES = 18;

    private static final double LOG2_10 = Math.log(10) / Math.log(2); // ~3.3219

    /** Powers of 10 as longs. */
    static final long[] POW10 = new long[MAX_DECIMAL_PLACES + 1];

    /** Powers of 10 as doubles (precomputed for speed). */
    static final double[] DPOW10 = new double[MAX_DECIMAL_PLACES + 1];

    static {
        POW10[0] = 1L;
        DPOW10[0] = 1.0;
        for (int i = 1; i <= MAX_DECIMAL_PLACES; i++) {
            POW10[i] = POW10[i - 1] * 10;
            DPOW10[i] = POW10[i];
        }
    }

    private PongoEraser() {}

    /**
     * Detect the number of decimal places of a double value. Returns -1 if not decimal-native.
     *
     * <p>A value is "decimal-native" if there exists a dp in [0, MAX_DECIMAL_PLACES] such that
     * {@code Math.round(|value| * 10^dp) / 10^dp} reproduces the exact same IEEE 754 bits.
     */
    public static int detectDecimalPlaces(double value) {
        if (value == 0.0 || Double.isInfinite(value) || Double.isNaN(value)) {
            return -1;
        }
        double abs = Math.abs(value);
        long originalBits = Double.doubleToRawLongBits(abs);
        for (int dp = 0; dp <= MAX_DECIMAL_PLACES; dp++) {
            long scaled = Math.round(abs * DPOW10[dp]);
            double reconstructed = scaled / DPOW10[dp];
            if (Double.doubleToRawLongBits(reconstructed) == originalBits) {
                return dp;
            }
        }
        return -1;
    }

    /**
     * Compute the number of trailing mantissa bits that can be safely zeroed for a decimal-native
     * value with the given number of decimal places.
     *
     * @param value the original double
     * @param dp decimal places (from {@link #detectDecimalPlaces})
     * @return number of trailing bits that can be safely zeroed (0 if not erasable)
     */
    public static int computeErasableBits(double value, int dp) {
        if (dp <= 0) {
            return 0;
        }
        double abs = Math.abs(value);
        long bits = Double.doubleToRawLongBits(abs);
        int biasedExp = (int) ((bits >>> 52) & 0x7FF);
        int exponent = biasedExp - 1023;

        int maxErasable = (int) Math.floor(52 - exponent - 1 - dp * LOG2_10);
        return Math.max(0, maxErasable);
    }

    /**
     * Erase trailing mantissa bits from a double value.
     *
     * @param bits the raw long bits of the double
     * @param erasableBits number of trailing mantissa bits to zero
     * @return erased long bits
     */
    public static long eraseBits(long bits, int erasableBits) {
        if (erasableBits <= 0) {
            return bits;
        }
        long mask = -1L << erasableBits;
        return bits & mask;
    }

    /**
     * Restore the original double from erased bits.
     *
     * @param erasedBits the erased long bits
     * @param dp number of decimal places
     * @return the restored original long bits
     */
    public static long restore(long erasedBits, int dp) {
        if (dp <= 0) {
            return erasedBits;
        }
        double erased = Double.longBitsToDouble(erasedBits);
        double abs = Math.abs(erased);
        long scaled = Math.round(abs * DPOW10[dp]);
        double restored = scaled / DPOW10[dp];
        if (erased < 0) {
            restored = -restored;
        }
        return Double.doubleToRawLongBits(restored);
    }
}
