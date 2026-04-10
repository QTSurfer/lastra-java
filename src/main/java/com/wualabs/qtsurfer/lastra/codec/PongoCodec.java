package com.wualabs.qtsurfer.lastra.codec;

/**
 * Pongo codec: decimal-aware erasure + Gorilla XOR compression for double columns.
 *
 * <p>Achieves significantly better compression than plain Gorilla on decimal-native data
 * (financial prices, sensor readings) by zeroing trailing mantissa bits before XOR encoding.
 *
 * <p>Flag protocol (written before each XOR-compressed value):
 * <ul>
 *   <li><b>"0"</b> (1 bit): erased, same dp as previous</li>
 *   <li><b>"10"</b> (2 bits): not erased (special value or not decimal-native)</li>
 *   <li><b>"11XXXXX"</b> (7 bits): erased, new dp (5 bits, 0-18)</li>
 * </ul>
 *
 * <p>Wire format: {@code [4 bytes count LE] [bitstream with interleaved flags + Gorilla XOR]}.
 */
public final class PongoCodec {

    /** Minimum erasable bits to justify the flag overhead. */
    private static final int MIN_ERASABLE_BITS = 5;

    private PongoCodec() {}

    public static byte[] encode(double[] values, int count) {
        if (count == 0) return new byte[0];

        GorillaCodec.BitWriter w = new GorillaCodec.BitWriter(count * 2);
        w.writeRawInt(count);

        // First value: raw 64 bits (no flag for first value)
        long storedVal = Double.doubleToRawLongBits(values[0]);
        w.writeBits(storedVal, 64);

        int storedLeadingZeros = Integer.MAX_VALUE;
        int storedTrailingZeros = 0;
        int lastDp = Integer.MAX_VALUE;

        for (int i = 1; i < count; i++) {
            double value = values[i];
            long bits = Double.doubleToRawLongBits(value);

            // Attempt Pongo erasure
            long bitsToXor = bits;
            boolean erased = false;
            int dp = -1;

            if (value != 0.0 && !Double.isInfinite(value) && !Double.isNaN(value)) {
                dp = PongoEraser.detectDecimalPlaces(value);
                if (dp >= 0) {
                    int erasable = PongoEraser.computeErasableBits(value, dp);
                    if (erasable >= MIN_ERASABLE_BITS) {
                        long erasedBits = PongoEraser.eraseBits(bits, erasable);
                        // Verify round-trip
                        long restored = PongoEraser.restore(erasedBits, dp);
                        if (restored == bits) {
                            bitsToXor = erasedBits;
                            erased = true;
                        }
                    }
                }
            }

            // Write Pongo flag
            if (!erased) {
                // "10" = not erased
                w.writeBit(1);
                w.writeBit(0);
            } else if (dp == lastDp) {
                // "0" = erased, same dp
                w.writeBit(0);
            } else {
                // "11" + 5-bit dp
                w.writeBit(1);
                w.writeBit(1);
                w.writeBits(dp, 5);
                lastDp = dp;
            }

            // Gorilla XOR compression
            long xor = storedVal ^ bitsToXor;

            if (xor == 0) {
                w.writeBit(0);
            } else {
                int leadingZeros = Long.numberOfLeadingZeros(xor);
                int trailingZeros = Long.numberOfTrailingZeros(xor);
                if (leadingZeros >= 32) leadingZeros = 31;

                w.writeBit(1);

                if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                    w.writeBit(0);
                    int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
                    w.writeBits(xor >>> storedTrailingZeros, significantBits);
                } else {
                    w.writeBit(1);
                    w.writeBits(leadingZeros, 5);
                    int significantBits = 64 - leadingZeros - trailingZeros;
                    w.writeBits(significantBits, 6);
                    w.writeBits(xor >>> trailingZeros, significantBits);
                    storedLeadingZeros = leadingZeros;
                    storedTrailingZeros = trailingZeros;
                }
            }

            storedVal = bitsToXor;
        }

        return w.toByteArray();
    }

    public static double[] decode(byte[] data, int count) {
        if (count == 0) return new double[0];

        GorillaCodec.BitReader r = new GorillaCodec.BitReader(data);

        int storedCount = r.readRawInt();
        if (storedCount != count) {
            throw new IllegalArgumentException("Count mismatch: header=" + storedCount + ", expected=" + count);
        }

        double[] result = new double[count];

        // First value: raw 64 bits
        long storedVal = r.readBits(64);
        result[0] = Double.longBitsToDouble(storedVal);

        int storedLeadingZeros = Integer.MAX_VALUE;
        int storedTrailingZeros = 0;
        int lastDp = Integer.MAX_VALUE;

        for (int i = 1; i < count; i++) {
            // Read Pongo flag
            boolean erased;
            int dp = lastDp;

            int flag = r.readBit();
            if (flag == 0) {
                // "0" = erased, same dp
                erased = true;
            } else {
                int secondBit = r.readBit();
                if (secondBit == 0) {
                    // "10" = not erased
                    erased = false;
                } else {
                    // "11XXXXX" = erased, new dp
                    erased = true;
                    dp = (int) r.readBits(5);
                    lastDp = dp;
                }
            }

            // Gorilla XOR decompression
            if (r.readBit() == 0) {
                // Same value as previous
            } else {
                if (r.readBit() == 1) {
                    // New window
                    storedLeadingZeros = (int) r.readBits(5);
                    int significantBits = (int) r.readBits(6);
                    if (significantBits == 0) significantBits = 64;
                    storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
                }
                int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
                long value = r.readBits(significantBits);
                value <<= storedTrailingZeros;
                storedVal = storedVal ^ value;
            }

            // Restore if erased
            if (erased) {
                long restoredBits = PongoEraser.restore(storedVal, dp);
                result[i] = Double.longBitsToDouble(restoredBits);
            } else {
                result[i] = Double.longBitsToDouble(storedVal);
            }
        }

        return result;
    }
}
