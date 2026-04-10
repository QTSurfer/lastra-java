package com.wualabs.qtsurfer.lastra.codec;

/**
 * Gorilla XOR compression for double columns.
 *
 * <p>Implements the value compression from the Facebook Gorilla paper (VLDB 2015),
 * without timestamp encoding (Lastra handles timestamps separately via DELTA_VARINT).
 *
 * <p>Algorithm:
 * <ol>
 *   <li>First value stored as raw 64-bit IEEE 754</li>
 *   <li>Subsequent values XOR'd with previous value</li>
 *   <li>XOR == 0: single "0" bit (identical value)</li>
 *   <li>XOR != 0, leading/trailing zeros fit previous window: "10" + significant bits</li>
 *   <li>XOR != 0, new window: "11" + 5-bit leading zeros + 6-bit length + significant bits</li>
 * </ol>
 *
 * <p>Wire format: {@code [4 bytes count LE] [bitstream]}.
 */
public final class GorillaCodec {

    private GorillaCodec() {}

    public static byte[] encode(double[] values, int count) {
        if (count == 0) return new byte[0];

        BitWriter w = new BitWriter(count * 2); // conservative initial size

        // Write count as 4 bytes LE
        w.writeRawInt(count);

        // First value: raw 64 bits
        long storedVal = Double.doubleToRawLongBits(values[0]);
        w.writeBits(storedVal, 64);

        int storedLeadingZeros = Integer.MAX_VALUE;
        int storedTrailingZeros = 0;

        for (int i = 1; i < count; i++) {
            long val = Double.doubleToRawLongBits(values[i]);
            long xor = storedVal ^ val;

            if (xor == 0) {
                // Same value: write single 0 bit
                w.writeBit(0);
            } else {
                int leadingZeros = Long.numberOfLeadingZeros(xor);
                int trailingZeros = Long.numberOfTrailingZeros(xor);

                // Cap leading zeros to 5-bit range (0-31)
                if (leadingZeros >= 32) {
                    leadingZeros = 31;
                }

                w.writeBit(1);

                if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                    // Fits within previous window: "0" + significant bits
                    w.writeBit(0);
                    int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
                    w.writeBits(xor >>> storedTrailingZeros, significantBits);
                } else {
                    // New window: "1" + 5-bit leading + 6-bit length + significant bits
                    w.writeBit(1);
                    w.writeBits(leadingZeros, 5);
                    int significantBits = 64 - leadingZeros - trailingZeros;
                    w.writeBits(significantBits, 6);
                    w.writeBits(xor >>> trailingZeros, significantBits);

                    storedLeadingZeros = leadingZeros;
                    storedTrailingZeros = trailingZeros;
                }
            }
            storedVal = val;
        }

        return w.toByteArray();
    }

    public static double[] decode(byte[] data, int count) {
        if (count == 0) return new double[0];

        BitReader r = new BitReader(data);

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

        for (int i = 1; i < count; i++) {
            if (r.readBit() == 0) {
                // Same value
                result[i] = Double.longBitsToDouble(storedVal);
            } else {
                if (r.readBit() == 1) {
                    // New window
                    storedLeadingZeros = (int) r.readBits(5);
                    int significantBits = (int) r.readBits(6);
                    if (significantBits == 0) {
                        significantBits = 64; // overflow case
                    }
                    storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
                }
                // Read significant bits
                int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
                long value = r.readBits(significantBits);
                value <<= storedTrailingZeros;
                storedVal = storedVal ^ value;
                result[i] = Double.longBitsToDouble(storedVal);
            }
        }

        return result;
    }

    // --- Bit-level I/O ---

    static final class BitWriter {
        private byte[] buf;
        private int bytePos;
        private int bitPos; // bits remaining in current byte (8 = fresh byte)

        BitWriter(int initialBytes) {
            buf = new byte[Math.max(initialBytes, 16)];
            bytePos = 0;
            bitPos = 8;
        }

        void writeRawInt(int value) {
            ensureCapacity(4);
            buf[bytePos++] = (byte) (value & 0xFF);
            buf[bytePos++] = (byte) ((value >>> 8) & 0xFF);
            buf[bytePos++] = (byte) ((value >>> 16) & 0xFF);
            buf[bytePos++] = (byte) ((value >>> 24) & 0xFF);
            bitPos = 8;
        }

        void writeBit(int bit) {
            ensureCapacity(1);
            if (bitPos == 8) {
                buf[bytePos] = 0;
            }
            if (bit != 0) {
                buf[bytePos] |= (byte) (1 << (bitPos - 1));
            }
            bitPos--;
            if (bitPos == 0) {
                bytePos++;
                bitPos = 8;
            }
        }

        void writeBits(long value, int numBits) {
            for (int i = numBits - 1; i >= 0; i--) {
                writeBit((int) ((value >>> i) & 1));
            }
        }

        byte[] toByteArray() {
            int len = bitPos == 8 ? bytePos : bytePos + 1;
            byte[] result = new byte[len];
            System.arraycopy(buf, 0, result, 0, len);
            return result;
        }

        private void ensureCapacity(int extra) {
            int needed = bytePos + extra + 1;
            if (needed > buf.length) {
                byte[] newBuf = new byte[Math.max(buf.length * 2, needed)];
                System.arraycopy(buf, 0, newBuf, 0, bytePos + 1);
                buf = newBuf;
            }
        }
    }

    static final class BitReader {
        private final byte[] buf;
        private int bytePos;
        private int bitPos; // bits remaining in current byte (8 = fresh byte)

        BitReader(byte[] data) {
            this.buf = data;
            this.bytePos = 0;
            this.bitPos = 8;
        }

        int readRawInt() {
            int v = (buf[bytePos] & 0xFF)
                    | ((buf[bytePos + 1] & 0xFF) << 8)
                    | ((buf[bytePos + 2] & 0xFF) << 16)
                    | ((buf[bytePos + 3] & 0xFF) << 24);
            bytePos += 4;
            bitPos = 8;
            return v;
        }

        int readBit() {
            int bit = (buf[bytePos] >>> (bitPos - 1)) & 1;
            bitPos--;
            if (bitPos == 0) {
                bytePos++;
                bitPos = 8;
            }
            return bit;
        }

        long readBits(int numBits) {
            long result = 0;
            for (int i = 0; i < numBits; i++) {
                result = (result << 1) | readBit();
            }
            return result;
        }
    }
}
