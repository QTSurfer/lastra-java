package com.wualabs.qtsurfer.reef.codec;

import java.io.ByteArrayOutputStream;

/**
 * Delta-of-delta + zigzag varint codec for long columns (timestamps).
 *
 * <p>Encodes regular intervals as ~1 byte per value. Wire format:
 * <pre>
 * [8 bytes] first value (LE long)
 * [varint]  first delta (zigzag)
 * [varint]  delta-of-delta[2] (zigzag)
 * ...
 * </pre>
 */
public final class DeltaVarintCodec {

    private DeltaVarintCodec() {}

    public static byte[] encode(long[] values, int count) {
        if (count == 0) return new byte[0];
        ByteArrayOutputStream buf = new ByteArrayOutputStream(8 + count);
        // First value: fixed 8 bytes LE
        long first = values[0];
        writeLongLE(buf, first);
        if (count > 1) {
            long prevDelta = values[1] - values[0];
            writeZigzagVarint(buf, prevDelta);
            for (int i = 2; i < count; i++) {
                long delta = values[i] - values[i - 1];
                long dod = delta - prevDelta;
                writeZigzagVarint(buf, dod);
                prevDelta = delta;
            }
        }
        return buf.toByteArray();
    }

    public static long[] decode(byte[] data, int count) {
        if (count == 0) return new long[0];
        long[] result = new long[count];
        int pos = 0;
        // First value: fixed 8 bytes LE
        result[0] = readLongLE(data, pos);
        pos += 8;
        if (count > 1) {
            int[] posRef = {pos};
            long prevDelta = readZigzagVarint(data, posRef);
            result[1] = result[0] + prevDelta;
            for (int i = 2; i < count; i++) {
                long dod = readZigzagVarint(data, posRef);
                long delta = prevDelta + dod;
                result[i] = result[i - 1] + delta;
                prevDelta = delta;
            }
        }
        return result;
    }

    private static void writeLongLE(ByteArrayOutputStream out, long value) {
        for (int i = 0; i < 8; i++) {
            out.write((int) (value & 0xFF));
            value >>>= 8;
        }
    }

    private static long readLongLE(byte[] data, int pos) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result |= (data[pos + i] & 0xFFL) << (i * 8);
        }
        return result;
    }

    static void writeZigzagVarint(ByteArrayOutputStream out, long value) {
        long encoded = (value << 1) ^ (value >> 63);
        while ((encoded & ~0x7FL) != 0) {
            out.write((int) ((encoded & 0x7F) | 0x80));
            encoded >>>= 7;
        }
        out.write((int) (encoded & 0x7F));
    }

    static long readZigzagVarint(byte[] data, int[] posRef) {
        int pos = posRef[0];
        long result = 0;
        int shift = 0;
        while (true) {
            byte b = data[pos++];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        posRef[0] = pos;
        return (result >>> 1) ^ -(result & 1);
    }
}
