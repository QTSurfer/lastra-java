package com.wualabs.qtsurfer.lastra.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Raw (uncompressed) codec for long and double columns. Little-endian byte order.
 */
public final class RawCodec {

    private RawCodec() {}

    public static byte[] encodeLongs(long[] values, int count) {
        ByteBuffer buf = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) buf.putLong(values[i]);
        return buf.array();
    }

    public static long[] decodeLongs(byte[] data, int count) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long[] result = new long[count];
        for (int i = 0; i < count; i++) result[i] = buf.getLong();
        return result;
    }

    public static byte[] encodeDoubles(double[] values, int count) {
        ByteBuffer buf = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) buf.putDouble(values[i]);
        return buf.array();
    }

    public static double[] decodeDoubles(byte[] data, int count) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        double[] result = new double[count];
        for (int i = 0; i < count; i++) result[i] = buf.getDouble();
        return result;
    }
}
