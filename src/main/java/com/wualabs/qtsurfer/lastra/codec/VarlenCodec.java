package com.wualabs.qtsurfer.lastra.codec;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Variable-length binary codec. Supports optional ZSTD or gzip block compression.
 *
 * <p>Wire format:
 * <pre>
 * [1 byte]  block compression (0=NONE, 1=ZSTD, 2=GZIP)
 * [4 bytes] uncompressed length (LE, only if compressed)
 * [4 bytes] payload length (LE)
 * [N bytes] payload:
 *   Per value:
 *     [4 bytes] length (LE, -1 = null)
 *     [N bytes] data
 * </pre>
 */
public final class VarlenCodec {

    public static final byte COMPRESSION_NONE = 0;
    public static final byte COMPRESSION_ZSTD = 1;
    public static final byte COMPRESSION_GZIP = 2;

    private VarlenCodec() {}

    public static byte[] encode(byte[][] values, int count, byte compression) {
        // Build raw varlen payload
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        for (int i = 0; i < count; i++) {
            byte[] v = values[i];
            if (v == null) {
                writIntLE(raw, -1);
            } else {
                writIntLE(raw, v.length);
                raw.write(v, 0, v.length);
            }
        }
        byte[] payload = raw.toByteArray();

        // Apply block compression
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(compression);
        if (compression == COMPRESSION_NONE) {
            writIntLE(out, payload.length);
            out.write(payload, 0, payload.length);
        } else if (compression == COMPRESSION_ZSTD) {
            byte[] compressed = Zstd.compress(payload);
            writIntLE(out, payload.length);
            writIntLE(out, compressed.length);
            out.write(compressed, 0, compressed.length);
        } else if (compression == COMPRESSION_GZIP) {
            try {
                byte[] compressed = gzipCompress(payload);
                writIntLE(out, payload.length);
                writIntLE(out, compressed.length);
                out.write(compressed, 0, compressed.length);
            } catch (IOException e) {
                throw new RuntimeException("Gzip compression failed", e);
            }
        }
        return out.toByteArray();
    }

    public static byte[][] decode(byte[] data, int count) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        byte compression = buf.get();
        byte[] payload;
        if (compression == COMPRESSION_NONE) {
            int len = buf.getInt();
            payload = new byte[len];
            buf.get(payload);
        } else if (compression == COMPRESSION_ZSTD) {
            int uncompressedLen = buf.getInt();
            int compressedLen = buf.getInt();
            byte[] compressed = new byte[compressedLen];
            buf.get(compressed);
            payload = Zstd.decompress(compressed, uncompressedLen);
        } else if (compression == COMPRESSION_GZIP) {
            int uncompressedLen = buf.getInt();
            int compressedLen = buf.getInt();
            byte[] compressed = new byte[compressedLen];
            buf.get(compressed);
            try {
                payload = gzipDecompress(compressed, uncompressedLen);
            } catch (IOException e) {
                throw new RuntimeException("Gzip decompression failed", e);
            }
        } else {
            throw new IllegalArgumentException("Unknown compression: " + compression);
        }

        ByteBuffer payloadBuf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        byte[][] result = new byte[count][];
        for (int i = 0; i < count; i++) {
            int len = payloadBuf.getInt();
            if (len < 0) {
                result[i] = null;
            } else {
                result[i] = new byte[len];
                payloadBuf.get(result[i]);
            }
        }
        return result;
    }

    private static void writIntLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(data);
        }
        return baos.toByteArray();
    }

    private static byte[] gzipDecompress(byte[] compressed, int expectedLen) throws IOException {
        byte[] result = new byte[expectedLen];
        try (GZIPInputStream gzip = new GZIPInputStream(
                new java.io.ByteArrayInputStream(compressed))) {
            int offset = 0;
            while (offset < expectedLen) {
                int read = gzip.read(result, offset, expectedLen - offset);
                if (read < 0) break;
                offset += read;
            }
        }
        return result;
    }
}
