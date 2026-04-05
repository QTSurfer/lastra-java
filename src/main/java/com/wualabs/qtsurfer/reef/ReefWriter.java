package com.wualabs.qtsurfer.reef;

import com.wualabs.qtsurfer.alp.AlpCompressor;
import com.wualabs.qtsurfer.reef.codec.DeltaVarintCodec;
import com.wualabs.qtsurfer.reef.codec.RawCodec;
import com.wualabs.qtsurfer.reef.codec.VarlenCodec;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Writes Reef files: header + series columns + optional events columns + footer.
 *
 * <p>Usage:
 * <pre>
 * try (ReefWriter w = new ReefWriter(outputStream)) {
 *     w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
 *     w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
 *     w.addEventColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
 *     w.addEventColumn("type", DataType.BINARY, Codec.VARLEN);
 *
 *     w.beginSeries(rowCount);
 *     // write series data...
 *
 *     w.beginEvents(eventCount);
 *     // write event data...
 * }
 * </pre>
 */
public class ReefWriter implements Closeable {

    private final OutputStream out;
    private final List<ColumnDescriptor> seriesColumns = new ArrayList<>();
    private final List<ColumnDescriptor> eventColumns = new ArrayList<>();

    // Buffered column data (accumulated during write phase)
    private final List<long[]> seriesLongBuffers = new ArrayList<>();
    private final List<double[]> seriesDoubleBuffers = new ArrayList<>();
    private final List<byte[][]> seriesBinaryBuffers = new ArrayList<>();
    private final List<long[]> eventLongBuffers = new ArrayList<>();
    private final List<double[]> eventDoubleBuffers = new ArrayList<>();
    private final List<byte[][]> eventBinaryBuffers = new ArrayList<>();

    private int seriesRowCount;
    private int eventsRowCount;

    public ReefWriter(OutputStream out) {
        this.out = out;
    }

    public ReefWriter addSeriesColumn(String name, Reef.DataType dataType, Reef.Codec codec) {
        return addSeriesColumn(name, dataType, codec, null);
    }

    public ReefWriter addSeriesColumn(String name, Reef.DataType dataType, Reef.Codec codec,
                                      Map<String, String> metadata) {
        seriesColumns.add(new ColumnDescriptor(name, dataType, codec, metadata));
        return this;
    }

    public ReefWriter addEventColumn(String name, Reef.DataType dataType, Reef.Codec codec) {
        return addEventColumn(name, dataType, codec, null);
    }

    public ReefWriter addEventColumn(String name, Reef.DataType dataType, Reef.Codec codec,
                                     Map<String, String> metadata) {
        eventColumns.add(new ColumnDescriptor(name, dataType, codec, metadata));
        return this;
    }

    /**
     * Set series data from arrays. Each array corresponds to a series column in order.
     * Use {@code long[]} for LONG columns, {@code double[]} for DOUBLE, {@code byte[][]} for BINARY.
     */
    public ReefWriter writeSeries(int rowCount, Object... columnData) {
        this.seriesRowCount = rowCount;
        for (int i = 0; i < seriesColumns.size(); i++) {
            ColumnDescriptor col = seriesColumns.get(i);
            switch (col.dataType()) {
                case LONG:
                    seriesLongBuffers.add((long[]) columnData[i]);
                    seriesDoubleBuffers.add(null);
                    seriesBinaryBuffers.add(null);
                    break;
                case DOUBLE:
                    seriesLongBuffers.add(null);
                    seriesDoubleBuffers.add((double[]) columnData[i]);
                    seriesBinaryBuffers.add(null);
                    break;
                case BINARY:
                    seriesLongBuffers.add(null);
                    seriesDoubleBuffers.add(null);
                    seriesBinaryBuffers.add((byte[][]) columnData[i]);
                    break;
            }
        }
        return this;
    }

    /**
     * Set events data from arrays. Same convention as {@link #writeSeries}.
     */
    public ReefWriter writeEvents(int rowCount, Object... columnData) {
        this.eventsRowCount = rowCount;
        for (int i = 0; i < eventColumns.size(); i++) {
            ColumnDescriptor col = eventColumns.get(i);
            switch (col.dataType()) {
                case LONG:
                    eventLongBuffers.add((long[]) columnData[i]);
                    eventDoubleBuffers.add(null);
                    eventBinaryBuffers.add(null);
                    break;
                case DOUBLE:
                    eventLongBuffers.add(null);
                    eventDoubleBuffers.add((double[]) columnData[i]);
                    eventBinaryBuffers.add(null);
                    break;
                case BINARY:
                    eventLongBuffers.add(null);
                    eventDoubleBuffers.add(null);
                    eventBinaryBuffers.add((byte[][]) columnData[i]);
                    break;
            }
        }
        return this;
    }

    @Override
    public void close() throws IOException {
        boolean hasEvents = !eventColumns.isEmpty() && eventsRowCount > 0;
        int flags = Reef.FLAG_HAS_FOOTER;
        if (hasEvents) flags |= Reef.FLAG_HAS_EVENTS;

        ByteArrayOutputStream body = new ByteArrayOutputStream();

        // === HEADER ===
        ByteBuffer header = ByteBuffer.allocate(22).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(Reef.MAGIC);
        header.putShort((short) Reef.VERSION);
        header.putShort((short) flags);
        header.putInt(seriesRowCount);
        header.putInt(seriesColumns.size());
        header.putInt(eventsRowCount);
        header.putShort((short) eventColumns.size());
        body.write(header.array());

        // === COLUMN DESCRIPTORS ===
        writeColumnDescriptors(body, seriesColumns);
        if (hasEvents) {
            writeColumnDescriptors(body, eventColumns);
        }

        // === SERIES DATA ===
        List<byte[]> seriesCompressed = compressColumns(seriesColumns,
                seriesLongBuffers, seriesDoubleBuffers, seriesBinaryBuffers, seriesRowCount);
        List<Integer> seriesOffsets = new ArrayList<>();
        int dataStart = body.size();
        for (byte[] colData : seriesCompressed) {
            seriesOffsets.add(body.size() - dataStart);
            writeIntLE(body, colData.length);
            body.write(colData);
        }

        // === EVENTS DATA ===
        List<Integer> eventOffsets = new ArrayList<>();
        if (hasEvents) {
            List<byte[]> eventsCompressed = compressColumns(eventColumns,
                    eventLongBuffers, eventDoubleBuffers, eventBinaryBuffers, eventsRowCount);
            for (byte[] colData : eventsCompressed) {
                eventOffsets.add(body.size() - dataStart);
                writeIntLE(body, colData.length);
                body.write(colData);
            }
        }

        // === FOOTER ===
        for (int offset : seriesOffsets) writeIntLE(body, offset);
        for (int offset : eventOffsets) writeIntLE(body, offset);
        writeIntLE(body, Reef.FOOTER_MAGIC);

        out.write(body.toByteArray());
        out.flush();
    }

    private void writeColumnDescriptors(ByteArrayOutputStream out, List<ColumnDescriptor> columns)
            throws IOException {
        for (ColumnDescriptor col : columns) {
            out.write(col.codec().id);
            out.write(col.dataType().id);
            int colFlags = 0;
            if (col.hasMetadata()) colFlags |= 0x02;
            out.write(colFlags);
            byte[] nameBytes = col.name().getBytes(StandardCharsets.UTF_8);
            out.write(nameBytes.length);
            out.write(nameBytes);
            if (col.hasMetadata()) {
                byte[] metaBytes = mapToJson(col.metadata()).getBytes(StandardCharsets.UTF_8);
                writeShortLE(out, metaBytes.length);
                out.write(metaBytes);
            }
        }
    }

    private List<byte[]> compressColumns(List<ColumnDescriptor> columns,
                                          List<long[]> longBufs, List<double[]> doubleBufs,
                                          List<byte[][]> binaryBufs, int rowCount) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor col = columns.get(i);
            switch (col.dataType()) {
                case LONG:
                    result.add(compressLongColumn(longBufs.get(i), rowCount, col.codec()));
                    break;
                case DOUBLE:
                    result.add(compressDoubleColumn(doubleBufs.get(i), rowCount, col.codec()));
                    break;
                case BINARY:
                    result.add(compressBinaryColumn(binaryBufs.get(i), rowCount, col.codec()));
                    break;
            }
        }
        return result;
    }

    private byte[] compressLongColumn(long[] data, int count, Reef.Codec codec) {
        switch (codec) {
            case DELTA_VARINT: return DeltaVarintCodec.encode(data, count);
            case RAW: return RawCodec.encodeLongs(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for LONG: " + codec);
        }
    }

    private byte[] compressDoubleColumn(double[] data, int count, Reef.Codec codec) {
        switch (codec) {
            case ALP: return new AlpCompressor().compress(data, count);
            case RAW: return RawCodec.encodeDoubles(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for DOUBLE: " + codec);
        }
    }

    private byte[] compressBinaryColumn(byte[][] data, int count, Reef.Codec codec) {
        switch (codec) {
            case VARLEN: return VarlenCodec.encode(data, count, VarlenCodec.COMPRESSION_NONE);
            case VARLEN_ZSTD: return VarlenCodec.encode(data, count, VarlenCodec.COMPRESSION_ZSTD);
            case VARLEN_GZIP: return VarlenCodec.encode(data, count, VarlenCodec.COMPRESSION_GZIP);
            default: throw new IllegalArgumentException("Unsupported codec for BINARY: " + codec);
        }
    }

    private static void writeIntLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static void writeShortLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
    }

    private static String mapToJson(Map<String, String> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(e.getKey()).append("\":\"").append(e.getValue()).append("\"");
            first = false;
        }
        return sb.append("}").toString();
    }
}
