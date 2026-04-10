package com.wualabs.qtsurfer.lastra;

import com.wualabs.qtsurfer.alp.AlpDecompressor;
import com.wualabs.qtsurfer.lastra.codec.DeltaVarintCodec;
import com.wualabs.qtsurfer.lastra.codec.GorillaCodec;
import com.wualabs.qtsurfer.lastra.codec.PongoCodec;
import com.wualabs.qtsurfer.lastra.codec.RawCodec;
import com.wualabs.qtsurfer.lastra.codec.VarlenCodec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Reads Lastra files. Supports selective column reading via footer offsets.
 *
 * <p>Usage:
 * <pre>
 * LastraReader reader = LastraReader.from(inputStream);
 * long[] ts = reader.readSeriesLong("ts");
 * double[] close = reader.readSeriesDouble("close");
 * byte[][] signals = reader.readEventBinary("data");
 * </pre>
 */
public class LastraReader {

    private final ByteBuffer buf;
    private final int flags;
    private final int seriesRowCount;
    private final int eventsRowCount;
    private final List<ColumnDescriptor> seriesColumns;
    private final List<ColumnDescriptor> eventColumns;
    private final int dataOffset;
    private final int[] seriesOffsets;
    private final int[] eventOffsets;

    // Cached column data regions (offset + length in buf)
    private final int[] seriesDataPos;
    private final int[] seriesDataLen;
    private final int[] eventDataPos;
    private final int[] eventDataLen;

    // Per-column CRC32 checksums (empty if file has no checksums)
    private final int[] seriesCrcs;
    private final int[] eventCrcs;
    private final boolean hasChecksums;

    private LastraReader(byte[] data) {
        this.buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Header
        int magic = buf.getInt();
        if (magic != Lastra.MAGIC) {
            throw new IllegalArgumentException(
                    String.format("Not a Lastra file (magic: 0x%08X)", magic));
        }
        int version = buf.getShort() & 0xFFFF;
        if (version > Lastra.VERSION) {
            throw new IllegalArgumentException("Unsupported Lastra version: " + version);
        }
        this.flags = buf.getShort() & 0xFFFF;
        this.seriesRowCount = buf.getInt();
        int seriesColCount = buf.getInt();
        this.eventsRowCount = buf.getInt();
        int eventColCount = buf.getShort() & 0xFFFF;

        // Column descriptors
        this.seriesColumns = readColumnDescriptors(seriesColCount);
        boolean hasEvents = (flags & Lastra.FLAG_HAS_EVENTS) != 0;
        this.eventColumns = hasEvents ? readColumnDescriptors(eventColCount) : Collections.emptyList();

        // Data section starts here
        this.dataOffset = buf.position();

        // Read footer (offsets + optional checksums)
        boolean hasFooter = (flags & Lastra.FLAG_HAS_FOOTER) != 0;
        this.hasChecksums = (flags & Lastra.FLAG_HAS_CHECKSUMS) != 0;

        if (hasFooter) {
            int totalCols = seriesColCount + eventColumns.size();
            int footerInts = totalCols; // offsets
            if (hasChecksums) footerInts += totalCols; // + CRCs
            footerInts += 1; // REF! magic

            int footerStart = data.length - footerInts * 4;
            ByteBuffer footer = ByteBuffer.wrap(data, footerStart, footerInts * 4)
                    .order(ByteOrder.LITTLE_ENDIAN);

            this.seriesOffsets = new int[seriesColCount];
            for (int i = 0; i < seriesColCount; i++) {
                seriesOffsets[i] = footer.getInt();
            }
            this.eventOffsets = new int[eventColumns.size()];
            for (int i = 0; i < eventColumns.size(); i++) {
                eventOffsets[i] = footer.getInt();
            }

            if (hasChecksums) {
                this.seriesCrcs = new int[seriesColCount];
                for (int i = 0; i < seriesColCount; i++) {
                    seriesCrcs[i] = footer.getInt();
                }
                this.eventCrcs = new int[eventColumns.size()];
                for (int i = 0; i < eventColumns.size(); i++) {
                    eventCrcs[i] = footer.getInt();
                }
            } else {
                this.seriesCrcs = new int[0];
                this.eventCrcs = new int[0];
            }

            int footerMagic = footer.getInt();
            if (footerMagic != Lastra.FOOTER_MAGIC) {
                throw new IllegalArgumentException("Invalid Lastra footer");
            }
        } else {
            this.seriesOffsets = new int[0];
            this.eventOffsets = new int[0];
            this.seriesCrcs = new int[0];
            this.eventCrcs = new int[0];
        }

        // Precompute column data positions by scanning length prefixes
        this.seriesDataPos = new int[seriesColCount];
        this.seriesDataLen = new int[seriesColCount];
        int pos = dataOffset;
        for (int i = 0; i < seriesColCount; i++) {
            int len = getIntLE(data, pos);
            seriesDataPos[i] = pos + 4;
            seriesDataLen[i] = len;
            pos += 4 + len;
        }
        this.eventDataPos = new int[eventColumns.size()];
        this.eventDataLen = new int[eventColumns.size()];
        for (int i = 0; i < eventColumns.size(); i++) {
            int len = getIntLE(data, pos);
            eventDataPos[i] = pos + 4;
            eventDataLen[i] = len;
            pos += 4 + len;
        }
    }

    /**
     * Read a Lastra file from an InputStream (loads entirely into memory).
     */
    public static LastraReader from(InputStream in) throws IOException {
        return new LastraReader(in.readAllBytes());
    }

    /**
     * Read a Lastra file from a byte array.
     */
    public static LastraReader from(byte[] data) {
        return new LastraReader(data);
    }

    public int seriesRowCount() { return seriesRowCount; }
    public int eventsRowCount() { return eventsRowCount; }
    public List<ColumnDescriptor> seriesColumns() { return seriesColumns; }
    public List<ColumnDescriptor> eventColumns() { return eventColumns; }

    public ColumnDescriptor getSeriesColumn(String name) {
        return findColumn(seriesColumns, name);
    }

    public ColumnDescriptor getEventColumn(String name) {
        return findColumn(eventColumns, name);
    }

    /** Returns true if this file contains per-column CRC32 checksums. */
    public boolean hasChecksums() { return hasChecksums; }

    // --- Series column readers ---

    public long[] readSeriesLong(String name) {
        int idx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractAndVerify(seriesDataPos[idx], seriesDataLen[idx], seriesCrcs, idx, name);
        return decodeLongColumn(colData, seriesRowCount, seriesColumns.get(idx).codec());
    }

    public double[] readSeriesDouble(String name) {
        int idx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractAndVerify(seriesDataPos[idx], seriesDataLen[idx], seriesCrcs, idx, name);
        return decodeDoubleColumn(colData, seriesRowCount, seriesColumns.get(idx).codec());
    }

    public byte[][] readSeriesBinary(String name) {
        int idx = findColumnIndex(seriesColumns, name);
        byte[] colData = extractAndVerify(seriesDataPos[idx], seriesDataLen[idx], seriesCrcs, idx, name);
        return decodeBinaryColumn(colData, seriesRowCount);
    }

    // --- Event column readers ---

    public long[] readEventLong(String name) {
        int idx = findColumnIndex(eventColumns, name);
        byte[] colData = extractAndVerify(eventDataPos[idx], eventDataLen[idx], eventCrcs, idx, name);
        return decodeLongColumn(colData, eventsRowCount, eventColumns.get(idx).codec());
    }

    public double[] readEventDouble(String name) {
        int idx = findColumnIndex(eventColumns, name);
        byte[] colData = extractAndVerify(eventDataPos[idx], eventDataLen[idx], eventCrcs, idx, name);
        return decodeDoubleColumn(colData, eventsRowCount, eventColumns.get(idx).codec());
    }

    public byte[][] readEventBinary(String name) {
        int idx = findColumnIndex(eventColumns, name);
        byte[] colData = extractAndVerify(eventDataPos[idx], eventDataLen[idx], eventCrcs, idx, name);
        return decodeBinaryColumn(colData, eventsRowCount);
    }

    // --- Decoders ---

    private long[] decodeLongColumn(byte[] data, int count, Lastra.Codec codec) {
        switch (codec) {
            case DELTA_VARINT: return DeltaVarintCodec.decode(data, count);
            case RAW: return RawCodec.decodeLongs(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for LONG: " + codec);
        }
    }

    private double[] decodeDoubleColumn(byte[] data, int count, Lastra.Codec codec) {
        switch (codec) {
            case ALP: return new AlpDecompressor().decompress(data);
            case GORILLA: return GorillaCodec.decode(data, count);
            case PONGO: return PongoCodec.decode(data, count);
            case RAW: return RawCodec.decodeDoubles(data, count);
            default: throw new IllegalArgumentException("Unsupported codec for DOUBLE: " + codec);
        }
    }

    private byte[][] decodeBinaryColumn(byte[] data, int count) {
        return VarlenCodec.decode(data, count);
    }

    // --- Helpers ---

    private byte[] extractAndVerify(int pos, int len, int[] crcs, int idx, String colName) {
        byte[] data = new byte[len];
        System.arraycopy(buf.array(), pos, data, 0, len);
        if (hasChecksums && idx < crcs.length) {
            CRC32 crc = new CRC32();
            crc.update(data);
            int actual = (int) crc.getValue();
            if (actual != crcs[idx]) {
                throw new IllegalStateException(String.format(
                        "CRC32 mismatch on column '%s': expected 0x%08X, got 0x%08X",
                        colName, crcs[idx], actual));
            }
        }
        return data;
    }

    private List<ColumnDescriptor> readColumnDescriptors(int count) {
        List<ColumnDescriptor> cols = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int codecId = buf.get() & 0xFF;
            int typeId = buf.get() & 0xFF;
            int colFlags = buf.get() & 0xFF;
            int nameLen = buf.get() & 0xFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            Map<String, String> metadata = Collections.emptyMap();
            if ((colFlags & 0x02) != 0) {
                int metaLen = buf.getShort() & 0xFFFF;
                byte[] metaBytes = new byte[metaLen];
                buf.get(metaBytes);
                metadata = parseJsonMap(new String(metaBytes, StandardCharsets.UTF_8));
            }
            cols.add(new ColumnDescriptor(name, Lastra.DataType.fromId(typeId),
                    Lastra.Codec.fromId(codecId), metadata));
        }
        return cols;
    }

    private static int findColumnIndex(List<ColumnDescriptor> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) return i;
        }
        throw new IllegalArgumentException("Column not found: " + name);
    }

    private static ColumnDescriptor findColumn(List<ColumnDescriptor> columns, String name) {
        return columns.get(findColumnIndex(columns, name));
    }

    private static int getIntLE(byte[] data, int pos) {
        return (data[pos] & 0xFF)
                | ((data[pos + 1] & 0xFF) << 8)
                | ((data[pos + 2] & 0xFF) << 16)
                | ((data[pos + 3] & 0xFF) << 24);
    }

    private static Map<String, String> parseJsonMap(String json) {
        // Minimal JSON object parser for {"key":"value",...}
        Map<String, String> map = new LinkedHashMap<>();
        json = json.trim();
        if (json.startsWith("{")) json = json.substring(1);
        if (json.endsWith("}")) json = json.substring(0, json.length() - 1);
        if (json.isEmpty()) return map;
        String[] pairs = json.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split(":", 2);
            String key = kv[0].trim().replace("\"", "");
            String value = kv[1].trim().replace("\"", "");
            map.put(key, value);
        }
        return map;
    }
}
