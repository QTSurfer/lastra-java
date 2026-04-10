package com.wualabs.qtsurfer.lastra;

import java.util.Collections;
import java.util.Map;

/**
 * Describes a column in a Lastra file: name, data type, codec, and optional metadata.
 */
public final class ColumnDescriptor {

    private final String name;
    private final Lastra.DataType dataType;
    private final Lastra.Codec codec;
    private final Map<String, String> metadata;

    public ColumnDescriptor(String name, Lastra.DataType dataType, Lastra.Codec codec) {
        this(name, dataType, codec, Collections.emptyMap());
    }

    public ColumnDescriptor(String name, Lastra.DataType dataType, Lastra.Codec codec,
                            Map<String, String> metadata) {
        this.name = name;
        this.dataType = dataType;
        this.codec = codec;
        this.metadata = metadata == null ? Collections.emptyMap() : metadata;
    }

    public String name() { return name; }
    public Lastra.DataType dataType() { return dataType; }
    public Lastra.Codec codec() { return codec; }
    public Map<String, String> metadata() { return metadata; }
    public boolean hasMetadata() { return !metadata.isEmpty(); }
}
