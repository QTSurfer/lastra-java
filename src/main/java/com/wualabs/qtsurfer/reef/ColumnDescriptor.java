package com.wualabs.qtsurfer.reef;

import java.util.Collections;
import java.util.Map;

/**
 * Describes a column in a Reef file: name, data type, codec, and optional metadata.
 */
public final class ColumnDescriptor {

    private final String name;
    private final Reef.DataType dataType;
    private final Reef.Codec codec;
    private final Map<String, String> metadata;

    public ColumnDescriptor(String name, Reef.DataType dataType, Reef.Codec codec) {
        this(name, dataType, codec, Collections.emptyMap());
    }

    public ColumnDescriptor(String name, Reef.DataType dataType, Reef.Codec codec,
                            Map<String, String> metadata) {
        this.name = name;
        this.dataType = dataType;
        this.codec = codec;
        this.metadata = metadata == null ? Collections.emptyMap() : metadata;
    }

    public String name() { return name; }
    public Reef.DataType dataType() { return dataType; }
    public Reef.Codec codec() { return codec; }
    public Map<String, String> metadata() { return metadata; }
    public boolean hasMetadata() { return !metadata.isEmpty(); }
}
