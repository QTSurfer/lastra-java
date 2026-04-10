# Reef

[![CI](https://github.com/QTSurfer/reef-java/actions/workflows/ci.yml/badge.svg)](https://github.com/QTSurfer/reef-java/actions/workflows/ci.yml)
[![JitPack](https://jitpack.io/v/QTSurfer/reef-java.svg)](https://jitpack.io/#QTSurfer/reef-java)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Columnar time series file format optimized for numeric data. Ideal for financial tick data, IoT sensors, and infrastructure metrics.

Combines [ALP](https://github.com/QTSurfer/alp-java), Gorilla, and Pongo compression for doubles, delta-varint for timestamps, and ZSTD/gzip for binary data — with per-column codec selection in a single `.reef` file.

## Features

- **Per-column codecs**: ALP/Gorilla/Pongo for numeric data, delta-varint for timestamps, ZSTD/gzip for binary
- **Two sections**: regular time series (series) + sparse timestamped events (events)
- **Column metadata**: optional key-value metadata per column (e.g., indicator parameters, sensor config)
- **Selective column access**: footer offsets enable reading specific columns without decompressing others
- **Little-endian throughout**: JS/TS readers can use `Float64Array` zero-copy on decoded data
- **Zero Hadoop/Parquet dependency**: pure Java + [alp-java](https://github.com/QTSurfer/alp-java) + zstd-jni, Gorilla and Pongo codecs have zero external deps
- Java 11+

## File Format

```
HEADER (24 bytes, LE):
  "REEF" magic | version (1) | flags | seriesRowCount | seriesColCount
  eventsRowCount | eventsColCount

COLUMN DESCRIPTORS (series, then events):
  codec | dataType | flags | name
  optional: metadata (JSON, gzip-compressed)

SERIES DATA:        per column: [4 bytes length] [compressed data]
EVENTS DATA:        per column: [4 bytes length] [compressed data]

FOOTER:             column offsets + "REF!" magic
```

## Codecs

| Codec | ID | DataType | Use case |
|-------|-----|----------|----------|
| `DELTA_VARINT` | 1 | LONG | Timestamps (~1 byte/value for regular intervals) |
| `ALP` | 2 | DOUBLE | Decimal doubles: prices, temperatures, measurements (~3-4 bits/value for 2dp) |
| `GORILLA` | 6 | DOUBLE | XOR compression ([Facebook VLDB 2015](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)). Best for volatile metrics: CPU%, latency, network |
| `PONGO` | 7 | DOUBLE | Decimal-aware erasure + Gorilla XOR. Best for decimal-native data: prices, sensor readings (~18 bits/value on 2dp) |
| `VARLEN` | 3 | BINARY | Short strings, event types, labels |
| `VARLEN_ZSTD` | 4 | BINARY | JSON payloads, bulk binary data |
| `VARLEN_GZIP` | 5 | BINARY | Metadata, small text (browser DecompressionStream compatible) |
| `RAW` | 0 | LONG/DOUBLE | Uncompressed fallback |

## Usage

### Write OHLCV ticker data

```java
try (ReefWriter w = new ReefWriter(outputStream)) {
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("open", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("high", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("low", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("volume", DataType.DOUBLE, Codec.ALP);
    w.writeSeries(rowCount, ts, open, high, low, close, volume);
}
```

### Write IoT sensor data with alerts

```java
try (ReefWriter w = new ReefWriter(outputStream)) {
    // Series: sensor readings with metadata
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("temperature", DataType.DOUBLE, Codec.PONGO,
        Map.of("unit", "celsius", "sensor", "dht22"));
    w.addSeriesColumn("humidity", DataType.DOUBLE, Codec.ALP,
        Map.of("unit", "%", "sensor", "dht22"));
    w.addSeriesColumn("pressure", DataType.DOUBLE, Codec.GORILLA);

    // Events: alerts with their own timestamps
    w.addEventColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addEventColumn("type", DataType.BINARY, Codec.VARLEN);
    w.addEventColumn("data", DataType.BINARY, Codec.VARLEN_ZSTD);

    w.writeSeries(sampleCount, ts, temp, humidity, pressure);
    w.writeEvents(alertCount, alertTs, alertTypes, alertData);
}
```

### Write financial strategy results

```java
try (ReefWriter w = new ReefWriter(outputStream)) {
    w.addSeriesColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addSeriesColumn("close", DataType.DOUBLE, Codec.ALP);
    w.addSeriesColumn("ema1", DataType.DOUBLE, Codec.ALP,
        Map.of("indicator", "ema", "periods", "10"));
    w.addSeriesColumn("rsi1", DataType.DOUBLE, Codec.ALP,
        Map.of("indicator", "rsi", "periods", "14"));

    w.addEventColumn("ts", DataType.LONG, Codec.DELTA_VARINT);
    w.addEventColumn("type", DataType.BINARY, Codec.VARLEN);
    w.addEventColumn("data", DataType.BINARY, Codec.VARLEN_ZSTD);

    w.writeSeries(tickCount, ts, close, ema, rsi);
    w.writeEvents(signalCount, signalTs, signalTypes, signalData);
}
```

### Read (selective columns)

```java
ReefReader r = ReefReader.from(inputStream);

// Read only what you need — other columns are not decompressed
long[] ts = r.readSeriesLong("ts");
double[] close = r.readSeriesDouble("close");

// Column metadata
Map<String, String> meta = r.getSeriesColumn("ema1").metadata();
// {"indicator": "ema", "periods": "10"}

// Events (independent timestamps)
long[] signalTs = r.readEventLong("ts");
byte[][] signalData = r.readEventBinary("data");
```

## Compression Ratios

OHLCV ticker data (1000 rows, 2 decimal places):

| Format | Size | Ratio |
|--------|------|-------|
| Raw | 48 KB | 1x |
| **Reef** | **~3.5 KB** | **~13x** |
| [Apache Parquet](https://parquet.apache.org/) (SNAPPY) | ~8 KB | ~6x |

## Reef vs Apache Parquet

### Codec comparison

| Aspect | [Apache Parquet](https://parquet.apache.org/) | Reef |
|--------|---|---|
| **Timestamps (int64)** | [DELTA_BINARY_PACKED](https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-encoding-delta_binary_packed--5) (~1-2 bytes/value) | DELTA_VARINT (~1 byte/value) |
| **Doubles (prices)** | PLAIN + block compression (SNAPPY/ZSTD) | **ALP** (~3-4 bits/value), **Pongo** (~18 bits/value), **Gorilla** (XOR) |
| **Strings / binary** | DICTIONARY + RLE, DELTA_BYTE_ARRAY | VARLEN, VARLEN_ZSTD, VARLEN_GZIP |
| **Block compression** | SNAPPY, GZIP, ZSTD, LZ4, BROTLI | No (compression integrated per codec) |
| **Per-column codec** | Same codec per file or row group | **Different codec per column** |
| **Optimized for** | General purpose, big data | Financial time series |

### Real-world benchmark (BTC/USDT, 3,591 rows, 11 columns)

| Format | Size | Ratio vs Parquet |
|--------|------|------------------|
| [Apache Parquet](https://parquet.apache.org/) (SNAPPY) | 118 KB | 1x |
| Reef (ALP default) | 82 KB | 1.4x smaller |
| **Reef (mixed codecs via [reef-convert](https://github.com/QTSurfer/qtsurfer-reef-convert) `--best`)** | **73 KB** | **1.6x smaller** |

### Why Reef compresses better for financial data

[Apache Parquet](https://parquet.apache.org/) stores doubles as raw 8 bytes (PLAIN encoding) then applies generic block compression (SNAPPY/ZSTD). Reef applies **semantic compression** per column:

- **ALP**: understands that `65007.28` has 2 decimal places → 3-4 bits/value
- **Pongo**: detects decimal patterns and erases mantissa noise before XOR → ~18 bits/value
- **Gorilla**: XOR between consecutive similar values → good for volatile data

### Where Parquet wins

[Apache Parquet](https://parquet.apache.org/) has a much larger ecosystem (Spark, DuckDB, Arrow, Pandas) and advanced features: predicate pushdown, bloom filters, column statistics, nested types, and modular encryption.

## Dependency (JitPack)

### Maven

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.qtsurfer</groupId>
    <artifactId>reef-java</artifactId>
    <version>0.6.1</version>
</dependency>
```

### Gradle

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.qtsurfer:reef-java:0.6.1'
}
```

## License

Copyright 2026 Wualabs LTD. Apache License 2.0 — see [LICENSE](LICENSE).
