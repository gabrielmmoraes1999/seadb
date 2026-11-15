package io.github.gabrielmmoraes1999.seadb;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

final class LiveSlot {
    final long rowId;
    final long offset;
    final long size;

    LiveSlot(long rowId, long offset, long size) {
        this.rowId = rowId;
        this.offset = offset;
        this.size = size;
    }
}

final class MetaImage {
    final Schema schema;
    final long nextRowId;
    final long fileSize;
    final List<LiveSlot> rows;
    final List<Extent> free;
    final long[] indexRoots;

    MetaImage(Schema schema, long nextRowId, long fileSize, List<LiveSlot> rows, List<Extent> free, long[] indexRoots) {
        this.schema = schema;
        this.nextRowId = nextRowId;
        this.fileSize = fileSize;
        this.rows = rows;
        this.free = free;
        this.indexRoots = indexRoots;
    }
}

final class MetaCodec {

    private static final int FLAG_NOT_NULL = 1;
    private static final int FLAG_AUTO = 2;

    private MetaCodec() {
    }

    static byte[] encode(Schema schema, long nextRowId, long fileSize, List<LiveSlot> rows, List<Extent> free, long[] indexRoots) {
        return encode(schema, nextRowId, fileSize, rows, free, indexRoots, 0, Format.VERSION);
    }

    static byte[] encode(Schema schema, long nextRowId, long fileSize, List<LiveSlot> rows, List<Extent> free, long[] indexRoots, int minLength) {
        return encode(schema, nextRowId, fileSize, rows, free, indexRoots, minLength, Format.VERSION);
    }

    static byte[] encode(Schema schema, long nextRowId, long fileSize, List<LiveSlot> rows, List<Extent> free, long[] indexRoots,
                         int minLength, int version) {
        if (schema.needsIndexExtensions()) {
            version = Format.META_VERSION;
        } else if (version == Format.MIN_VERSION && schema.needsV2()) {
            version = Format.VERSION;
        }
        ByteWriter writer = new ByteWriter();
        writer.u32(Format.META_MAGIC);
        writer.u32(version);
        writer.u64(nextRowId);
        writer.u64(fileSize);
        writeSchema(writer, schema, version);
        long[] roots = indexRoots == null ? new long[0] : indexRoots;
        writer.u16(roots.length);
        for (long root : roots) {
            writer.u64(root);
        }
        writer.u32(rows.size());
        for (LiveSlot row : rows) {
            writer.u64(row.rowId);
            writer.u64(row.offset);
            writer.u64(row.size);
        }
        writer.u32(free.size());
        for (Extent extent : free) {
            writer.u64(extent.offset);
            writer.u64(extent.length);
        }
        int pad = padding(writer.size() + 2 + 4);
        int total = writer.size() + 2 + pad + 4;
        if (minLength > total) {
            int extra = minLength - total;
            if (pad + extra > 65535) {
                extra = 65535 - pad;
            }
            pad += extra;
        }
        writer.u16(pad);
        writer.zeros(pad);
        byte[] body = writer.toByteArray();
        int crc = Format.crc(body, 0, body.length);
        ByteWriter full = new ByteWriter();
        full.raw(body);
        full.u32(crc);
        return full.toByteArray();
    }

    static MetaImage decode(byte[] data) {
        if (data.length < 8 || (data.length % 8) != 0) {
            throw new SeaException("table metadata is corrupt");
        }
        int stored = ByteBuffer.wrap(data, data.length - 4, 4).order(Format.ORDER).getInt();
        int actual = Format.crc(data, 0, data.length - 4);
        if (stored != actual) {
            throw new SeaException("table metadata failed verification");
        }
        ByteReader reader = new ByteReader(data, data.length - 4);
        if (reader.u32() != Format.META_MAGIC) {
            throw new SeaException("table metadata is corrupt");
        }
        int version = reader.u32();
        if (!Format.knownMeta(version)) {
            throw new SeaException("unsupported metadata version: " + version);
        }
        long nextRowId = reader.u64();
        long fileSize = reader.u64();
        Schema schema = readSchema(reader, version);
        int rootCount = reader.u16();
        if (rootCount < 0 || rootCount > Format.MAX_COLUMNS) {
            throw new SeaException("table metadata is corrupt");
        }
        long[] indexRoots = new long[rootCount];
        for (int i = 0; i < rootCount; i++) {
            indexRoots[i] = reader.u64();
        }
        int liveCount = reader.u32();
        if (liveCount < 0) {
            throw new SeaException("table metadata is corrupt");
        }
        List<LiveSlot> rows = new ArrayList<>(liveCount);
        for (int i = 0; i < liveCount; i++) {
            rows.add(new LiveSlot(reader.u64(), reader.u64(), reader.u64()));
        }
        int freeCount = reader.u32();
        if (freeCount < 0) {
            throw new SeaException("table metadata is corrupt");
        }
        List<Extent> free = new ArrayList<>(freeCount);
        for (int i = 0; i < freeCount; i++) {
            free.add(new Extent(reader.u64(), reader.u64()));
        }
        int pad = reader.u16();
        reader.skip(pad);
        if (reader.remaining() != 0) {
            throw new SeaException("table metadata is corrupt");
        }
        return new MetaImage(schema, nextRowId, fileSize, rows, free, indexRoots);
    }

    private static int padding(int unpadded) {
        return (8 - (unpadded % 8)) % 8;
    }

    private static void writeSchema(ByteWriter writer, Schema schema, int version) {
        writer.utf(schema.name);
        writer.u16(schema.columns.size());
        for (Column column : schema.columns) {
            writer.utf(column.name);
            writer.u8(column.type.ordinal());
            writer.u32(column.length);
            writer.u8(column.scale);
            int flags = (column.notNull ? FLAG_NOT_NULL : 0) | (column.autoIncrement ? FLAG_AUTO : 0);
            writer.u8(flags);
            writer.u64(column.nextIdentity);
            writeDefault(writer, column);
            if (version >= Format.VERSION) {
                writer.u8(column.collation);
                writer.utf(column.domain == null ? "" : column.domain);
                writer.utf(column.generated == null ? "" : column.generated);
                writer.utf(column.defaultExpr == null ? "" : column.defaultExpr);
            }
        }
        writer.u16(schema.primaryKey.length);
        for (int index : schema.primaryKey) {
            writer.u16(index);
        }
        writer.u16(schema.uniques.size());
        for (UniqueKey key : schema.uniques) {
            writer.utf(key.name);
            writer.u16(key.columns.length);
            for (int index : key.columns) {
                writer.u16(index);
            }
        }
        writer.u16(schema.indexes.size());
        for (IndexDef index : schema.indexes) {
            writer.utf(index.name);
            writer.u8(index.unique ? 1 : 0);
            writer.u16(index.columns.length);
            for (int column : index.columns) {
                writer.u16(column);
            }
            writer.u8(index.descending ? 1 : 0);
            if (version >= Format.META_VERSION) {
                writer.utf(index.predicate == null ? "" : index.predicate);
                String[] expressions = index.expressions == null ? new String[0] : index.expressions;
                writer.u16(expressions.length);
                for (String expression : expressions) {
                    writer.utf(expression == null ? "" : expression);
                }
                writer.u16(index.include.length);
                for (int column : index.include) {
                    writer.u16(column);
                }
            }
        }
        writer.utf(schema.primaryConstraint == null ? "" : schema.primaryConstraint);
        writer.utf(schema.primaryIndex == null ? "" : schema.primaryIndex);
        writer.u8(schema.primaryDescending ? 1 : 0);
        writer.u16(schema.foreignKeys.size());
        for (ForeignKey key : schema.foreignKeys) {
            writer.utf(key.name);
            writer.u16(key.columns.length);
            for (int column : key.columns) {
                writer.u16(column);
            }
            writer.utf(key.referencedSchema);
            writer.utf(key.referencedTable);
            writer.u16(key.referencedColumns.length);
            for (String column : key.referencedColumns) {
                writer.utf(column);
            }
            writer.utf(key.indexName);
            writer.u8(key.descending ? 1 : 0);
            writer.u8(key.onUpdate.ordinal());
            writer.u8(key.onDelete.ordinal());
        }
        if (version >= Format.VERSION) {
            writer.u16(schema.checks.size());
            for (CheckConstraint check : schema.checks) {
                writer.utf(check.name);
                writer.utf(check.column == null ? "" : check.column);
                writer.utf(check.expression);
            }
        }
    }

    private static Schema readSchema(ByteReader reader, int version) {
        String name = SystemTables.currentName(reader.utf());
        int count = reader.u16();
        if (count <= 0 || count > Format.MAX_COLUMNS) {
            throw new SeaException("table schema is corrupt");
        }
        List<Column> columns = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String columnName = reader.utf();
            if (SystemTables.isSystem(name)) {
                columnName = SystemTables.currentName(columnName);
            }
            int ordinal = reader.u8();
            if (ordinal < 0 || ordinal >= DataType.values().length) {
                throw new SeaException("unknown column type in " + columnName);
            }
            DataType type = DataType.values()[ordinal];
            int length = reader.u32();
            int scale = reader.u8();
            int flags = reader.u8();
            long nextIdentity = reader.u64();
            DefaultValue def = readDefault(reader, type);
            int collation = 0;
            String domain = null;
            String generated = null;
            String defaultExpr = null;
            if (version >= Format.VERSION) {
                collation = reader.u8();
                domain = reader.utf();
                generated = reader.utf();
                defaultExpr = reader.utf();
            }
            columns.add(new Column(columnName, type, length, scale,
                    (flags & FLAG_NOT_NULL) != 0,
                    (flags & FLAG_AUTO) != 0,
                    nextIdentity,
                    def.hasDefault,
                    def.value,
                    collation,
                    domain,
                    generated,
                    defaultExpr));
        }
        int pkCount = reader.u16();
        int[] primary = new int[pkCount];
        for (int i = 0; i < pkCount; i++) {
            primary[i] = reader.u16();
        }
        int uniqueCount = reader.u16();
        List<UniqueKey> uniques = new ArrayList<>();
        for (int i = 0; i < uniqueCount; i++) {
            String uniqueName = reader.utf();
            int size = reader.u16();
            int[] group = new int[size];
            for (int j = 0; j < size; j++) {
                group[j] = reader.u16();
            }
            uniques.add(new UniqueKey(uniqueName, group));
        }
        int indexCount = reader.u16();
        if (indexCount < 0 || indexCount > Format.MAX_COLUMNS) {
            throw new SeaException("table schema is corrupt");
        }
        List<IndexDef> indexes = new ArrayList<>();
        for (int i = 0; i < indexCount; i++) {
            String indexName = reader.utf();
            boolean unique = reader.u8() != 0;
            int size = reader.u16();
            if (size <= 0 || size > columns.size()) {
                throw new SeaException("table schema is corrupt");
            }
            int[] group = new int[size];
            for (int j = 0; j < size; j++) {
                int column = reader.u16();
                if (column < 0 || column >= columns.size()) {
                    throw new SeaException("table schema is corrupt");
                }
                group[j] = column;
            }
            boolean descending = reader.u8() != 0;
            String predicate = null;
            String[] expressions = null;
            int[] include = new int[0];
            if (version >= Format.META_VERSION) {
                String written = reader.utf();
                predicate = written.isEmpty() ? null : written;
                int expressionCount = reader.u16();
                expressions = new String[expressionCount];
                for (int j = 0; j < expressionCount; j++) {
                    String expression = reader.utf();
                    expressions[j] = expression.isEmpty() ? null : expression;
                }
                int includeCount = reader.u16();
                include = new int[includeCount];
                for (int j = 0; j < includeCount; j++) {
                    int column = reader.u16();
                    if (column < 0 || column >= columns.size()) {
                        throw new SeaException("table schema is corrupt");
                    }
                    include[j] = column;
                }
            }
            indexes.add(new IndexDef(indexName, group, unique, descending, predicate, expressions, include));
        }
        String primaryConstraint = reader.utf();
        String primaryIndex = reader.utf();
        boolean primaryDescending = reader.u8() != 0;
        int foreignCount = reader.u16();
        if (foreignCount < 0 || foreignCount > Format.MAX_COLUMNS) {
            throw new SeaException("table schema is corrupt");
        }
        List<ForeignKey> foreignKeys = new ArrayList<>();
        for (int i = 0; i < foreignCount; i++) {
            String foreignName = reader.utf();
            int size = reader.u16();
            if (size <= 0 || size > columns.size()) {
                throw new SeaException("table schema is corrupt");
            }
            int[] local = new int[size];
            for (int j = 0; j < size; j++) {
                int column = reader.u16();
                if (column < 0 || column >= columns.size()) {
                    throw new SeaException("table schema is corrupt");
                }
                local[j] = column;
            }
            String referencedSchema = reader.utf();
            String referencedTable = reader.utf();
            int referencedCount = reader.u16();
            if (referencedCount <= 0 || referencedCount > Format.MAX_COLUMNS) {
                throw new SeaException("table schema is corrupt");
            }
            String[] referenced = new String[referencedCount];
            for (int j = 0; j < referencedCount; j++) {
                referenced[j] = reader.utf();
            }
            String indexName = reader.utf();
            boolean descending = reader.u8() != 0;
            ReferentialAction onUpdate = ReferentialAction.stored(reader.u8());
            ReferentialAction onDelete = ReferentialAction.stored(reader.u8());
            foreignKeys.add(new ForeignKey(foreignName, local, referencedSchema, referencedTable,
                    referenced, indexName, descending, onUpdate, onDelete));
        }
        List<CheckConstraint> checks = new ArrayList<>();
        if (version >= Format.VERSION) {
            int checkCount = reader.u16();
            for (int i = 0; i < checkCount; i++) {
                checks.add(new CheckConstraint(reader.utf(), reader.utf(), reader.utf()));
            }
        }
        return new Schema(name, columns, primary, uniques, indexes,
                primaryConstraint, primaryIndex, primaryDescending, foreignKeys, checks);
    }

    private static void writeDefault(ByteWriter writer, Column column) {
        writer.u8(column.hasDefault ? 1 : 0);
        if (!column.hasDefault) {
            return;
        }
        if (column.defaultValue == null) {
            writer.u8(0);
            return;
        }
        writer.u8(1);
        switch (column.type) {
            case INTEGER -> writer.u32(((Number) column.defaultValue).intValue());
            case BIGINT -> writer.u64(((Number) column.defaultValue).longValue());
            case SMALLINT -> writer.u16(((Number) column.defaultValue).shortValue());
            case FLOAT -> writer.f32(((Number) column.defaultValue).floatValue());
            case DOUBLE -> writer.f64((Double) column.defaultValue);
            case BOOLEAN -> writer.u8((Boolean) column.defaultValue ? 1 : 0);
            case DATE -> writer.u64(((LocalDate) column.defaultValue).toEpochDay());
            case TIME -> writer.u64(((LocalTime) column.defaultValue).toNanoOfDay());
            case TIMESTAMP -> writer.u64(((LocalDateTime) column.defaultValue).toInstant(ZoneOffset.UTC).toEpochMilli());
            case DECIMAL -> writer.utf(((BigDecimal) column.defaultValue).toPlainString());
            case CHAR, VARCHAR, JSON -> writer.utf((String) column.defaultValue);
            case UUID -> writer.utf(((java.util.UUID) column.defaultValue).toString());
            case INTERVAL -> writer.u64(((SqlInterval) column.defaultValue).millis);
            case TIMESTAMPTZ -> {
                java.time.OffsetDateTime stamp = (java.time.OffsetDateTime) column.defaultValue;
                writer.u64(stamp.toInstant().toEpochMilli());
                writer.u16(stamp.getOffset().getTotalSeconds() / 60);
            }
            case BLOB -> {
                byte[] bytes = (byte[]) column.defaultValue;
                writer.u32(bytes.length);
                writer.raw(bytes);
            }
        }
    }

    private static DefaultValue readDefault(ByteReader reader, DataType type) {
        if (reader.u8() == 0) {
            return new DefaultValue(false, null);
        }
        if (reader.u8() == 0) {
            return new DefaultValue(true, null);
        }
        Object value = switch (type) {
            case INTEGER -> reader.u32();
            case BIGINT -> reader.u64();
            case SMALLINT -> (short) reader.u16();
            case FLOAT -> reader.f32();
            case DOUBLE -> reader.f64();
            case BOOLEAN -> reader.u8() == 1;
            case DATE -> LocalDate.ofEpochDay(reader.u64());
            case TIME -> LocalTime.ofNanoOfDay(reader.u64());
            case TIMESTAMP -> LocalDateTime.ofInstant(Instant.ofEpochMilli(reader.u64()), ZoneOffset.UTC);
            case DECIMAL -> new BigDecimal(reader.utf());
            case CHAR, VARCHAR, JSON -> reader.utf();
            case UUID -> java.util.UUID.fromString(reader.utf());
            case INTERVAL -> new SqlInterval(reader.u64());
            case TIMESTAMPTZ -> {
                long epoch = reader.u64();
                int minutes = reader.u16();
                if (minutes >= 32768) {
                    minutes -= 65536;
                }
                yield java.time.OffsetDateTime.ofInstant(Instant.ofEpochMilli(epoch),
                        java.time.ZoneOffset.ofTotalSeconds(minutes * 60));
            }
            case BLOB -> reader.bytes(reader.u32());
        };
        return new DefaultValue(true, value);
    }

    private record DefaultValue(boolean hasDefault, Object value) {
    }

    private static final class ByteWriter {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        int size() {
            return out.size();
        }

        void u8(int value) {
            out.write(value & 0xFF);
        }

        void u16(int value) {
            out.write((value >>> 8) & 0xFF);
            out.write(value & 0xFF);
        }

        void u32(int value) {
            out.write((value >>> 24) & 0xFF);
            out.write((value >>> 16) & 0xFF);
            out.write((value >>> 8) & 0xFF);
            out.write(value & 0xFF);
        }

        void u64(long value) {
            for (int shift = 56; shift >= 0; shift -= 8) {
                out.write((int) (value >>> shift) & 0xFF);
            }
        }

        void f32(float value) {
            u32(Float.floatToIntBits(value));
        }

        void f64(double value) {
            u64(Double.doubleToLongBits(value));
        }

        void utf(String text) {
            byte[] raw = text.getBytes(StandardCharsets.UTF_8);
            if (raw.length > 65535) {
                throw new SeaException("text is too long for the schema");
            }
            u16(raw.length);
            raw(raw);
        }

        void raw(byte[] data) {
            out.write(data, 0, data.length);
        }

        void zeros(int count) {
            for (int i = 0; i < count; i++) {
                out.write(0);
            }
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }
    }

    private static final class ByteReader {
        private final byte[] data;
        private final int end;
        private int position;

        ByteReader(byte[] data, int end) {
            this.data = data;
            this.end = end;
        }

        int remaining() {
            return end - position;
        }

        int u8() {
            need(1);
            return data[position++] & 0xFF;
        }

        int u16() {
            need(2);
            int value = ((data[position] & 0xFF) << 8) | (data[position + 1] & 0xFF);
            position += 2;
            return value;
        }

        int u32() {
            need(4);
            int value = ByteBuffer.wrap(data, position, 4).order(Format.ORDER).getInt();
            position += 4;
            return value;
        }

        long u64() {
            need(8);
            long value = ByteBuffer.wrap(data, position, 8).order(Format.ORDER).getLong();
            position += 8;
            return value;
        }

        float f32() {
            return Float.intBitsToFloat(u32());
        }

        double f64() {
            return Double.longBitsToDouble(u64());
        }

        String utf() {
            int length = u16();
            return new String(bytes(length), StandardCharsets.UTF_8);
        }

        byte[] bytes(int length) {
            if (length < 0) {
                throw new SeaException("table metadata is corrupt");
            }
            need(length);
            byte[] copy = new byte[length];
            System.arraycopy(data, position, copy, 0, length);
            position += length;
            return copy;
        }

        void skip(int length) {
            need(length);
            position += length;
        }

        private void need(int length) {
            if (length < 0 || position + length > end) {
                throw new SeaException("table metadata is corrupt");
            }
        }
    }
}
