package io.github.gabrielmmoraes1999.seadb;

import java.util.Iterator;
import java.util.List;

/**
 * Result of the last statement in a script. Queries return rows;
 * other statements return the number of affected records.
 */
public final class SqlResult implements Iterable<Row> {

    private final boolean query;
    private final int affectedRows;
    private final List<ColumnMeta> columns;
    private final List<Row> rows;
    private final List<ColumnMeta> generatedColumns;
    private final List<Row> generatedRows;
    private final long cursor;

    private SqlResult(boolean query, int affectedRows, List<ColumnMeta> columns, List<Row> rows,
                      List<ColumnMeta> generatedColumns, List<Row> generatedRows) {
        this(query, affectedRows, columns, rows, generatedColumns, generatedRows, 0L);
    }

    private SqlResult(boolean query, int affectedRows, List<ColumnMeta> columns, List<Row> rows,
                      List<ColumnMeta> generatedColumns, List<Row> generatedRows, long cursor) {
        this.query = query;
        this.affectedRows = affectedRows;
        this.columns = columns;
        this.rows = rows;
        this.generatedColumns = generatedColumns;
        this.generatedRows = generatedRows;
        this.cursor = cursor;
    }

    public static SqlResult query(List<ColumnMeta> columns, List<Row> rows) {
        return query(columns, rows, List.of(), List.of());
    }

    public static SqlResult query(List<ColumnMeta> columns, List<Row> rows, List<ColumnMeta> generatedColumns, List<Row> generatedRows) {
        return new SqlResult(true, rows.size(), List.copyOf(columns), List.copyOf(rows),
                List.copyOf(generatedColumns), List.copyOf(generatedRows));
    }

    public static SqlResult update(int affectedRows) {
        return update(affectedRows, List.of(), List.of());
    }

    public static SqlResult update(int affectedRows, List<ColumnMeta> generatedColumns, List<Row> generatedRows) {
        return new SqlResult(false, affectedRows, List.of(), List.of(),
                List.copyOf(generatedColumns), List.copyOf(generatedRows));
    }

    public boolean isQuery() {
        return query;
    }

    public int affectedRows() {
        return affectedRows;
    }

    public List<ColumnMeta> columns() {
        return columns;
    }

    public List<Row> rows() {
        return rows;
    }

    public List<ColumnMeta> generatedColumns() {
        return generatedColumns;
    }

    public List<Row> generatedRows() {
        return generatedRows;
    }

    public int size() {
        return rows.size();
    }

    /** Non-zero while further blocks of this query can be read with {@code FETCH}. */
    public long cursor() {
        return cursor;
    }

    public SqlResult withCursor(long cursor) {
        return new SqlResult(query, affectedRows, columns, rows, generatedColumns, generatedRows, cursor);
    }

    @Override
    public Iterator<Row> iterator() {
        return rows.iterator();
    }

    @Override
    public String toString() {
        if (!query) {
            return "affected=" + affectedRows;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                builder.append(" | ");
            }
            builder.append(columns.get(i).name());
        }
        builder.append(System.lineSeparator());
        for (Row row : rows) {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    builder.append(" | ");
                }
                builder.append(Values.display(row.get(i)));
            }
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }
}
