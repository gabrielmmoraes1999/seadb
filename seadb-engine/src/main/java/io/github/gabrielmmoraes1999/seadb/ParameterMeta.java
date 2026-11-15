package io.github.gabrielmmoraes1999.seadb;

/**
 * Type of one {@code ?} discovered from the statement text, without running it.
 * {@code precision} and {@code scale} are the column or parameter sizes. Zero precision
 * with {@link DataType#VARCHAR} means the placeholder has no target.
 */
public record ParameterMeta(DataType type, int precision, int scale, boolean nullable) {

    public static ParameterMeta unknown() {
        return new ParameterMeta(DataType.VARCHAR, 0, 0, true);
    }
}
