package io.github.gabrielmmoraes1999.seadb;

/**
 * SQL, integrity, or table-file read error.
 */
public class SeaException extends RuntimeException {

    private final String sqlState;

    public SeaException(String message) {
        this(message, null, null);
    }

    public SeaException(String message, Throwable cause) {
        this(message, null, cause);
    }

    public SeaException(String message, String sqlState) {
        this(message, sqlState, null);
    }

    private SeaException(String message, String sqlState, Throwable cause) {
        super(message, cause);
        this.sqlState = sqlState == null || sqlState.isBlank() ? "42000" : sqlState;
    }

    public String sqlState() {
        return sqlState;
    }
}
