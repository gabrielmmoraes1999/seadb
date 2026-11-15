package io.github.gabrielmmoraes1999.seadb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * {@code EXECUTE PROCEDURE} and {@code {call name(...)}}.
 * {@code setX} binds the input {@code ?}. {@code getX} reads the single row of {@code RETURNS},
 * which stops at the first {@code SUSPEND}.
 */
final class SeaCallableStatement extends SeaPreparedStatement implements CallableStatement {

    private Object[] returns = new Object[0];
    private List<String> names = List.of();
    private boolean wasNull;
    private boolean returned;

    SeaCallableStatement(SeaConnection connection, String sql, int resultSetType, int resultSetConcurrency, int holdability) {
        super(connection, sql, resultSetType, resultSetConcurrency, holdability);
    }

    @Override
    public boolean execute() throws SQLException {
        boolean query = super.execute();
        capture();
        return query;
    }

    private void capture() {
        returned = false;
        returns = new Object[0];
        names = List.of();
        if (generated == null || !generated.isQuery() || generated.rows().isEmpty()) {
            return;
        }
        var row = generated.rows().get(0);
        returns = new Object[row.size()];
        List<String> labels = new ArrayList<>();
        for (int i = 0; i < row.size(); i++) {
            returns[i] = row.get(i);
            labels.add(generated.columns().get(i).name());
        }
        names = labels;
        returned = true;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        checkOpen();
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        checkOpen();
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkOpen();
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        checkOpen();
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        checkOpen();
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        checkOpen();
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkOpen();
        return wasNull;
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        return value == null ? null : String.valueOf(value);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean flag) {
            return flag;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        Number number = number(parameterIndex);
        return number == null ? 0 : number.byteValue();
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        Number number = number(parameterIndex);
        return number == null ? 0 : number.shortValue();
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        Number number = number(parameterIndex);
        return number == null ? 0 : number.intValue();
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        Number number = number(parameterIndex);
        return number == null ? 0 : number.longValue();
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        Number number = number(parameterIndex);
        return number == null ? 0 : number.floatValue();
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        Number number = number(parameterIndex);
        return number == null ? 0 : number.doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        BigDecimal decimal = getBigDecimal(parameterIndex);
        return decimal == null ? null : decimal.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        return value instanceof byte[] bytes ? bytes.clone() : null;
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof Date date) {
            return date;
        }
        if (value instanceof LocalDate date) {
            return Date.valueOf(date);
        }
        return Date.valueOf(LocalDate.parse(String.valueOf(value)));
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof Time time) {
            return time;
        }
        if (value instanceof LocalTime time) {
            return Time.valueOf(time);
        }
        return Time.valueOf(LocalTime.parse(String.valueOf(value)));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp;
        }
        if (value instanceof LocalDateTime time) {
            return Timestamp.valueOf(time);
        }
        if (value instanceof LocalDate date) {
            return Timestamp.valueOf(date.atStartOfDay());
        }
        return Timestamp.valueOf(LocalDateTime.parse(String.valueOf(value)));
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return out(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        return new BigDecimal(String.valueOf(value));
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(parameterIndex);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("REF");
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("BLOB");
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("CLOB");
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("ARRAY");
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return getDate(parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return getTime(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return getTimestamp(parameterIndex);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("URL");
    }

    @Override
    public void setURL(String parameterName, URL value) throws SQLException {
        throw Jdbc.unsupported("URL");
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBoolean(String parameterName, boolean value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setByte(String parameterName, byte value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setShort(String parameterName, short value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setInt(String parameterName, int value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setLong(String parameterName, long value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setFloat(String parameterName, float value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setDouble(String parameterName, double value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setString(String parameterName, String value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBytes(String parameterName, byte[] value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setDate(String parameterName, Date value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setTime(String parameterName, Time value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream value, int length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream value, int length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setObject(String parameterName, Object value, int targetSqlType, int scale) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setObject(String parameterName, Object value, int targetSqlType) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setObject(String parameterName, Object value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setDate(String parameterName, Date value, Calendar cal) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setTime(String parameterName, Time value, Calendar cal) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp value, Calendar cal) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        named(parameterName);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return getString(index(parameterName));
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(index(parameterName));
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return getByte(index(parameterName));
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return getShort(index(parameterName));
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return getInt(index(parameterName));
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return getLong(index(parameterName));
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return getFloat(index(parameterName));
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return getDouble(index(parameterName));
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(index(parameterName));
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return getDate(index(parameterName));
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return getTime(index(parameterName));
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(index(parameterName));
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return getObject(index(parameterName));
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(index(parameterName));
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return getObject(parameterName);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        throw Jdbc.unsupported("REF");
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        throw Jdbc.unsupported("BLOB");
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        throw Jdbc.unsupported("CLOB");
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        throw Jdbc.unsupported("ARRAY");
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(parameterName);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(parameterName);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(parameterName);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        throw Jdbc.unsupported("URL");
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("ROWID");
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw Jdbc.unsupported("ROWID");
    }

    @Override
    public void setRowId(String parameterName, RowId value) throws SQLException {
        throw Jdbc.unsupported("ROWID");
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        named(parameterName);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("NCLOB");
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        throw Jdbc.unsupported("NCLOB");
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw Jdbc.unsupported("SQLXML");
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw Jdbc.unsupported("SQLXML");
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw Jdbc.unsupported("SQLXML");
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return getString(parameterIndex);
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return getString(parameterName);
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return getCharacterStream(parameterName);
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        String text = getString(parameterIndex);
        return text == null ? null : new java.io.StringReader(text);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return getCharacterStream(index(parameterName));
    }

    @Override
    public void setBlob(String parameterName, Blob value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setClob(String parameterName, Clob value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream value, long length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream value, long length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        named(parameterName);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        named(parameterName);
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        Object value = out(parameterIndex);
        if (value == null || type == null) {
            return null;
        }
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        throw new SQLException("cannot convert the return value to " + type.getName());
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return getObject(index(parameterName), type);
    }

    private Object out(int parameterIndex) throws SQLException {
        checkOpen();
        if (!returned) {
            throw new SQLException("procedure did not return values");
        }
        if (parameterIndex < 1 || parameterIndex > returns.length) {
            throw new SQLException("parameter index " + parameterIndex);
        }
        Object value = returns[parameterIndex - 1];
        wasNull = value == null;
        return value;
    }

    private Number number(int parameterIndex) throws SQLException {
        Object value = out(parameterIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number;
        }
        throw new SQLException("return value is not a number");
    }

    private int index(String parameterName) throws SQLException {
        checkOpen();
        if (parameterName == null) {
            throw new SQLException("parameter name is required");
        }
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(parameterName)) {
                return i + 1;
            }
        }
        throw new SQLException("return value not found: " + parameterName);
    }

    private static void named(String parameterName) throws SQLException {
        throw new SQLException("parameter name is not an input: " + parameterName);
    }
}
