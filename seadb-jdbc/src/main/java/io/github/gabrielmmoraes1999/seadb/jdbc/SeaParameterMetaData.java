package io.github.gabrielmmoraes1999.seadb.jdbc;

import io.github.gabrielmmoraes1999.seadb.ParameterMeta;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

final class SeaParameterMetaData implements ParameterMetaData {

    private final List<ParameterMeta> parameters;

    SeaParameterMetaData(List<ParameterMeta> parameters) {
        this.parameters = parameters;
    }

    @Override
    public int getParameterCount() {
        return parameters.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return at(param).nullable() ? parameterNullable : parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return switch (at(param).type()) {
            case SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL -> true;
            default -> false;
        };
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        ParameterMeta meta = at(param);
        return Jdbc.precision(meta.type(), meta.precision());
    }

    @Override
    public int getScale(int param) throws SQLException {
        return at(param).scale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return Jdbc.sqlType(at(param).type());
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return at(param).type().name();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return Jdbc.className(at(param).type());
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        at(param);
        return parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface != null && iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("is not " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface != null && iface.isInstance(this);
    }

    private ParameterMeta at(int param) throws SQLException {
        if (param < 1 || param > parameters.size()) {
            throw new SQLException("parameter index " + param);
        }
        return parameters.get(param - 1);
    }
}
