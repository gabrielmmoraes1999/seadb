package io.github.gabrielmmoraes1999.seadb.jdbc;

import io.github.gabrielmmoraes1999.seadb.Database;
import io.github.gabrielmmoraes1999.seadb.DatabaseOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CallableJdbcTest {

    @TempDir
    Path dir;

    @Test
    void callAndParameterMeta() throws Exception {
        Path file = dir.resolve("call.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Connection connection = DriverManager.getConnection("jdbc:seadb:" + file.toAbsolutePath(), "dba", "dba")) {
            assertFalse(new SeaDriver().jdbcCompliant());
            assertTrue(connection.getMetaData().supportsStoredProcedures());
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
                statement.execute("""
                        CREATE PROCEDURE soma (a INTEGER, b INTEGER)
                        RETURNS (resultado INTEGER)
                        AS
                        BEGIN
                          resultado = a + b;
                          SUSPEND;
                          resultado = 0;
                          SUSPEND;
                        END
                        """);
                statement.execute("""
                        CREATE PROCEDURE eco (a INTEGER)
                        RETURNS (n INTEGER)
                        AS
                        BEGIN
                          n = a;
                          SUSPEND;
                        END
                        """);
                statement.execute("""
                        CREATE PROCEDURE grava (n INTEGER)
                        AS
                        BEGIN
                          INSERT INTO t (id, nome) VALUES (n, 'row');
                        END
                        """);
                statement.execute("""
                        CREATE PROCEDURE nada
                        AS
                        BEGIN
                          EXIT;
                        END
                        """);
            }

            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO t (id, nome) VALUES (?, ?)")) {
                ParameterMetaData meta = insert.getParameterMetaData();
                assertEquals(2, meta.getParameterCount());
                assertEquals(Types.INTEGER, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNoNulls, meta.isNullable(1));
                assertEquals(Types.VARCHAR, meta.getParameterType(2));
                assertEquals(20, meta.getPrecision(2));
                assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(2));
                assertThrows(SQLFeatureNotSupportedException.class, () -> insert.setArray(1, null));
                assertThrows(SQLFeatureNotSupportedException.class, () -> insert.setRef(1, null));
                assertThrows(SQLFeatureNotSupportedException.class, () -> insert.setRowId(1, null));
            }
            assertEquals(0, count(connection));

            try (PreparedStatement bare = connection.prepareStatement("SELECT ?")) {
                ParameterMetaData meta = bare.getParameterMetaData();
                assertEquals(Types.VARCHAR, meta.getParameterType(1));
                assertEquals(0, meta.getPrecision(1));
                assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(1));
            }

            try (CallableStatement call = connection.prepareCall("{call soma(?, ?)}")) {
                ParameterMetaData meta = call.getParameterMetaData();
                assertEquals(Types.INTEGER, meta.getParameterType(1));
                assertEquals(Types.INTEGER, meta.getParameterType(2));
                call.setInt(1, 2);
                call.setInt(2, 3);
                assertTrue(call.execute());
                assertEquals(5, call.getInt(1));
                assertEquals(5, call.getInt("resultado"));
                assertFalse(call.wasNull());
            }

            try (CallableStatement call = connection.prepareCall("EXECUTE PROCEDURE eco(?)")) {
                call.setNull(1, Types.INTEGER);
                assertTrue(call.execute());
                assertEquals(0, call.getInt(1));
                assertTrue(call.wasNull());
            }

            try (CallableStatement call = connection.prepareCall("EXECUTE PROCEDURE nada")) {
                assertFalse(call.execute());
                SQLException missing = assertThrows(SQLException.class, () -> call.getInt(1));
                assertTrue(missing.getMessage().contains("did not return"));
            }

            SQLException badCall = assertThrows(SQLException.class, () -> connection.prepareCall("SELECT 1"));
            assertTrue(badCall.getMessage().contains("not supported"));

            connection.setAutoCommit(false);
            try (CallableStatement call = connection.prepareCall("{call grava(?)}")) {
                call.setInt(1, 7);
                assertFalse(call.execute());
                connection.rollback();
            }
            connection.setAutoCommit(true);
            assertEquals(0, count(connection));

            try (CallableStatement call = connection.prepareCall("{call grava(?, ?)}")) {
                call.setInt(1, 8);
                call.setInt(2, 9);
                assertThrows(SQLException.class, call::execute);
            }
            assertEquals(0, count(connection));
        }
    }

    private static int count(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT id FROM t")) {
            int count = 0;
            while (rows.next()) {
                count++;
            }
            return count;
        }
    }
}
