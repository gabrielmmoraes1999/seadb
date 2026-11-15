package io.github.gabrielmmoraes1999.seadb.jdbc;

import io.github.gabrielmmoraes1999.seadb.Database;
import io.github.gabrielmmoraes1999.seadb.DatabaseOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NewTypeJdbcTest {

    @TempDir
    Path dir;

    @Test
    void typeInfoAndResult() throws Exception {
        Path file = dir.resolve("types.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Connection connection = DriverManager.getConnection("jdbc:seadb:" + file.toAbsolutePath(), "dba", "dba")) {
            assertFalse(new SeaDriver().jdbcCompliant());
            Set<String> names = new HashSet<>();
            try (ResultSet types = connection.getMetaData().getTypeInfo()) {
                while (types.next()) {
                    names.add(types.getString("TYPE_NAME"));
                }
            }
            assertTrue(names.contains("UUID"));
            assertTrue(names.contains("JSON"));
            assertTrue(names.contains("INTERVAL"));
            assertTrue(names.contains("TIMESTAMPTZ"));
            connection.createStatement().execute("""
                    CREATE TABLE item (
                        id UUID,
                        doc JSON,
                        span INTERVAL DAY TO SECOND,
                        at TIMESTAMP WITH TIME ZONE
                    )
                    """);
            connection.createStatement().execute("""
                    INSERT INTO item VALUES (
                        '550e8400-e29b-41d4-a716-446655440000',
                        '{"a":1}',
                        INTERVAL '1 02:00:00' DAY TO SECOND,
                        '2026-01-01 00:00:00+00'
                    )
                    """);
            try (ResultSet rows = connection.createStatement().executeQuery("SELECT id, doc FROM item")) {
                assertTrue(rows.next());
                assertEquals("550e8400-e29b-41d4-a716-446655440000", rows.getString("id"));
                assertEquals("{\"a\":1}", rows.getString("doc"));
                assertEquals(Types.OTHER, rows.getMetaData().getColumnType(1));
            }
        }
    }
}
