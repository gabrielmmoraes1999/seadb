package io.github.gabrielmmoraes1999.seadb.jdbc;

import io.github.gabrielmmoraes1999.seadb.Database;
import io.github.gabrielmmoraes1999.seadb.DatabaseOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorJdbcTest {

    @TempDir
    Path dir;

    @Test
    void forwardBlocksAndScrollDrains() throws Exception {
        Path file = dir.resolve("cursor.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Connection connection = DriverManager.getConnection("jdbc:seadb:" + file.toAbsolutePath(), "dba", "dba")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
                statement.execute("INSERT INTO t VALUES (1, 'a')");
                statement.execute("INSERT INTO t VALUES (2, NULL)");
                statement.execute("INSERT INTO t VALUES (3, 'c')");
                statement.setFetchSize(2);
                try (ResultSet rows = statement.executeQuery("SELECT id, nome FROM t")) {
                    assertTrue(rows.next());
                    assertEquals(1, rows.getInt("id"));
                    assertTrue(rows.next());
                    assertEquals(2, rows.getInt("id"));
                    assertNull(rows.getString("nome"));
                    assertTrue(rows.next());
                    assertEquals(3, rows.getInt("id"));
                    assertTrue(!rows.next());
                }
            }
            try (Statement statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                statement.setFetchSize(2);
                try (ResultSet rows = statement.executeQuery("SELECT id FROM t")) {
                    assertTrue(rows.last());
                    assertEquals(3, rows.getInt(1));
                    assertTrue(rows.first());
                    assertEquals(1, rows.getInt(1));
                    assertTrue(rows.absolute(2));
                    assertEquals(2, rows.getInt(1));
                }
            }
        }
    }
}
