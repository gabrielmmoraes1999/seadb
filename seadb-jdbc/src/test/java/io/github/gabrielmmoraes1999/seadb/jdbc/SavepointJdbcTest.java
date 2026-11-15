package io.github.gabrielmmoraes1999.seadb.jdbc;

import io.github.gabrielmmoraes1999.seadb.Database;
import io.github.gabrielmmoraes1999.seadb.DatabaseOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SavepointJdbcTest {

    @TempDir
    Path dir;

    @Test
    void savepointJdbcDesfazALinha() throws Exception {
        Path file = dir.resolve("sp.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Connection connection = DriverManager.getConnection("jdbc:seadb:" + file.toAbsolutePath(), "dba", "dba")) {
            assertTrue(connection.getMetaData().supportsSavepoints());
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            }
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO item (id, nome) VALUES (1, 'ana')");
                Savepoint mark = connection.setSavepoint("marca");
                statement.execute("INSERT INTO item (id, nome) VALUES (2, 'bia')");
                connection.rollback(mark);
                connection.releaseSavepoint(mark);
                connection.commit();
            }
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT nome FROM item")) {
                assertEquals(true, rows.next());
                assertEquals("ana", rows.getString("nome"));
                assertEquals(false, rows.next());
            }
        }
    }
}
