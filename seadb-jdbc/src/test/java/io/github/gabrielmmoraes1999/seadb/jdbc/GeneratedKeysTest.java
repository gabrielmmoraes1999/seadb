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
import static org.junit.jupiter.api.Assertions.assertFalse;

class GeneratedKeysTest {

    @TempDir
    Path dir;

    @Test
    void devolveChavesEReturning() throws Exception {
        Path file = dir.resolve("loja.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Connection connection = DriverManager.getConnection("jdbc:seadb:" + file.toAbsolutePath(), "dba", "dba");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE item (id INTEGER PRIMARY KEY AUTOINCREMENT, nome VARCHAR(10))");
            statement.execute("INSERT INTO item (nome) VALUES ('Ana'), ('Bruno')", Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = statement.getGeneratedKeys()) {
                assertEquals(true, keys.next());
                assertEquals(1L, keys.getLong("GENERATED_KEY"));
                assertEquals(true, keys.next());
                assertEquals(2L, keys.getLong("GENERATED_KEY"));
                assertFalse(keys.next());
            }
            assertEquals(true, statement.execute("INSERT INTO item (nome) VALUES ('Carla') RETURNING nome"));
            try (ResultSet rows = statement.getResultSet()) {
                assertEquals(true, rows.next());
                assertEquals("Carla", rows.getString("nome"));
            }
            statement.execute("CREATE TABLE nota (id INTEGER PRIMARY KEY, nome VARCHAR(10))");
            statement.execute("INSERT INTO nota (id, nome) VALUES (7, 'Lia')", Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = statement.getGeneratedKeys()) {
                assertFalse(keys.next());
            }
        }
    }
}
