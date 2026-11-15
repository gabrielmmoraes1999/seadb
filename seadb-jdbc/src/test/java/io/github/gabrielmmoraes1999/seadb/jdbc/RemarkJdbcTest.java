package io.github.gabrielmmoraes1999.seadb.jdbc;

import io.github.gabrielmmoraes1999.seadb.Database;
import io.github.gabrielmmoraes1999.seadb.DatabaseOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RemarkJdbcTest {

    @TempDir
    Path dir;

    @Test
    void remarksAppearOnTheExistingMetadataColumns() throws Exception {
        Path file = dir.resolve("remarks.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Connection connection = DriverManager.getConnection("jdbc:seadb:" + file.toAbsolutePath(), "dba", "dba")) {
            connection.createStatement().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            connection.createStatement().execute("COMMENT ON TABLE t IS 'clientes'");
            connection.createStatement().execute("COMMENT ON COLUMN t.nome IS 'rotulo'");
            try (ResultSet tables = connection.getMetaData().getTables(null, "public", "t", null)) {
                tables.next();
                assertEquals("clientes", tables.getString("REMARKS"));
            }
            try (ResultSet columns = connection.getMetaData().getColumns(null, "public", "t", "nome")) {
                columns.next();
                assertEquals("rotulo", columns.getString("REMARKS"));
            }
            try (ResultSet columns = connection.getMetaData().getColumns(null, "public", "t", "id")) {
                columns.next();
                assertNull(columns.getString("REMARKS"));
            }
            connection.createStatement().execute("""
                    CREATE TABLE customer (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name VARCHAR(120) NOT NULL,
                        email VARCHAR(120) UNIQUE,
                        birth_date DATE,
                        photo BLOB
                    )
                    """);
            try (java.sql.PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO customer (name, email, birth_date, photo) VALUES (?, ?, ?, ?)")) {
                insert.setString(1, "Ana");
                insert.setString(2, "ana@example.com");
                insert.setDate(3, java.sql.Date.valueOf("1990-04-12"));
                insert.setBytes(4, new byte[]{1, 2, 3});
                insert.executeUpdate();
            }
            try (java.sql.PreparedStatement query = connection.prepareStatement(
                    "SELECT id, name, photo FROM customer WHERE email = ?")) {
                query.setString(1, "ana@example.com");
                try (ResultSet rows = query.executeQuery()) {
                    rows.next();
                    assertEquals("Ana", rows.getString("name"));
                    assertEquals(3, rows.getBytes("photo").length);
                }
            }
        }
    }
}
