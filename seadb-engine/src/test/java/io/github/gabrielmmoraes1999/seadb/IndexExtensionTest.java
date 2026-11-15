package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexExtensionTest {

    @TempDir
    Path temp;

    @Test
    void partialExpressionAndCovering() throws Exception {
        Path file = temp.resolve("idx.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR(20))");
            SeaException rejected = assertThrows(SeaException.class,
                    () -> db.execute("CREATE INDEX bad ON t (a) WHERE b = 1"));
            assertTrue(rejected.getMessage().contains("key column"));
            db.execute("CREATE INDEX bad ON t (a)");
            db.execute("DROP INDEX bad");

            db.execute("CREATE UNIQUE INDEX pos ON t (a) WHERE a > 0");
            db.execute("INSERT INTO t VALUES (1, NULL, 'n')");
            db.execute("INSERT INTO t VALUES (2, NULL, 'n2')");
            db.execute("INSERT INTO t VALUES (3, 0, 'z')");
            db.execute("INSERT INTO t VALUES (4, 5, 'p')");
            SeaException duplicate = assertThrows(SeaException.class,
                    () -> db.execute("INSERT INTO t VALUES (5, 5, 'again')"));
            assertTrue(duplicate.getMessage().toLowerCase().contains("duplicate")
                    || duplicate.getMessage().toLowerCase().contains("unique")
                    || duplicate.getMessage().toLowerCase().contains("index"));
            assertEquals(List.of("n", "n2"), names(db, "SELECT b FROM t WHERE b LIKE 'n%' ORDER BY id"));
            assertEquals(List.of("p"), names(db, "SELECT b FROM t WHERE a > 1"));
            assertEquals(List.of(), names(db, "SELECT b FROM t WHERE a > 10"));

            db.execute("CREATE INDEX low ON t (LOWER(b))");
            db.execute("INSERT INTO t VALUES (6, 8, 'Ana')");
            db.execute("INSERT INTO t VALUES (7, 9, NULL)");
            assertEquals(List.of(6), ids(db, "SELECT id FROM t WHERE LOWER(b) = 'ana'"));
            assertEquals(List.of(7), ids(db, "SELECT id FROM t WHERE b IS NULL AND a = 9"));

            db.execute("CREATE INDEX cov ON t (a) INCLUDE (b)");
            assertEquals("p", db.query("SELECT b FROM t WHERE a = 5").rows().get(0).getString("b"));
            assertEquals(null, db.query("SELECT b FROM t WHERE a = 9").rows().get(0).getString("b"));

            db.execute("BEGIN");
            db.execute("CREATE INDEX later ON t (b) WHERE b IS NOT NULL");
            db.execute("INSERT INTO t VALUES (8, 11, 'tmp')");
            db.execute("ROLLBACK");
            assertEquals(List.of(), ids(db, "SELECT id FROM t WHERE a = 11"));
            db.execute("CREATE INDEX later ON t (b)");
        }
        try (DatabaseFile stored = DatabaseFile.open(file)) {
            TableStore table = stored.tables().stream()
                    .filter(region -> region.name().equalsIgnoreCase("t"))
                    .map(region -> TableStore.open(stored, region))
                    .findFirst()
                    .orElseThrow();
            StoredRow row = table.row(4);
            Format.writeFully(stored.channel, table.base() + row.slotOffset, new byte[4]);
        }
        try (Database db = Database.open(file, "dba", "dba")) {
            assertEquals("p", db.query("SELECT a, b FROM t WHERE a = 5").rows().get(0).getString("b"));
            assertThrows(SeaException.class, () -> db.query("SELECT id, b FROM t WHERE a = 5"));
        }
        Path sql = temp.resolve("idx.sql");
        Database.exportSql(file, sql);
        String text = Files.readString(sql).toUpperCase();
        assertTrue(text.contains("CREATE INDEX"));
        assertTrue(text.contains("WHERE"));
        assertTrue(text.contains("LOWER"));
        assertTrue(text.contains("INCLUDE"));
    }

    private static List<String> names(Database db, String sql) {
        List<String> values = new ArrayList<>();
        for (Row row : db.query(sql).rows()) {
            values.add(row.getString("b"));
        }
        return values;
    }

    private static List<Integer> ids(Database db, String sql) {
        List<Integer> values = new ArrayList<>();
        for (Row row : db.query(sql).rows()) {
            values.add(row.getInt("id"));
        }
        return values;
    }
}
