package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnalyzeTest {

    @TempDir
    Path temp;

    @Test
    void countsNullRollbackAndRange() throws Exception {
        assertTrue(IndexFeatures.estimateExceedsHalf(10, 10, 1, 10, 1, null));
        assertFalse(IndexFeatures.estimateExceedsHalf(10, 10, 1, 10, 8, null));
        assertFalse(IndexFeatures.estimateExceedsHalf(10, 10, null, 10, 1, null));

        Path file = temp.resolve("stats.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
            db.execute("CREATE INDEX ia ON t (a)");
            for (int id = 1; id <= 10; id++) {
                db.execute("INSERT INTO t VALUES (" + id + ", " + id + ")");
            }
            db.execute("INSERT INTO t VALUES (11, NULL)");
            db.execute("ANALYZE t");

            SqlResult stats = db.query("""
                    SELECT SEA$INDEX_NAME, SEA$CARDINALITY, SEA$INDEX_ENTRIES
                    FROM SEA$STATISTICS WHERE SEA$RELATION_NAME = 't'
                    """);
            long tableRows = 0;
            long indexEntries = 0;
            for (Row row : stats.rows()) {
                assertEquals(11L, row.getLong("SEA$CARDINALITY"));
                String index = row.getString("SEA$INDEX_NAME");
                if (index == null || index.isEmpty()) {
                    tableRows = row.getLong("SEA$INDEX_ENTRIES");
                } else if (index.equalsIgnoreCase("ia")) {
                    indexEntries = row.getLong("SEA$INDEX_ENTRIES");
                    assertEquals("1", db.query("""
                            SELECT SEA$LEADING_MIN FROM SEA$STATISTICS
                            WHERE SEA$RELATION_NAME = 't' AND SEA$INDEX_NAME = 'ia'
                            """).rows().get(0).getString("SEA$LEADING_MIN"));
                }
            }
            assertEquals(11L, tableRows);
            assertEquals(10L, indexEntries);

            assertEquals(10, db.query("SELECT id FROM t WHERE a > 0").size());
            assertEquals(1, db.query("SELECT id FROM t WHERE a > 9").size());
            assertEquals(11L, db.query("SELECT id FROM t WHERE a IS NULL").rows().get(0).getLong("id"));

            int before = stats.size();
            SeaException missing = assertThrows(SeaException.class, () -> db.execute("ANALYZE missing"));
            assertTrue(missing.getMessage().contains("table not found"));
            assertEquals(before, db.query("SELECT SEA$INDEX_NAME FROM SEA$STATISTICS WHERE SEA$RELATION_NAME = 't'").size());

            db.execute("BEGIN");
            db.execute("INSERT INTO t VALUES (12, 12)");
            db.execute("ANALYZE t");
            db.execute("ROLLBACK");
            assertEquals(11L, db.query("""
                    SELECT SEA$CARDINALITY FROM SEA$STATISTICS
                    WHERE SEA$RELATION_NAME = 't' AND SEA$INDEX_NAME = ''
                    """).rows().get(0).getLong("SEA$CARDINALITY"));
            assertEquals(0, db.query("SELECT id FROM t WHERE id = 12").size());
        }

        Path sql = temp.resolve("stats.sql");
        Database.exportSql(file, sql);
        String text = Files.readString(sql).toUpperCase();
        assertTrue(text.contains("ANALYZE"));
        Path copy = temp.resolve("copy.db");
        Database.create(copy, DatabaseOptions.defaults());
        try (Database db = Database.open(copy, "dba", "dba")) {
            db.execute(Files.readString(sql).replaceAll("(?m)^go\\s*$", ";"));
            assertEquals(11L, db.query("""
                    SELECT SEA$CARDINALITY FROM SEA$STATISTICS
                    WHERE SEA$RELATION_NAME = 't' AND SEA$INDEX_NAME = ''
                    """).rows().get(0).getLong("SEA$CARDINALITY"));
        }
    }
}
