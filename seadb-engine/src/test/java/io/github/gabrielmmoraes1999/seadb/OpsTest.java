package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpsTest {

    @TempDir
    Path temp;

    @Test
    void explainCommentAndRestoreByTime() throws Exception {
        Path file = temp.resolve("ops.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("INSERT INTO t VALUES (1, 'a')");
            String plan = db.query("EXPLAIN SELECT id FROM t WHERE id = 1").rows().get(0).getString("plan");
            assertTrue(plan.startsWith("index"));
            assertTrue(plan.contains("id"));
            db.query("EXPLAIN INSERT INTO t SELECT id FROM t");
            assertEquals(1, db.query("SELECT id FROM t").size());
            assertEquals("INSERT ... SELECT is not supported",
                    assertThrows(SeaException.class, () -> db.execute("INSERT INTO t SELECT id FROM t")).getMessage());

            db.execute("COMMENT ON TABLE t IS 'clientes'");
            assertThrows(SeaException.class, () -> db.execute("COMMENT ON TABLE t IS ''"));
            assertEquals("clientes", db.query("""
                    SELECT SEA$REMARK FROM SEA$REMARKS WHERE SEA$KIND = 'TABLE' AND SEA$OBJECT_NAME = 't'
                    """).rows().get(0).getString(0));
            db.execute("BEGIN");
            db.execute("COMMENT ON TABLE t IS NULL");
            db.execute("ROLLBACK");
            assertEquals("clientes", db.query("""
                    SELECT SEA$REMARK FROM SEA$REMARKS WHERE SEA$KIND = 'TABLE' AND SEA$OBJECT_NAME = 't'
                    """).rows().get(0).getString(0));
            db.execute("COMMENT ON COLUMN t.nome IS 'rotulo'");
            assertEquals("rotulo", db.query("""
                    SELECT SEA$REMARK FROM SEA$REMARKS WHERE SEA$KIND = 'COLUMN' AND SEA$COLUMN_NAME = 'nome'
                    """).rows().get(0).getString(0));
        }
        Path snap = temp.resolve("snap.db");
        Files.copy(file, snap);
        Files.copy(TransactionLog.beside(file), TransactionLog.beside(snap));
        Instant cut = Instant.now();
        Thread.sleep(20);
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("INSERT INTO t VALUES (2, 'b')");
        }
        Path sql = temp.resolve("ops.sql");
        Database.exportSql(file, sql);
        String exported = Files.readString(sql);
        assertTrue(exported.contains("INSERT"));
        assertTrue(exported.contains("BEGIN TRANSACTION"));
        assertTrue(exported.contains("COMMIT WORK"));
        assertTrue(exported.contains("\ngo\n"));
        Path full = temp.resolve("full.db");
        Files.copy(snap, full);
        Files.copy(TransactionLog.beside(snap), TransactionLog.beside(full));
        Database.applyLog(full, TransactionLog.beside(file));
        try (Database db = Database.open(full, "dba", "dba")) {
            assertEquals(2, db.query("SELECT id FROM t").size());
        }
        Path cutFile = temp.resolve("cut.db");
        Files.copy(snap, cutFile);
        Files.copy(TransactionLog.beside(snap), TransactionLog.beside(cutFile));
        Database.applyLog(cutFile, TransactionLog.beside(file), null, null, null, cut);
        try (Database db = Database.open(cutFile, "dba", "dba")) {
            assertEquals(1, db.query("SELECT id FROM t").size());
        }
    }

    @Test
    void exportSkipsSelectAndUsesTheSybaseScript() throws Exception {
        Path file = temp.resolve("script.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("BEGIN");
            db.execute("INSERT INTO t VALUES (1, 'a')");
            db.execute("SELECT id FROM t");
            db.execute("EXPLAIN SELECT id FROM t");
            db.execute("COMMIT");
        }
        Path sql = temp.resolve("script.sql");
        Database.exportSql(file, sql);
        String exported = Files.readString(sql);
        String upper = exported.toUpperCase();
        assertFalse(upper.contains("SELECT"));
        assertFalse(upper.contains("EXPLAIN"));
        assertTrue(exported.contains("--INSERT-"));
        assertTrue(exported.contains("--CONNECT-"));
        assertTrue(exported.contains("-DBA-"));
        assertTrue(exported.contains("--CHECKPOINT-0000-"));
        assertTrue(exported.contains("BEGIN TRANSACTION"));
        assertTrue(exported.contains("\ngo\n"));
        assertTrue(exported.contains("COMMIT WORK"));
        assertTrue(exported.contains("'a'"));
    }
}
