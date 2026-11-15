package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogApplyProgressTest {
    @TempDir
    Path dir;

    private Path database(String name) {
        Path file = dir.resolve(name + ".db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY)");
            db.execute("INSERT INTO item VALUES (1)");
        }
        return file;
    }

    @Test
    void reportsCheckpointEvenWhenThereAreNoSqlBatchesToReplay() {
        Path file = database("clean");
        List<String> messages = new ArrayList<>();
        Database.applyLog(file, TransactionLog.beside(file), null, null, messages::add);
        assertTrue(messages.getFirst().startsWith("Starting database \""));
        assertTrue(messages.contains("Database recovery in progress"));
        assertTrue(messages.stream().anyMatch(text -> text.startsWith("    Last checkpoint at ")));
        assertTrue(messages.stream().anyMatch(text -> text.startsWith("    Transaction log: ")));
        assertEquals("Recovery complete", messages.getLast());
        try (Database db = Database.open(file, "dba", "dba")) {
            assertEquals(1, db.query("SELECT COUNT(*) FROM item").rows().get(0).getLong(0));
        }
    }

    @Test
    void reportsUndoBeforeCompletionAndRecoversUncommittedPages() {
        Path file = database("crashed");
        Database db = Database.open(file, "dba", "dba");
        try {
            db.checkpointPolicy(CheckpointPolicy.MANUAL);
            db.execute("BEGIN");
            db.execute("INSERT INTO item VALUES (2)");
            db.checkpoint();
            db.crash();
        } finally {
            db.close();
        }
        List<String> messages = new ArrayList<>();
        Database.applyLog(file, TransactionLog.beside(file), null, null, messages::add);
        assertTrue(messages.contains("Database recovery in progress"));
        assertEquals("Recovery complete", messages.getLast());
        int finished = -1;
        for (int i = 0; i < messages.size(); i++) {
            if (messages.get(i).startsWith("Finished checkpoint of \"")) {
                finished = i;
            }
        }
        if (finished >= 0) {
            assertTrue(finished < messages.size() - 1);
        }
        try (Database recovered = Database.open(file, "dba", "dba")) {
            assertEquals(1, recovered.query("SELECT COUNT(*) FROM item").rows().get(0).getLong(0));
        }
    }

    @Test
    void rejectedLogDoesNotReportAnAppliedCheckpoint() {
        Path file = database("target");
        Path other = database("other");
        List<String> messages = new ArrayList<>();
        assertThrows(SeaException.class, () -> Database.applyLog(file, TransactionLog.beside(other),
                null, null, messages::add));
        assertFalse(messages.stream().anyMatch(text -> text.equals("Recovery complete")
                || text.startsWith("Finished checkpoint")));
    }
}
