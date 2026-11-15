package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IsolationTest {

    @TempDir
    Path temp;

    @Test
    void isolamentoSnapshotELock() throws Exception {
        Path file = temp.resolve("iso.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("INSERT INTO SEA$CONFIG (SEA$KEY, SEA$VALUE) VALUES ('Lock.Timeout', '0')");
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("INSERT INTO item (id, nome) VALUES (1, 'ana'), (2, 'bia')");
            Session uma = db.session();
            Session outra = db.login("dba", "dba");

            db.execute(outra, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
            db.execute(uma, "BEGIN");
            db.execute(uma, "UPDATE item SET nome = 'suja' WHERE id = 1");
            assertEquals("suja", db.execute(outra, "SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
            db.execute(uma, "ROLLBACK");

            db.execute(outra, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
            db.execute(outra, "BEGIN");
            assertEquals("ana", db.execute(outra, "SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
            db.execute(uma, "UPDATE item SET nome = 'nova' WHERE id = 1");
            assertEquals("nova", db.execute(outra, "SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
            db.execute(outra, "COMMIT");

            db.execute(outra, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            db.execute(outra, "BEGIN");
            assertEquals("nova", db.execute(outra, "SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
            db.execute(uma, "UPDATE item SET nome = 'fim' WHERE id = 1");
            assertEquals("nova", db.execute(outra, "SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
            db.execute(outra, "COMMIT");
            assertEquals("fim", db.execute(outra, "SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));

            db.execute(uma, "BEGIN");
            db.execute(outra, "BEGIN");
            db.execute(uma, "UPDATE item SET nome = 'um' WHERE id = 1");
            db.execute(outra, "UPDATE item SET nome = 'dois' WHERE id = 2");
            db.execute(uma, "COMMIT");
            db.execute(outra, "COMMIT");
            assertEquals("um", db.execute(uma, "SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
            assertEquals("dois", db.execute(uma, "SELECT nome FROM item WHERE id = 2").rows().get(0).getString("nome"));

            db.execute("UPDATE SEA$CONFIG SET SEA$VALUE = '2' WHERE SEA$KEY = 'Lock.Timeout'");
            db.execute(uma, "BEGIN");
            db.execute(outra, "BEGIN");
            db.execute(uma, "UPDATE item SET nome = 'a' WHERE id = 1");
            db.execute(outra, "UPDATE item SET nome = 'b' WHERE id = 2");
            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> falha = new AtomicReference<>();
            Thread espera = new Thread(() -> {
                started.countDown();
                try {
                    db.execute(uma, "UPDATE item SET nome = 'x' WHERE id = 2");
                } catch (RuntimeException exception) {
                    falha.set(exception);
                }
            });
            espera.start();
            started.await();
            Thread.sleep(200);
            SeaException deadlock = org.junit.jupiter.api.Assertions.assertThrows(SeaException.class,
                    () -> db.execute(outra, "UPDATE item SET nome = 'y' WHERE id = 1"));
            assertTrue(deadlock.getMessage().contains("deadlock") || falha.get() != null);
            espera.join(3000);
            db.execute(uma, "ROLLBACK");
            db.execute(outra, "ROLLBACK");
        }
    }
}
