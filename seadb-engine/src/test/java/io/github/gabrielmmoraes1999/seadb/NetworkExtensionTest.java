package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetworkExtensionTest {

    @TempDir
    Path temp;

    @Test
    void columnGrantNullRollbackAndAudit() throws Exception {
        Path file = temp.resolve("net.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20), email VARCHAR(40))");
            db.execute("INSERT INTO t VALUES (1, NULL, 'a@b.c')");
            db.execute("INSERT INTO SEA$USERS (SEA$USER_NAME, SEA$PASSWORD) VALUES ('ana', 'ana')");
            SeaException missing = assertThrows(SeaException.class,
                    () -> db.execute("GRANT SELECT (nope) ON t TO ana"));
            assertEquals("column not found: nope", missing.getMessage());
            assertEquals(0, db.query("SELECT SEA$COLUMN_NAME FROM SEA$COLUMN_PRIVILEGES").size());
            assertThrows(SeaException.class, () -> db.execute("GRANT DELETE (id) ON t TO ana"));

            db.execute("BEGIN");
            db.execute("GRANT SELECT (nome) ON t TO ana");
            db.execute("GRANT INSERT (id, nome) ON t TO ana");
            db.execute("ROLLBACK");
            Session ana = db.login("ana", "ana");
            assertThrows(SeaException.class, () -> db.execute(ana, "SELECT nome FROM t"));

            db.execute("GRANT SELECT (nome) ON t TO ana");
            db.execute("GRANT INSERT (id, nome) ON t TO ana");
            SqlResult star = db.execute(ana, "SELECT * FROM t");
            assertEquals(1, star.columns().size());
            assertEquals("nome", star.columns().get(0).name());
            assertNull(star.rows().get(0).getString(0));
            SeaException hidden = assertThrows(SeaException.class, () -> db.execute(ana, "SELECT email FROM t"));
            assertEquals("column is not granted: email", hidden.getMessage());
            assertThrows(SeaException.class, () -> db.execute(ana, "INSERT INTO t (id, nome, email) VALUES (2, 'z', 'x')"));
            assertEquals(1, db.query("SELECT id FROM t").size());
            db.execute(ana, "INSERT INTO t (id, nome) VALUES (2, NULL)");
            assertNull(db.query("SELECT nome FROM t WHERE id = 2").rows().get(0).get(0));

            db.execute("REVOKE SELECT (nome) ON t FROM ana");
            assertThrows(SeaException.class, () -> db.execute(ana, "SELECT nome FROM t"));

            db.execute("INSERT INTO SEA$CONFIG (SEA$KEY, SEA$VALUE) VALUES ('Audit.Log', 'on')");
            db.execute("SELECT 1");
            assertThrows(SeaException.class, () -> db.execute("SELECT missing_table"));
            String audit = Files.readString(file.resolveSibling("net.db.audit"));
            assertTrue(audit.contains("\tok\tSELECT 1"));
            assertTrue(audit.contains("\tfail\t"));
            assertFalse(audit.contains("secret-value"));
            db.execute("INSERT INTO t (id, nome) VALUES (?, ?)", 9, "secret-value");
            audit = Files.readString(file.resolveSibling("net.db.audit"));
            assertFalse(audit.contains("secret-value"));

            db.execute("INSERT INTO SEA$CONFIG (SEA$KEY, SEA$VALUE) VALUES ('Max.Connections', '1')");
            SeaException limited = assertThrows(SeaException.class, () -> db.login("ana", "ana"));
            assertEquals("connection limit reached", limited.getMessage());
            assertEquals("08001", limited.sqlState());
        }
        Pace.arm(new Session("DBA", true, 1), System.currentTimeMillis() - 5);
        SeaException timed = assertThrows(SeaException.class, Pace::check);
        assertEquals("command timed out", timed.getMessage());
        Session session = new Session("DBA", true, 2);
        Pace.arm(session, 0);
        session.cancelled = true;
        SeaException cancelled = assertThrows(SeaException.class, Pace::check);
        assertEquals("command cancelled", cancelled.getMessage());
        Pace.clear();
        assertEquals(2, Pace.seconds(5, 2));
        assertEquals(4, Pace.seconds(4, 0));
    }
}
