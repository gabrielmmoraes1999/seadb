package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorTest {

    @TempDir
    Path temp;

    @Test
    void blocksKeepNullsAndAClosedCursorDoesNotWrite() {
        Path file = temp.resolve("cursor.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("INSERT INTO t VALUES (1, 'a')");
            db.execute("INSERT INTO t VALUES (2, NULL)");
            db.execute("INSERT INTO t VALUES (3, 'c')");
            assertEquals(3, db.query("SELECT id FROM t").size());

            Session session = db.session();
            SqlResult first = db.execute(session, "SELECT id, nome FROM t", new Object[0], 2);
            assertEquals(2, first.size());
            assertTrue(first.cursor() != 0);
            assertEquals(1L, first.rows().get(0).getLong("id"));
            assertNull(first.rows().get(1).getString("nome"));
            SqlResult rest = db.fetch(session, first.cursor(), 2);
            assertEquals(1, rest.size());
            assertEquals(0L, rest.cursor());
            assertEquals(3L, rest.rows().get(0).getLong("id"));
            assertThrows(SeaException.class, () -> db.fetch(session, first.cursor(), 2));
            assertEquals(3, db.query("SELECT id FROM t").size());
        }
    }
}
