package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SavepointTest {

    @TempDir
    Path temp;

    @Test
    void savepointDesfazLinhaETabela() throws Exception {
        Path file = temp.resolve("sp.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("CREATE GENERATOR seq");
            db.execute("BEGIN");
            db.execute("INSERT INTO item (id, nome) VALUES (1, 'ana')");
            db.execute("SAVEPOINT s");
            db.execute("INSERT INTO item (id, nome) VALUES (2, 'bia')");
            db.execute("CREATE TABLE extra (id INTEGER PRIMARY KEY)");
            db.execute("SELECT GEN_ID(seq, 1) FROM item WHERE id = 1");
            db.execute("ROLLBACK TO SAVEPOINT s");
            assertEquals(1, db.query("SELECT id FROM item").size());
            assertEquals("ana", db.query("SELECT nome FROM item").rows().get(0).getString("nome"));
            SeaException ausente = assertThrows(SeaException.class, () -> db.query("SELECT id FROM extra"));
            assertTrue(ausente.getMessage().toLowerCase().contains("extra") || ausente.getMessage().contains("not found"));
            db.execute("RELEASE SAVEPOINT s");
            assertThrows(SeaException.class, () -> db.execute("ROLLBACK TO SAVEPOINT s"));
            db.execute("COMMIT");
            assertEquals(1, db.query("SELECT id FROM item").size());
            SqlResult gerador = db.query("SELECT GEN_ID(seq, 0) FROM item WHERE id = 1");
            assertEquals(1L, ((Number) gerador.rows().get(0).get(0)).longValue());
        }
    }
}
