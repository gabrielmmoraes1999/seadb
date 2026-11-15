package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteSqlTest {

    @TempDir
    Path dir;

    private Database open() {
        Path file = dir.resolve("banco.db");
        if (!Files.exists(file)) {
            Database.create(file, DatabaseOptions.defaults());
        }
        return Database.open(file, "dba", "dba");
    }

    @Test
    void insereSelecionaConjuntosEConflito() {
        try (Database db = open()) {
            db.execute("CREATE TABLE origem (id INTEGER, nome VARCHAR(10))");
            db.execute("CREATE TABLE destino (id INTEGER PRIMARY KEY, nome VARCHAR(10))");
            db.execute("INSERT INTO origem (id, nome) VALUES (1, 'Ana'), (2, 'Bruno'), (2, 'Bia')");

            SeaException select = assertThrows(SeaException.class,
                    () -> db.execute("INSERT INTO destino (id) SELECT nome FROM origem"));
            assertEquals("INSERT ... SELECT is not supported", select.getMessage());
            assertEquals(0, db.query("SELECT id FROM destino").size());

            db.execute("BEGIN");
            SeaException noMeio = assertThrows(SeaException.class,
                    () -> db.execute("INSERT INTO destino (id, nome) SELECT id, nome FROM origem"));
            assertEquals("INSERT ... SELECT is not supported", noMeio.getMessage());
            db.execute("ROLLBACK");
            assertEquals(0, db.query("SELECT id FROM destino").size());

            db.execute("DELETE FROM origem WHERE nome = 'Bia'");
            db.execute("INSERT INTO destino (id, nome) VALUES (1, 'Ana'), (2, 'Bruno')");
            SqlResult gravado = db.query("SELECT nome FROM destino ORDER BY id");
            assertEquals(List.of("Ana", "Bruno"), gravado.rows().stream().map(row -> row.getString("nome")).toList());

            SqlResult uniao = db.query("SELECT nome FROM destino UNION SELECT nome FROM origem ORDER BY nome");
            assertEquals(List.of("Ana", "Bruno"), uniao.rows().stream().map(row -> row.getString("nome")).toList());
            SqlResult exceto = db.query("SELECT id FROM destino EXCEPT SELECT id FROM origem WHERE id = 1");
            assertEquals(2, exceto.rows().get(0).getInt("id"));
            SqlResult cruza = db.query("SELECT nome FROM destino INTERSECT SELECT nome FROM origem");
            assertEquals(2, cruza.size());
            SqlResult todos = db.query("SELECT id FROM origem EXCEPT ALL SELECT id FROM destino");
            assertEquals(0, todos.size());

            db.execute("""
                    INSERT INTO destino (id, nome) VALUES (1, 'Lia')
                    ON CONFLICT (id) DO UPDATE SET nome = EXCLUDED.nome
                    """);
            assertEquals("Lia", db.query("SELECT nome FROM destino WHERE id = 1").rows().get(0).getString("nome"));
            db.execute("INSERT INTO destino (id, nome) VALUES (1, 'Nao') ON CONFLICT (id) DO NOTHING");
            assertEquals("Lia", db.query("SELECT nome FROM destino WHERE id = 1").rows().get(0).getString("nome"));

            db.execute("""
                    MERGE INTO destino d
                    USING origem o
                    ON d.id = o.id
                    WHEN MATCHED THEN UPDATE SET nome = o.nome
                    WHEN NOT MATCHED THEN INSERT (id, nome) VALUES (o.id, o.nome)
                    """);
            assertEquals("Ana", db.query("SELECT nome FROM destino WHERE id = 1").rows().get(0).getString("nome"));

            SeaException merge = assertThrows(SeaException.class, () -> db.execute("""
                    MERGE INTO destino d
                    USING (SELECT 1 AS id) s
                    ON 1 = 1
                    WHEN MATCHED THEN UPDATE SET nome = 'x'
                    """));
            assertTrue(merge.getMessage().contains("MERGE matched more than one target row"));
        }
    }

    @Test
    void truncaSemTriggerEDevolveChaves() {
        try (Database db = open()) {
            db.execute("""
                    CREATE TABLE item (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nome VARCHAR(10)
                    )
                    """);
            db.execute("""
                    CREATE TABLE filho (
                        id INTEGER PRIMARY KEY,
                        item_id INTEGER,
                        FOREIGN KEY (item_id) REFERENCES item (id)
                    )
                    """);
            db.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, texto VARCHAR(20))");
            db.execute("""
                    CREATE TRIGGER item_ad FOR item
                    AFTER DELETE POSITION 0
                    AS
                    BEGIN
                      INSERT INTO log (id, texto) VALUES (1, 'apagou');
                    END
                    """);
            db.execute("INSERT INTO item (nome) VALUES ('Ana')");
            db.execute("INSERT INTO item (nome) VALUES ('Bruno')");
            assertEquals(2, db.query("SELECT id FROM item WHERE id = 2").rows().get(0).getInt("id"));

            db.execute("INSERT INTO filho (id, item_id) VALUES (1, 1)");
            SeaException bloqueio = assertThrows(SeaException.class, () -> db.execute("TRUNCATE TABLE item"));
            assertTrue(bloqueio.getMessage().contains("TRUNCATE blocked by child rows"));
            assertEquals(2, db.query("SELECT id FROM item").size());

            db.execute("DELETE FROM filho");
            db.execute("BEGIN");
            db.execute("TRUNCATE TABLE item");
            db.execute("ROLLBACK");
            assertEquals(2, db.query("SELECT id FROM item").size());

            SqlResult apagado = db.query("DELETE FROM item WHERE id = 2 RETURNING nome");
            assertEquals("Bruno", apagado.rows().get(0).getString("nome"));
            db.execute("DELETE FROM log");
            db.execute("TRUNCATE TABLE item");
            assertEquals(0, db.query("SELECT id FROM log").size());
            db.execute("INSERT INTO item (nome) VALUES ('Carla')");
            assertEquals(1, db.query("SELECT id FROM item").rows().get(0).getInt("id"));
            assertNull(db.query("SELECT nome FROM item WHERE nome IS NULL").rows().isEmpty() ? null : "x");
        }
    }
}
