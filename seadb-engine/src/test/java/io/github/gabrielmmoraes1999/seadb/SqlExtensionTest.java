package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlExtensionTest {

    @TempDir
    Path temp;

    @Test
    void naturalUsingNullAndOuter() throws Exception {
        try (Database db = open("join")) {
            db.execute("CREATE TABLE a (id INTEGER, nome VARCHAR(10))");
            db.execute("CREATE TABLE b (id INTEGER, total INTEGER)");
            db.execute("INSERT INTO a VALUES (1, 'Ana'), (NULL, 'Nil')");
            db.execute("INSERT INTO b VALUES (1, 10), (NULL, 5)");
            SqlResult natural = db.query("SELECT * FROM a NATURAL LEFT JOIN b");
            assertEquals(List.of("id", "nome", "total"), natural.columns().stream().map(ColumnMeta::name).toList());
            assertEquals(2, natural.size());
            SqlResult nil = db.query("SELECT total FROM a NATURAL LEFT JOIN b WHERE nome = 'Nil'");
            assertNull(nil.rows().get(0).get(0));
            SqlResult using = db.query("SELECT nome, total FROM a RIGHT JOIN b USING (id) WHERE nome IS NULL");
            assertEquals(1, using.size());
            assertNull(using.rows().get(0).get(0));
        }
    }

    @Test
    void insteadOfAndReplaceAndRename() throws Exception {
        try (Database db = open("ddl")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("CREATE VIEW item_v AS SELECT id, nome FROM item");
            db.execute("""
                    CREATE TRIGGER item_v_ins FOR item_v
                    INSTEAD OF INSERT POSITION 1
                    AS
                    BEGIN
                      INSERT INTO item (id, nome) VALUES (NEW.id, NEW.nome);
                    END
                    """);
            db.execute("INSERT INTO item_v (id, nome) VALUES (1, 'Ana')");
            assertEquals("Ana", db.query("SELECT nome FROM item WHERE id = 1").rows().get(0).getString("nome"));
            SeaException readOnly = assertThrows(SeaException.class, () -> db.execute("DELETE FROM item_v WHERE id = 1"));
            assertTrue(readOnly.getMessage().contains("view is read-only"));
            db.execute("CREATE OR REPLACE VIEW item_v AS SELECT id, nome FROM item WHERE id > 0");
            assertEquals("Ana", db.query("SELECT nome FROM item_v").rows().get(0).getString("nome"));
            SeaException changed = assertThrows(SeaException.class,
                    () -> db.execute("CREATE OR REPLACE VIEW item_v AS SELECT nome FROM item"));
            assertTrue(changed.getMessage().contains("view output columns changed"));
            assertEquals(1, db.query("SELECT id FROM item_v").size());
            db.execute("CREATE VIEW other_v AS SELECT id FROM item");
            db.execute("ALTER TABLE item RENAME COLUMN nome TO title");
            String source = db.query("SELECT SEA$VIEW_SOURCE FROM SEA$VIEWS WHERE SEA$VIEW_NAME = 'item_v'")
                    .rows().get(0).getString("SEA$VIEW_SOURCE");
            assertTrue(source.toLowerCase().contains("title"));
            String other = db.query("SELECT SEA$VIEW_SOURCE FROM SEA$VIEWS WHERE SEA$VIEW_NAME = 'other_v'")
                    .rows().get(0).getString("SEA$VIEW_SOURCE");
            assertTrue(other.toLowerCase().contains(" id "));
            db.execute("BEGIN");
            db.execute("ALTER TABLE item RENAME TO goods");
            db.execute("ROLLBACK");
            assertEquals("Ana", db.query("SELECT title FROM item").rows().get(0).getString("title"));
        }
    }

    @Test
    void alterColumnDoesNotHalfWrite() throws Exception {
        try (Database db = open("alter")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, qty INTEGER)");
            db.execute("INSERT INTO item VALUES (1, 10), (2, NULL)");
            SeaException notNull = assertThrows(SeaException.class,
                    () -> db.execute("ALTER TABLE item ALTER COLUMN qty SET NOT NULL"));
            assertTrue(notNull.getMessage().contains("NULL"));
            assertNull(db.query("SELECT qty FROM item WHERE id = 2").rows().get(0).get(0));
            db.execute("UPDATE item SET qty = 3 WHERE id = 2");
            db.execute("ALTER TABLE item ALTER COLUMN qty TYPE BIGINT");
            assertEquals(10L, db.query("SELECT qty FROM item WHERE id = 1").rows().get(0).getLong("qty"));
            SeaException key = assertThrows(SeaException.class,
                    () -> db.execute("ALTER TABLE item ALTER COLUMN id DROP NOT NULL"));
            assertTrue(key.getMessage().contains("primary key"));
            db.execute("ALTER TABLE item ALTER COLUMN qty SET DEFAULT 7");
            db.execute("INSERT INTO item (id) VALUES (3)");
            assertEquals(7L, db.query("SELECT qty FROM item WHERE id = 3").rows().get(0).getLong("qty"));
        }
    }

    @Test
    void checkDomainGeneratedAndDefault() throws Exception {
        try (Database db = open("check")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, qty INTEGER CHECK (qty > 0))");
            db.execute("INSERT INTO item VALUES (1, NULL)");
            SeaException failed = assertThrows(SeaException.class, () -> db.execute("INSERT INTO item VALUES (2, 0)"));
            assertTrue(failed.getMessage().contains("check constraint failed"));
            assertEquals(1, db.query("SELECT id FROM item").size());
            db.execute("BEGIN");
            db.execute("INSERT INTO item VALUES (3, 4)");
            db.execute("ROLLBACK");
            assertEquals(1, db.query("SELECT id FROM item").size());
            db.execute("CREATE DOMAIN positive AS INTEGER CHECK (VALUE > 0)");
            db.execute("CREATE TABLE price (id INTEGER PRIMARY KEY, amount positive)");
            SeaException domain = assertThrows(SeaException.class, () -> db.execute("INSERT INTO price VALUES (1, 0)"));
            assertTrue(domain.getMessage().contains("check constraint failed"));
            SeaException inUse = assertThrows(SeaException.class, () -> db.execute("DROP DOMAIN positive"));
            assertTrue(inUse.getMessage().contains("domain is in use"));
            db.execute("CREATE TABLE calc (n INTEGER, doubled INTEGER GENERATED ALWAYS AS (n + n) STORED)");
            db.execute("INSERT INTO calc (n) VALUES (4)");
            assertEquals(8, db.query("SELECT doubled FROM calc").rows().get(0).getInt("doubled"));
            SeaException generated = assertThrows(SeaException.class,
                    () -> db.execute("INSERT INTO calc (n, doubled) VALUES (1, 2)"));
            assertTrue(generated.getMessage().contains("generated column"));
            db.execute("CREATE TABLE stamp (id INTEGER PRIMARY KEY, at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            db.execute("INSERT INTO stamp (id) VALUES (1)");
            assertTrue(db.query("SELECT at FROM stamp").rows().get(0).get(0) instanceof LocalDateTime);
        }
    }

    @Test
    void routinesAndExecutePrivilege() throws Exception {
        try (Database db = open("routine")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("""
                    CREATE FUNCTION add_one (n INTEGER)
                    RETURNS INTEGER
                    AS
                    BEGIN
                      RETURN n + 1;
                    END
                    """);
            assertEquals(3, db.query("SELECT add_one(2) AS n FROM SEA$DATABASE").rows().get(0).getInt("n"));
            db.execute("""
                    EXECUTE BLOCK (n INTEGER = 5)
                    AS
                    BEGIN
                      INSERT INTO item (id, nome) VALUES (:n, 'block');
                    END
                    """);
            assertEquals("block", db.query("SELECT nome FROM item WHERE id = 5").rows().get(0).getString("nome"));
            db.execute("""
                    CREATE PROCEDURE run_sql (text VARCHAR(80))
                    AS
                    BEGIN
                      EXECUTE STATEMENT text;
                    WHEN ANY INTO text DO
                      text = text;
                    END
                    """);
            db.execute("EXECUTE PROCEDURE run_sql('INSERT INTO item (id, nome) VALUES (6, ''dyn'')')");
            assertEquals("dyn", db.query("SELECT nome FROM item WHERE id = 6").rows().get(0).getString("nome"));
            db.execute("""
                    CREATE PROCEDURE boom
                    AS
                    DECLARE VARIABLE msg VARCHAR(40);
                    BEGIN
                      EXCEPTION 'stop';
                    WHEN EXCEPTION INTO msg DO
                      INSERT INTO item (id, nome) VALUES (7, 'caught');
                    END
                    """);
            db.execute("EXECUTE PROCEDURE boom");
            assertEquals("caught", db.query("SELECT nome FROM item WHERE id = 7").rows().get(0).getString("nome"));
            db.execute("INSERT INTO SEA$USERS (SEA$USER_NAME, SEA$PASSWORD) VALUES ('ana', 'ana')");
            Session ana = db.login("ana", "ana");
            SeaException denied = assertThrows(SeaException.class,
                    () -> db.execute(ana, "SELECT add_one(1) AS n FROM SEA$DATABASE"));
            assertTrue(denied.getMessage().contains("EXECUTE"));
            db.execute("GRANT EXECUTE ON FUNCTION add_one TO ana");
            assertEquals(2, db.execute(ana, "SELECT add_one(1) AS n FROM SEA$DATABASE").rows().get(0).getInt("n"));
        }
    }

    @Test
    void windows() throws Exception {
        try (Database db = open("window")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)");
            db.execute("INSERT INTO item VALUES (1, 1, 10), (2, 1, 10), (3, 2, NULL)");
            SqlResult numbers = db.query("SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY n) AS rn FROM item ORDER BY id");
            assertEquals(1L, numbers.rows().get(0).getLong("rn"));
            assertEquals(2L, numbers.rows().get(1).getLong("rn"));
            SeaException where = assertThrows(SeaException.class,
                    () -> db.query("SELECT id FROM item WHERE ROW_NUMBER() OVER (ORDER BY id) = 1"));
            assertTrue(where.getMessage().contains("window function is not allowed in WHERE"));
            SeaException star = assertThrows(SeaException.class,
                    () -> db.query("SELECT * FROM item ORDER BY ROW_NUMBER() OVER (ORDER BY id)"));
            assertTrue(star.getMessage().contains("SELECT * cannot be combined with a window function"));
            SqlResult lag = db.query("SELECT LAG(n, 1, 0) OVER (ORDER BY id) AS prev FROM item ORDER BY id");
            assertEquals(0, lag.rows().get(0).getInt("prev"));
            assertEquals(10, lag.rows().get(1).getInt("prev"));
        }
    }

    @Test
    void newTypesCollationAndVersion() throws Exception {
        Path file = temp.resolve("types.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE item (id UUID PRIMARY KEY, doc JSON, span INTERVAL DAY TO SECOND, at TIMESTAMP WITH TIME ZONE, name VARCHAR(20) COLLATE UNICODE_CI)");
            db.execute("INSERT INTO item VALUES (UUID '550E8400-E29B-41D4-A716-446655440000', '{\"a\":[1]}', INTERVAL '1 02:00:00' DAY TO SECOND, '2026-01-01 00:00:00+00', 'Água')");
            Row row = db.query("SELECT id, JSON_EXTRACT(doc, '$.a[0]') AS n, JSON_TYPE(doc) AS kind FROM item").rows().get(0);
            assertEquals("550e8400-e29b-41d4-a716-446655440000", row.get(0).toString());
            assertEquals(1L, ((Number) row.get("n")).longValue());
            assertEquals("object", row.getString("kind"));
            assertEquals(1, db.query("SELECT name FROM item WHERE name = 'agua'").size());
            SeaException duplicate = assertThrows(SeaException.class, () -> db.execute(
                    "INSERT INTO item VALUES (UUID '550e8400-e29b-41d4-a716-446655440000', 'null', INTERVAL '0 00:00:00' DAY TO SECOND, '2026-01-01 01:00:00+01', 'agua')"));
            assertTrue(duplicate.getMessage().toLowerCase().contains("unique") || duplicate.getMessage().toLowerCase().contains("primary")
                    || duplicate.getMessage().toLowerCase().contains("duplicate"));
            LocalDateTime shifted = (LocalDateTime) db.query("SELECT CAST('2026-01-01' AS DATE) + INTERVAL '1 02:00:00' DAY TO SECOND AS t")
                    .rows().get(0).get("t");
            assertEquals(LocalDateTime.of(2026, 1, 2, 2, 0), shifted);
        }
        byte[] header = Files.readAllBytes(file);
        int version = ByteBuffer.wrap(header).order(Format.ORDER).getShort(Format.VERSION_OFFSET) & 0xFFFF;
        assertEquals(1, version);
        try (Database db = Database.open(file, "dba", "dba")) {
            assertEquals(1, db.query("SELECT name FROM item").size());
        }
        assertEquals(1, ByteBuffer.wrap(Files.readAllBytes(file)).order(Format.ORDER).getShort(Format.VERSION_OFFSET) & 0xFFFF);
        header = Files.readAllBytes(file);
        long length = header.length;
        ByteBuffer.wrap(header).order(Format.ORDER).putShort(Format.VERSION_OFFSET, (short) 2);
        Files.write(file, header);
        SeaException previous = assertThrows(SeaException.class, () -> Database.open(file, "dba", "dba"));
        assertTrue(previous.getMessage().contains("unsupported database version"));
        assertEquals(length, Files.size(file));
        ByteBuffer.wrap(header).order(Format.ORDER).putShort(Format.VERSION_OFFSET, (short) 99);
        Files.write(file, header);
        SeaException rejected = assertThrows(SeaException.class, () -> Database.open(file, "dba", "dba"));
        assertTrue(rejected.getMessage().contains("unsupported database version"));
        assertEquals(length, Files.size(file));
    }

    @Test
    void committedCommandIsExportedAndReplayed() throws Exception {
        Path file = temp.resolve("replay.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, nome VARCHAR(10))");
            db.execute("INSERT INTO item VALUES (1, 'Ana')");
        }
        Path sql = temp.resolve("replay.sql");
        Database.exportSql(file, sql);
        String text = Files.readString(sql);
        assertTrue(text.toUpperCase().contains("CREATE TABLE"));
        assertTrue(text.contains("Ana"));
        Path copy = temp.resolve("copy.db");
        Database.create(copy, DatabaseOptions.defaults());
        try (Database db = Database.open(copy, "dba", "dba")) {
            db.execute(text);
            assertEquals("Ana", db.query("SELECT nome FROM item").rows().get(0).getString("nome"));
        }
    }

    private Database open(String name) {
        Path file = temp.resolve(name + ".db");
        Database.create(file, DatabaseOptions.defaults());
        return Database.open(file, "dba", "dba");
    }
}
