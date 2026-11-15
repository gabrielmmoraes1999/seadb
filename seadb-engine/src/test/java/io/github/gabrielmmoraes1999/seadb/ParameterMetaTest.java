package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterMetaTest {

    @TempDir
    Path temp;

    @Test
    void describesWithoutWriting() {
        Path file = temp.resolve("params.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("""
                    CREATE PROCEDURE soma (a INTEGER, b INTEGER)
                    RETURNS (resultado INTEGER)
                    AS
                    BEGIN
                      resultado = a + b;
                      SUSPEND;
                    END
                    """);
            List<ParameterMeta> insert = db.describeParameters("INSERT INTO t (id, nome) VALUES (?, ?)");
            assertEquals(DataType.INTEGER, insert.get(0).type());
            assertFalse(insert.get(0).nullable());
            assertEquals(DataType.VARCHAR, insert.get(1).type());
            assertEquals(20, insert.get(1).precision());
            assertTrue(insert.get(1).nullable());

            List<ParameterMeta> filter = db.describeParameters("SELECT nome FROM t WHERE id = ? AND nome = ?");
            assertEquals(DataType.INTEGER, filter.get(0).type());
            assertEquals(DataType.VARCHAR, filter.get(1).type());

            List<ParameterMeta> call = db.describeParameters("EXECUTE PROCEDURE soma(?, ?)");
            assertEquals(2, call.size());
            assertEquals(DataType.INTEGER, call.get(0).type());
            assertEquals(DataType.INTEGER, call.get(1).type());

            ParameterMeta bare = db.describeParameters("SELECT ?").get(0);
            assertEquals(DataType.VARCHAR, bare.type());
            assertEquals(0, bare.precision());
            assertTrue(bare.nullable());

            ParameterMeta missing = db.describeParameters("INSERT INTO missing VALUES (?)").get(0);
            assertEquals(DataType.VARCHAR, missing.type());
            assertEquals(0, missing.precision());
            assertThrows(SeaException.class, () -> db.describeParameters("SELEKT ?"));
            assertEquals(0, db.query("SELECT id FROM t").size());

            assertEquals(5L, db.query("EXECUTE PROCEDURE soma(2, 3)").rows().get(0).getLong("resultado"));
            assertEquals(1, db.query("EXECUTE PROCEDURE soma(2, 3)").size());
        }
    }
}
