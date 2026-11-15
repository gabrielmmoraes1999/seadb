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

class SubqueryTest {

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
    void subconsultaEscalarInEExists() {
        try (Database db = open()) {
            db.execute("""
                    CREATE TABLE cliente (
                        id INTEGER PRIMARY KEY,
                        nome VARCHAR(20)
                    )
                    """);
            db.execute("""
                    CREATE TABLE pedido (
                        id INTEGER PRIMARY KEY,
                        id_cliente INTEGER,
                        total INTEGER
                    )
                    """);
            db.execute("CREATE INDEX idx_pedido_cliente ON pedido (id_cliente)");
            db.execute("INSERT INTO cliente (id, nome) VALUES (1, 'Ana'), (2, 'Bruno'), (3, 'Carla')");
            db.execute("INSERT INTO pedido (id, id_cliente, total) VALUES (10, 1, 5), (11, 1, NULL), (12, 2, 8)");

            SqlResult nomes = db.query("""
                    SELECT c.nome AS nome,
                           (SELECT COUNT(*) FROM pedido p WHERE p.id_cliente = c.id) AS qtd
                    FROM cliente c
                    ORDER BY c.id
                    """);
            assertEquals(List.of("Ana", "Bruno", "Carla"), nomes.rows().stream().map(row -> row.getString("nome")).toList());
            assertEquals(List.of(2L, 1L, 0L), nomes.rows().stream().map(row -> row.getLong("qtd")).toList());

            SqlResult um = db.query("SELECT (SELECT nome FROM cliente WHERE id = 2) AS nome");
            assertEquals("Bruno", um.rows().get(0).getString("nome"));
            assertNull(db.query("SELECT (SELECT nome FROM cliente WHERE id = 9) AS nome").rows().get(0).get("nome"));

            SeaException varias = assertThrows(SeaException.class,
                    () -> db.query("SELECT (SELECT nome FROM cliente) AS nome"));
            assertTrue(varias.getMessage().contains("scalar subquery returned more than one row"));

            SqlResult membros = db.query("""
                    SELECT nome FROM cliente
                    WHERE id IN (SELECT id_cliente FROM pedido WHERE total = 8)
                    """);
            assertEquals("Bruno", membros.rows().get(0).getString("nome"));

            SqlResult fora = db.query("""
                    SELECT nome FROM cliente
                    WHERE id NOT IN (SELECT id_cliente FROM pedido WHERE total IS NOT NULL)
                    ORDER BY id
                    """);
            assertEquals(List.of("Carla"), fora.rows().stream().map(row -> row.getString("nome")).toList());

            SqlResult desconhecido = db.query("""
                    SELECT nome FROM cliente
                    WHERE id NOT IN (SELECT total FROM pedido)
                    """);
            assertEquals(0, desconhecido.size());

            SqlResult existe = db.query("""
                    SELECT c.nome AS nome
                    FROM cliente c
                    WHERE EXISTS (SELECT 1 FROM pedido p WHERE p.id_cliente = c.id AND p.total = 5)
                    """);
            assertEquals("Ana", existe.rows().get(0).getString("nome"));

            SqlResult nao = db.query("""
                    SELECT c.nome AS nome
                    FROM cliente c
                    WHERE NOT EXISTS (SELECT 1 FROM pedido p WHERE p.id_cliente = c.id)
                    """);
            assertEquals("Carla", nao.rows().get(0).getString("nome"));

            SqlResult juncao = db.query("""
                    SELECT c.nome AS nome
                    FROM cliente c
                    JOIN pedido p ON p.id_cliente = c.id AND EXISTS (
                        SELECT 1 FROM cliente x WHERE x.id = p.id_cliente AND x.nome = 'Ana')
                    ORDER BY p.id
                    """);
            assertEquals(List.of("Ana", "Ana"), juncao.rows().stream().map(row -> row.getString("nome")).toList());

            db.execute("BEGIN");
            db.execute("INSERT INTO cliente (id, nome) VALUES (4, 'Duda')");
            assertEquals("Duda", db.query("SELECT (SELECT nome FROM cliente WHERE id = 4) AS nome").rows().get(0).getString("nome"));
            db.execute("ROLLBACK");
            assertNull(db.query("SELECT (SELECT nome FROM cliente WHERE id = 4) AS nome").rows().get(0).get("nome"));

            SeaException fundo = assertThrows(SeaException.class, () -> db.query(aninhada(33)));
            assertTrue(fundo.getMessage().contains("subquery exceeded 32 nested levels"));
            assertEquals(1, db.query(aninhada(32)).rows().get(0).getInt("n"));
        }
    }

    private static String aninhada(int levels) {
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < levels; i++) {
            sql.append("(SELECT ");
        }
        sql.append("1");
        sql.append(")".repeat(levels));
        sql.append(" AS n");
        return sql.toString();
    }
}
