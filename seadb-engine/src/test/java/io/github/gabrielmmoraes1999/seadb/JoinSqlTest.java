package io.github.gabrielmmoraes1999.seadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinSqlTest {

    @TempDir
    Path temp;

    @Test
    void naturalUsingEVirgula() throws Exception {
        Path file = temp.resolve("join.db");
        Database.create(file, DatabaseOptions.defaults());
        try (Database db = Database.open(file, "dba", "dba")) {
            db.execute("CREATE TABLE cliente (id INTEGER PRIMARY KEY, nome VARCHAR(20))");
            db.execute("CREATE TABLE pedido (id INTEGER PRIMARY KEY, total INTEGER)");
            db.execute("INSERT INTO cliente (id, nome) VALUES (1, 'Ana'), (2, 'Bia')");
            db.execute("INSERT INTO pedido (id, total) VALUES (1, 10), (2, 20)");
            SqlResult natural = db.query("SELECT * FROM cliente NATURAL JOIN pedido");
            assertEquals(List.of("id", "nome", "total"), natural.columns().stream().map(ColumnMeta::name).toList());
            assertEquals(2, natural.size());
            assertEquals("Ana", natural.rows().get(0).getString("nome"));
            SqlResult using = db.query("SELECT nome, total FROM cliente JOIN pedido USING (id) WHERE nome = 'Bia'");
            assertEquals("Bia", using.rows().get(0).getString("nome"));
            assertEquals(20, using.rows().get(0).getInt("total"));
            SqlResult virgula = db.query("SELECT cliente.nome FROM cliente, pedido WHERE cliente.id = pedido.id AND pedido.total = 10");
            assertEquals("Ana", virgula.rows().get(0).get(0));
            db.execute("CREATE VIEW cliente_pedido AS SELECT cliente.nome AS nome, pedido.total AS total FROM cliente JOIN pedido ON cliente.id = pedido.id");
            assertEquals("Ana", db.query("SELECT nome FROM cliente_pedido WHERE total = 10").rows().get(0).getString("nome"));
            SeaException somente = org.junit.jupiter.api.Assertions.assertThrows(SeaException.class,
                    () -> db.execute("INSERT INTO cliente_pedido (nome, total) VALUES ('Lia', 1)"));
            org.junit.jupiter.api.Assertions.assertTrue(somente.getMessage().contains("read-only"));
        }
    }
}
